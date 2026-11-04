// Package postgres provides the PostgreSQL-backed durable implementation of
// jobs.Store. Transactions are short: workers never hold a DB lock while
// performing a handler's work.
package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"io/fs"
	"sort"
	"errors"
	"fmt"
	"time"

	"github.com/Martin-Aziz/reliable-workflows-lab/internal/jobs"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Store owns no database connections; the caller owns db and must close it.
type Store struct {
	db            *sql.DB
	leaseDuration time.Duration
	retryBase     time.Duration
}

type Option func(*Store)

func WithLeaseDuration(d time.Duration) Option {
	return func(s *Store) {
		if d > 0 {
			s.leaseDuration = d
		}
	}
}
func WithRetryBase(d time.Duration) Option {
	return func(s *Store) {
		if d > 0 {
			s.retryBase = d
		}
	}
}

func New(db *sql.DB, options ...Option) *Store {
	s := &Store{db: db, leaseDuration: 10 * time.Second, retryBase: time.Second}
	for _, opt := range options {
		opt(s)
	}
	return s
}

func (s *Store) Ready(ctx context.Context) error { return s.db.PingContext(ctx) }

// Migrate tracks additive migrations under a transaction-scoped advisory lock.
// Migration 001 is idempotent, including for previously initialized databases.
func (s *Store) Migrate(ctx context.Context) error {
	paths, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil { return err }
	sort.Strings(paths)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil { return err }
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(1544192026)`); err != nil { return err }
	if _, err = tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS workflow_schema_migrations (
		version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil { return err }
	for _, name := range paths {
		var applied bool
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(
			SELECT 1 FROM workflow_schema_migrations WHERE version=$1)`, name).Scan(&applied); err != nil {
			return err
		}
		if applied { continue }
		ddl, readErr := migrations.ReadFile(name)
		if readErr != nil { return readErr }
		if _, err = tx.ExecContext(ctx, string(ddl)); err != nil {
			return fmt.Errorf("migration %s: %w", name, err)
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO workflow_schema_migrations(version) VALUES ($1)`, name); err != nil {
			return err
		}
	}
	return tx.Commit()
}

const columns = `id,kind,payload::text,state,attempts,max_attempts,available_at,lease_until,worker_id,result::text,last_error,created_at,updated_at`

// UPDATE ... FROM has both target and candidate identifiers in scope;
// RETURNING must qualify fields from the updated target row.
const returningColumns = `j.id AS id,j.kind AS kind,j.payload::text AS payload,j.state AS state,j.attempts AS attempts,j.max_attempts AS max_attempts,j.available_at AS available_at,j.lease_until AS lease_until,j.worker_id AS worker_id,j.result::text AS result,j.last_error AS last_error,j.created_at AS created_at,j.updated_at AS updated_at`

type scanner interface{ Scan(...any) error }

func scanJob(row scanner) (jobs.Job, error) {
	var j jobs.Job
	var state string
	var payload string
	var until sql.NullTime
	var worker, result, lastError sql.NullString
	err := row.Scan(&j.ID, &j.Kind, &payload, &state, &j.Attempts, &j.MaxAttempts, &j.AvailableAt, &until, &worker, &result, &lastError, &j.CreatedAt, &j.UpdatedAt)
	if err != nil {
		return jobs.Job{}, err
	}
	j.State = jobs.State(state)
	j.Payload = json.RawMessage(payload)
	if until.Valid {
		t := until.Time
		j.LeaseUntil = &t
	}
	if worker.Valid {
		j.WorkerID = worker.String
	}
	if result.Valid {
		j.Result = json.RawMessage(result.String)
	}
	if lastError.Valid {
		j.LastError = lastError.String
	}
	return j, nil
}

func id() (string, error) { // cryptographic random IDs, distinct across processes
	return jobs.NewID()
}

func (s *Store) Enqueue(ctx context.Context, req jobs.Submission, key string) (jobs.Job, bool, error) {
	canonical, fingerprint, maxAttempts, err := jobs.NormalizeSubmission(req, key)
	if err != nil {
		return jobs.Job{}, false, err
	}
	jobID, err := id()
	if err != nil {
		return jobs.Job{}, false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return jobs.Job{}, false, err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO workflow_jobs (id,kind,payload,state,attempts,max_attempts,available_at,created_at,updated_at)
        VALUES ($1,$2,$3::jsonb,'queued',0,$4,now(),now(),now())`, jobID, req.Kind, string(canonical), maxAttempts)
	if err != nil {
		return jobs.Job{}, false, err
	}
	if key != "" {
		result, err := tx.ExecContext(ctx, `INSERT INTO workflow_idempotency (key,job_id,fingerprint)
            VALUES ($1,$2,$3) ON CONFLICT (key) DO NOTHING`, key, jobID, fingerprint[:])
		if err != nil {
			return jobs.Job{}, false, err
		}
		inserted, err := result.RowsAffected()
		if err != nil {
			return jobs.Job{}, false, err
		}
		if inserted == 0 {
			// Roll back the speculative job before looking up the winner. The
			// unique index serializes simultaneous submissions for a key.
			if err := tx.Rollback(); err != nil {
				return jobs.Job{}, false, err
			}
			var savedFingerprint []byte
			var savedID string
			err = s.db.QueryRowContext(ctx, `SELECT job_id,fingerprint FROM workflow_idempotency WHERE key=$1`, key).Scan(&savedID, &savedFingerprint)
			if err != nil {
				return jobs.Job{}, false, err
			}
			if !bytes.Equal(savedFingerprint, fingerprint[:]) {
				return jobs.Job{}, false, jobs.ErrConflict
			}
			j, err := s.Get(ctx, savedID)
			return j, true, err
		}
	}
	if err := tx.Commit(); err != nil {
		return jobs.Job{}, false, err
	}
	j, err := s.Get(ctx, jobID)
	return j, false, err
}

func (s *Store) Get(ctx context.Context, jobID string) (jobs.Job, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+columns+` FROM workflow_jobs WHERE id=$1`, jobID)
	j, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return jobs.Job{}, jobs.ErrNotFound
	}
	return j, err
}

func (s *Store) List(ctx context.Context, limit int) ([]jobs.Job, error) {
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+columns+` FROM workflow_jobs ORDER BY created_at DESC,id DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]jobs.Job, 0, limit)
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, j)
	}
	return result, rows.Err()
}

func (s *Store) Stats(ctx context.Context) (jobs.Stats, error) {
	var stats jobs.Stats
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*)::int,
        COUNT(*) FILTER (WHERE state='queued')::int,
        COUNT(*) FILTER (WHERE state='running')::int,
        COUNT(*) FILTER (WHERE state='succeeded')::int,
        COUNT(*) FILTER (WHERE state='dead')::int,
        COUNT(*) FILTER (WHERE state='canceled')::int FROM workflow_jobs`).Scan(
		&stats.Total, &stats.Queued, &stats.Running, &stats.Succeeded, &stats.Dead, &stats.Canceled)
	return stats, err
}

// reclaimExpired safely transitions bounded batches of expired leases to a
// delayed retry or terminal state. Attempts already consumed are not refunded.
func (s *Store) reclaimExpired(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `WITH expired AS (
        SELECT id FROM workflow_jobs WHERE state='running' AND lease_until<=now()
        ORDER BY lease_until,id LIMIT 100 FOR UPDATE SKIP LOCKED
    ), recovered AS (
        UPDATE workflow_jobs j SET
            state=CASE WHEN j.attempts>=j.max_attempts THEN 'dead' ELSE 'queued' END,
            worker_id=NULL,lease_token=NULL,lease_until=NULL,
            available_at=CASE WHEN j.attempts>=j.max_attempts THEN j.available_at
                ELSE now()+LEAST($1::float8*power(2::float8,LEAST(j.attempts-1,5)),30::float8)*interval '1 second' END,
            last_error='worker lease expired',updated_at=now()
        FROM expired WHERE j.id=expired.id RETURNING j.id,j.attempts
    ) UPDATE workflow_attempts a SET outcome='expired',finished_at=now()
        FROM recovered WHERE a.job_id=recovered.id AND a.attempt=recovered.attempts AND a.outcome IS NULL`, s.retryBase.Seconds())
	return err
}

// Claim obtains exactly one *lease* from concurrent PostgreSQL consumers; a
// retry may still execute the handler more than once after a crashed worker.
func (s *Store) Claim(ctx context.Context, workerID string) (jobs.Lease, bool, error) {
	if workerID == "" {
		return jobs.Lease{}, false, jobs.ErrInvalid
	}
	if err := s.reclaimExpired(ctx); err != nil {
		return jobs.Lease{}, false, err
	}
	token, err := id()
	if err != nil {
		return jobs.Lease{}, false, err
	}
	row := s.db.QueryRowContext(ctx, `WITH candidate AS (
        SELECT id FROM workflow_jobs WHERE state='queued' AND available_at<=now()
        ORDER BY available_at,created_at,id LIMIT 1 FOR UPDATE SKIP LOCKED
    ), claimed AS (
        UPDATE workflow_jobs j SET state='running', attempts=j.attempts+1,
            worker_id=$1,lease_token=$2,lease_until=now()+($3::float8*interval '1 second'),updated_at=now()
        FROM candidate WHERE j.id=candidate.id
        RETURNING `+returningColumns+`,j.lease_token
    ), recorded AS (
        INSERT INTO workflow_attempts (job_id,attempt,worker_id,lease_token,started_at)
        SELECT id,attempts,worker_id,lease_token,now() FROM claimed
        RETURNING job_id
    ) SELECT c.id,c.kind,c.payload,c.state,c.attempts,c.max_attempts,c.available_at,c.lease_until,
        c.worker_id,c.result,c.last_error,c.created_at,c.updated_at
    FROM claimed c JOIN recorded r ON r.job_id=c.id`, workerID, token, s.leaseDuration.Seconds())
	j, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return jobs.Lease{}, false, nil
	}
	if err != nil {
		return jobs.Lease{}, false, err
	}
	return jobs.Lease{Job: j, Token: token}, true, nil
}

func (s *Store) Complete(ctx context.Context, jobID, token string, result json.RawMessage) error {
	if !json.Valid(result) || len(result) > 64*1024 {
		return jobs.ErrInvalid
	}
	if token == "" {
		return jobs.ErrLeaseLost
	}
	r, err := s.db.ExecContext(ctx, `WITH finished AS (
        UPDATE workflow_jobs j SET state='succeeded',result=$3::jsonb,
            lease_token=NULL,lease_until=NULL,worker_id=NULL,updated_at=now()
        WHERE j.id=$1 AND j.state='running' AND j.lease_token=$2 AND j.lease_until>now()
        AND EXISTS (SELECT 1 FROM workflow_attempts a WHERE a.job_id=j.id AND a.attempt=j.attempts AND a.outcome IS NULL)
        RETURNING j.id,j.attempts
    ) UPDATE workflow_attempts a SET outcome='succeeded',finished_at=now()
       FROM finished WHERE a.job_id=finished.id AND a.attempt=finished.attempts AND a.outcome IS NULL`, jobID, token, string(result))
	return s.finalizeResult(ctx, jobID, r, err)
}

func (s *Store) Fail(ctx context.Context, jobID, token string, cause error) error {
	if token == "" {
		return jobs.ErrLeaseLost
	}
	msg := "handler failed"
	if cause != nil {
		msg = "handler execution failed"
	}
	r, err := s.db.ExecContext(ctx, `WITH finished AS (
        UPDATE workflow_jobs j SET
            state=CASE WHEN j.attempts>=j.max_attempts THEN 'dead' ELSE 'queued' END,
            available_at=CASE WHEN j.attempts>=j.max_attempts THEN j.available_at
                ELSE now()+LEAST($3::float8*power(2::float8,LEAST(j.attempts-1,5)),30::float8)*interval '1 second' END,
            last_error=$4,lease_token=NULL,lease_until=NULL,worker_id=NULL,updated_at=now()
        WHERE j.id=$1 AND j.state='running' AND j.lease_token=$2 AND j.lease_until>now()
        AND EXISTS (SELECT 1 FROM workflow_attempts a WHERE a.job_id=j.id AND a.attempt=j.attempts AND a.outcome IS NULL)
        RETURNING j.id,j.attempts
    ) UPDATE workflow_attempts a SET outcome='failed',finished_at=now()
       FROM finished WHERE a.job_id=finished.id AND a.attempt=finished.attempts AND a.outcome IS NULL`, jobID, token, s.retryBase.Seconds(), msg)
	return s.finalizeResult(ctx, jobID, r, err)
}

func (s *Store) finalizeResult(ctx context.Context, jobID string, result sql.Result, err error) error {
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 1 {
		return nil
	}
	if affected != 0 {
		return fmt.Errorf("unexpected number of rows updated: %d", affected)
	}
	_, err = s.Get(ctx, jobID)
	if errors.Is(err, jobs.ErrNotFound) {
		return jobs.ErrNotFound
	}
	if err != nil {
		return err
	}
	return jobs.ErrLeaseLost
}

// Renew extends a live, token-owned lease. Expired leases cannot be resurrected.
func (s *Store) Renew(ctx context.Context, jobID, token string) error {
	if token == "" { return jobs.ErrLeaseLost }
	r, err := s.db.ExecContext(ctx, `UPDATE workflow_jobs
		SET lease_until=now()+($3::float8*interval '1 second'),updated_at=now()
		WHERE id=$1 AND state='running' AND lease_token=$2 AND lease_until>now()`,
		jobID, token, s.leaseDuration.Seconds())
	return s.finalizeResult(ctx, jobID, r, err)
}

// Cancel atomically fences active work and marks a job terminal. It is idempotent
// for canceled jobs; succeeded and dead jobs cannot be canceled retroactively.
func (s *Store) Cancel(ctx context.Context, jobID string) (jobs.Job, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil { return jobs.Job{}, err }
	defer tx.Rollback()
	var state string
	err = tx.QueryRowContext(ctx, `SELECT state FROM workflow_jobs WHERE id=$1 FOR UPDATE`, jobID).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) { return jobs.Job{}, jobs.ErrNotFound }
	if err != nil { return jobs.Job{}, err }
	switch jobs.State(state) {
	case jobs.Succeeded, jobs.Dead:
		return jobs.Job{}, jobs.ErrTerminal
	case jobs.Canceled:
		// Replay does not mutate state.
	case jobs.Queued, jobs.Running:
		if _, err = tx.ExecContext(ctx, `UPDATE workflow_jobs
			SET state='canceled',worker_id=NULL,lease_token=NULL,lease_until=NULL,updated_at=now()
			WHERE id=$1`, jobID); err != nil {
			return jobs.Job{}, err
		}
		if state == string(jobs.Running) {
			if _, err = tx.ExecContext(ctx, `UPDATE workflow_attempts
				SET outcome='canceled',finished_at=now()
				WHERE job_id=$1 AND outcome IS NULL`, jobID); err != nil {
				return jobs.Job{}, err
			}
		}
	default:
		return jobs.Job{}, jobs.ErrInvalid
	}
	if err = tx.Commit(); err != nil { return jobs.Job{}, err }
	return s.Get(ctx, jobID)
}

var _ jobs.Store = (*Store)(nil)
