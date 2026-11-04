// Package jobs contains the concurrency-safe, deliberately in-memory Phase 1
// reference implementation of a leased job queue.
package jobs

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"
)

type State string

const (
	Queued    State = "queued"
	Running   State = "running"
	Succeeded State = "succeeded"
	Dead      State = "dead"
)

var (
	ErrNotFound  = errors.New("job not found")
	ErrInvalid   = errors.New("invalid job")
	ErrConflict  = errors.New("idempotency key already used with different request")
	ErrLeaseLost = errors.New("job lease no longer owned by worker")
)

type Job struct {
	ID          string          `json:"id"`
	Kind        string          `json:"kind"`
	Payload     json.RawMessage `json:"payload"`
	State       State           `json:"state"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	AvailableAt time.Time       `json:"available_at"`
	LeaseUntil  *time.Time      `json:"lease_until,omitempty"`
	WorkerID    string          `json:"worker_id,omitempty"`
	Result      json.RawMessage `json:"result,omitempty"`
	LastError   string          `json:"last_error,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
	leaseToken  string
}

type Submission struct {
	Kind        string          `json:"kind"`
	Payload     json.RawMessage `json:"payload"`
	MaxAttempts int             `json:"max_attempts,omitempty"`
}

type Lease struct {
	Job   Job
	Token string
}

type idempotentEntry struct {
	jobID       string
	fingerprint [32]byte
}

type Queue struct {
	mu            sync.Mutex
	jobs          map[string]*Job
	order         []string
	idempotency   map[string]idempotentEntry
	now           func() time.Time
	leaseDuration time.Duration
	retryBase     time.Duration
}

type Option func(*Queue)

func WithClock(now func() time.Time) Option { return func(q *Queue) { q.now = now } }
func WithLeaseDuration(d time.Duration) Option {
	return func(q *Queue) {
		if d > 0 {
			q.leaseDuration = d
		}
	}
}
func WithRetryBase(d time.Duration) Option {
	return func(q *Queue) {
		if d > 0 {
			q.retryBase = d
		}
	}
}

func NewQueue(opts ...Option) *Queue {
	q := &Queue{
		jobs:          make(map[string]*Job),
		idempotency:   make(map[string]idempotentEntry),
		now:           func() time.Time { return time.Now().UTC() },
		leaseDuration: 10 * time.Second,
		retryBase:     time.Second,
	}
	for _, opt := range opts {
		opt(q)
	}
	return q
}

func generateID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func clone(job Job) Job {
	job.Payload = slices.Clone(job.Payload)
	job.Result = slices.Clone(job.Result)
	if job.LeaseUntil != nil {
		t := *job.LeaseUntil
		job.LeaseUntil = &t
	}
	return job
}

func (q *Queue) Enqueue(req Submission, idempotencyKey string) (Job, bool, error) {
	if req.Kind == "" || !json.Valid(req.Payload) || len(req.Payload) > 64*1024 || len(idempotencyKey) > 128 {
		return Job{}, false, ErrInvalid
	}
	if req.MaxAttempts == 0 {
		req.MaxAttempts = 3
	}
	if req.MaxAttempts < 1 || req.MaxAttempts > 8 {
		return Job{}, false, ErrInvalid
	}
	// JSON re-encoding canonicalizes object keys; inconsequential whitespace and
	// key ordering must not make idempotent retries appear different.
	var normalized any
	if err := json.Unmarshal(req.Payload, &normalized); err != nil {
		return Job{}, false, ErrInvalid
	}
	canonical, err := json.Marshal(normalized)
	if err != nil {
		return Job{}, false, err
	}
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%s", req.Kind, req.MaxAttempts, canonical)))

	q.mu.Lock()
	defer q.mu.Unlock()
	if idempotencyKey != "" {
		if entry, ok := q.idempotency[idempotencyKey]; ok {
			if entry.fingerprint != digest {
				return Job{}, false, ErrConflict
			}
			return clone(*q.jobs[entry.jobID]), true, nil
		}
	}
	id, err := generateID()
	if err != nil {
		return Job{}, false, err
	}
	now := q.now()
	job := &Job{ID: id, Kind: req.Kind, Payload: canonical, State: Queued, MaxAttempts: req.MaxAttempts, AvailableAt: now, CreatedAt: now, UpdatedAt: now}
	q.jobs[id] = job
	q.order = append(q.order, id)
	if idempotencyKey != "" {
		q.idempotency[idempotencyKey] = idempotentEntry{jobID: id, fingerprint: digest}
	}
	return clone(*job), false, nil
}

func (q *Queue) Get(id string) (Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	j, ok := q.jobs[id]
	if !ok {
		return Job{}, ErrNotFound
	}
	return clone(*j), nil
}

// List returns a bounded snapshot in insertion order, without exposing leases.
// Stats returns a point-in-time count of each job state. For Phase 1 it is
// computed on demand; a durable implementation will use indexed queries.
type Stats struct {
	Total     int `json:"total"`
	Queued    int `json:"queued"`
	Running   int `json:"running"`
	Succeeded int `json:"succeeded"`
	Dead      int `json:"dead"`
}

func (q *Queue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	s := Stats{Total: len(q.jobs)}
	for _, j := range q.jobs {
		switch j.State {
		case Queued:
			s.Queued++
		case Running:
			s.Running++
		case Succeeded:
			s.Succeeded++
		case Dead:
			s.Dead++
		}
	}
	return s
}

func (q *Queue) List(limit int) []Job {
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	result := make([]Job, 0, min(limit, len(q.order)))
	for i := len(q.order) - 1; i >= 0 && len(result) < limit; i-- {
		result = append(result, clone(*q.jobs[q.order[i]]))
	}
	return result
}

func (q *Queue) delay(attempt int) time.Duration {
	// Exponential retry with a bounded delay. No random jitter in Phase 1,
	// making the state machine deterministic under a controlled clock.
	d := q.retryBase * time.Duration(1<<min(attempt-1, 5))
	return min(d, 30*time.Second)
}

// Claim atomically selects and leases one due job. Each attempt, including
// abandoned leases, consumes one of the bounded attempt budget.
func (q *Queue) Claim(workerID string) (Lease, bool, error) {
	if workerID == "" {
		return Lease{}, false, ErrInvalid
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.now()
	for _, id := range q.order {
		j := q.jobs[id]
		if j.State == Running && j.LeaseUntil != nil && !now.Before(*j.LeaseUntil) {
			j.WorkerID = ""
			j.LeaseUntil = nil
			j.leaseToken = ""
			j.LastError = "worker lease expired"
			j.UpdatedAt = now
			if j.Attempts >= j.MaxAttempts {
				j.State = Dead
			} else {
				j.State = Queued
				j.AvailableAt = now.Add(q.delay(j.Attempts))
			}
		}
		if j.State != Queued || now.Before(j.AvailableAt) {
			continue
		}
		token, err := generateID()
		if err != nil {
			return Lease{}, false, err
		}
		until := now.Add(q.leaseDuration)
		j.State = Running
		j.Attempts++
		j.WorkerID = workerID
		j.leaseToken = token
		j.LeaseUntil = &until
		j.UpdatedAt = now
		return Lease{Job: clone(*j), Token: token}, true, nil
	}
	return Lease{}, false, nil
}

func (q *Queue) verifyLease(j *Job, token string) error {
	if token == "" || j.State != Running || j.leaseToken != token || j.LeaseUntil == nil || !q.now().Before(*j.LeaseUntil) {
		return ErrLeaseLost
	}
	return nil
}

func (q *Queue) Complete(id, token string, result json.RawMessage) error {
	if !json.Valid(result) || len(result) > 64*1024 {
		return ErrInvalid
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	j, ok := q.jobs[id]
	if !ok {
		return ErrNotFound
	}
	if err := q.verifyLease(j, token); err != nil {
		return err
	}
	j.State = Succeeded
	j.Result = slices.Clone(result)
	j.WorkerID = ""
	j.LeaseUntil = nil
	j.leaseToken = ""
	j.UpdatedAt = q.now()
	return nil
}

func (q *Queue) Fail(id, token string, cause error) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	j, ok := q.jobs[id]
	if !ok {
		return ErrNotFound
	}
	if err := q.verifyLease(j, token); err != nil {
		return err
	}
	now := q.now()
	j.WorkerID = ""
	j.LeaseUntil = nil
	j.leaseToken = ""
	j.UpdatedAt = now
	// Never expose raw errors to clients: they may contain secrets. This is
	// limited to generic error categories, with diagnostics in server logs.
	if cause != nil {
		j.LastError = "handler execution failed"
	} else {
		j.LastError = "handler failed"
	}
	if j.Attempts >= j.MaxAttempts {
		j.State = Dead
	} else {
		j.State = Queued
		j.AvailableAt = now.Add(q.delay(j.Attempts))
	}
	return nil
}
