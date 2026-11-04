//go:build integration

package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"github.com/Martin-Aziz/reliable-workflows-lab/internal/jobs"
)

func integrationStores(t *testing.T) (*Store, *Store, *sql.DB, string) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; needs a dedicated PostgreSQL test database")
	}
	parsed, err := url.Parse(dsn)
	if err != nil || parsed.Scheme != "postgres" && parsed.Scheme != "postgresql" || !strings.HasSuffix(parsed.Path, "_test") {
		t.Fatal("TEST_DATABASE_URL must be a postgres(s) URL for a database ending in _test")
	}
	admin, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := admin.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	suffix, err := jobs.NewID()
	if err != nil {
		t.Fatal(err)
	}
	schema := "rwl_it_" + suffix
	if _, err := admin.ExecContext(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		defer admin.Close()
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := admin.ExecContext(cleanupCtx, "DROP SCHEMA "+schema+" CASCADE")
		if err != nil {
			t.Errorf("cleanup schema: %v", err)
		}
	})
	q := parsed.Query()
	q.Set("search_path", schema)
	parsed.RawQuery = q.Encode()
	isolatedDSN := parsed.String()
	open := func() *sql.DB {
		db, err := sql.Open("postgres", isolatedDSN)
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(8)
		t.Cleanup(func() { db.Close() })
		if err := db.PingContext(ctx); err != nil {
			t.Fatal(err)
		}
		return db
	}
	first, second := open(), open()
	a := New(first, WithRetryBase(10*time.Millisecond), WithLeaseDuration(5*time.Second))
	b := New(second, WithRetryBase(10*time.Millisecond), WithLeaseDuration(5*time.Second))
	if err := a.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return a, b, first, isolatedDSN
}

func TestPostgresConcurrencyRestartRecoveryAndFencing(t *testing.T) {
	a, b, db, dsn := integrationStores(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	submit := jobs.Submission{Kind: "echo", Payload: json.RawMessage(`{"a":1,"b":2}`), MaxAttempts: 2}
	const n = 24
	var wg sync.WaitGroup
	ids := make(chan string, n)
	errorsCh := make(chan error, n)
	created := make(chan bool, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			store := a
			if i%2 == 0 {
				store = b
			}
			j, replay, err := store.Enqueue(ctx, submit, "same-key")
			if err != nil {
				errorsCh <- err
				return
			}
			ids <- j.ID
			created <- !replay
		}(i)
	}
	wg.Wait()
	close(ids)
	close(errorsCh)
	close(created)
	for err := range errorsCh {
		t.Errorf("enqueue: %v", err)
	}
	var id string
	for next := range ids {
		if id == "" {
			id = next
		}
		if id != next {
			t.Fatal("idempotency created multiple jobs")
		}
	}
	if id == "" {
		t.Fatal("no job created")
	}
	totalCreated := 0
	for wasCreated := range created {
		if wasCreated {
			totalCreated++
		}
	}
	if totalCreated != 1 {
		t.Fatalf("created %d jobs", totalCreated)
	}
	changed := submit
	changed.Payload = json.RawMessage(`{"a":99}`)
	if _, _, err := b.Enqueue(ctx, changed, "same-key"); !errors.Is(err, jobs.ErrConflict) {
		t.Fatalf("expected conflict: %v", err)
	}
	// Two Store objects have independent connection pools, representing distinct server instances.
	if _, err := b.Get(ctx, id); err != nil {
		t.Fatalf("job not visible from second store: %v", err)
	}
	j, replay, err := b.Enqueue(ctx, jobs.Submission{Kind: "echo", Payload: json.RawMessage(`{"b":2,"a":1}`), MaxAttempts: 2}, "same-key")
	if err != nil || !replay || j.ID != id {
		t.Fatalf("replay across connection pools: %v %v", replay, err)
	}
	first, ok, err := a.Claim(ctx, "process-A")
	if err != nil || !ok || first.Job.ID != id {
		t.Fatalf("first claim: %v %v %#v", ok, err, first)
	}
	if _, err := db.ExecContext(ctx, `UPDATE workflow_jobs SET lease_until=now()-interval '1 second' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.Claim(ctx, "process-B"); err != nil {
		t.Fatal(err)
	}
	if err := a.Complete(ctx, id, first.Token, json.RawMessage(`true`)); !errors.Is(err, jobs.ErrLeaseLost) {
		t.Fatalf("stale completion accepted: %v", err)
	}
	time.Sleep(35 * time.Millisecond)
	second, ok, err := b.Claim(ctx, "process-B")
	if err != nil || !ok || second.Job.ID != id || second.Job.Attempts != 2 {
		t.Fatalf("claim retry: %v %v %#v", ok, err, second)
	}
	if err := b.Complete(ctx, id, second.Token, json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	saved, err := a.Get(ctx, id)
	if err != nil || saved.State != jobs.Succeeded || saved.LeaseUntil != nil {
		t.Fatalf("final state: %#v %v", saved, err)
	}
	stats, err := b.Stats(ctx)
	if err != nil || stats.Total != 1 || stats.Succeeded != 1 {
		t.Fatalf("stats: %#v %v", stats, err)
	}
	// A newly opened connection pool can observe data created by prior processes.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	restartedDB, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer restartedDB.Close()
	restarted := New(restartedDB)
	persisted, err := restarted.Get(ctx, id)
	if err != nil || persisted.State != jobs.Succeeded {
		t.Fatalf("not persisted after reconnect: %#v %v", persisted, err)
	}
	_, replay, err = restarted.Enqueue(ctx, submit, "same-key")
	if err != nil || !replay {
		t.Fatalf("idempotency after reconnect: %v %v", replay, err)
	}
	var expired, success int
	err = restartedDB.QueryRowContext(ctx, `SELECT COUNT(*) FILTER (WHERE outcome='expired')::int,COUNT(*) FILTER (WHERE outcome='succeeded')::int FROM workflow_attempts WHERE job_id=$1`, id).Scan(&expired, &success)
	if err != nil || expired != 1 || success != 1 {
		t.Fatalf("attempt history: expired=%d success=%d err=%v", expired, success, err)
	}
}

func TestPostgresManyWorkersClaimEachJobOnce(t *testing.T) {
	a, b, _, _ := integrationStores(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const count = 35
	expected := map[string]bool{}
	for i := 0; i < count; i++ {
		j, _, err := a.Enqueue(ctx, jobs.Submission{Kind: "echo", Payload: json.RawMessage(fmt.Sprintf(`{"number":%d}`, i))}, "")
		if err != nil {
			t.Fatal(err)
		}
		expected[j.ID] = true
	}
	claimed := make(chan string, count)
	failures := make(chan error, count)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			store := a
			if i%2 == 0 {
				store = b
			}
			for {
				lease, ok, err := store.Claim(ctx, fmt.Sprintf("worker-%d", i))
				if err != nil {
					failures <- err
					return
				}
				if !ok {
					return
				}
				claimed <- lease.Job.ID
				if err := store.Complete(ctx, lease.Job.ID, lease.Token, json.RawMessage(`{"ok":true}`)); err != nil {
					failures <- err
					return
				}
			}
		}(i)
	}
	wg.Wait()
	close(claimed)
	close(failures)
	for err := range failures {
		t.Errorf("worker: %v", err)
	}
	for id := range claimed {
		if !expected[id] {
			t.Fatalf("duplicate/unexpected job: %s", id)
		}
		delete(expected, id)
	}
	if len(expected) > 0 {
		t.Fatalf("missing %d jobs", len(expected))
	}
}
