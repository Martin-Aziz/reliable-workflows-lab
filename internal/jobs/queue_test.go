package jobs

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func fakeClock() (func() time.Time, func(time.Duration)) {
	var unix atomic.Int64
	unix.Store(time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC).UnixNano())
	return func() time.Time { return time.Unix(0, unix.Load()).UTC() }, func(d time.Duration) { unix.Add(int64(d)) }
}
func mustEnqueue(t *testing.T, q *Queue, kind string, maxAttempts int) Job {
	t.Helper()
	j, _, err := q.Enqueue(Submission{Kind: kind, Payload: json.RawMessage(`{"message":"test"}`), MaxAttempts: maxAttempts}, "")
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func TestConcurrentIdempotencyReplaysOneJob(t *testing.T) {
	q := NewQueue()
	const goroutines = 32
	var wg sync.WaitGroup
	results := make(chan string, goroutines)
	var created atomic.Int32
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			j, existing, err := q.Enqueue(Submission{Kind: "echo", Payload: json.RawMessage(`{"a":1,"b":2}`)}, "same-key")
			if err != nil {
				results <- "ERROR:" + err.Error()
				return
			}
			if !existing {
				created.Add(1)
			}
			results <- j.ID
		}()
	}
	wg.Wait()
	close(results)
	var first string
	for id := range results {
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatalf("replay mismatch: %q vs %q", id, first)
		}
	}
	if got := created.Load(); got != 1 {
		t.Fatalf("created %d jobs, want 1", got)
	}
	_, _, err := q.Enqueue(Submission{Kind: "echo", Payload: json.RawMessage(`{"b":2,"a":1}`)}, "same-key")
	if err != nil {
		t.Fatalf("canonical reordered keys should replay: %v", err)
	}
	_, _, err = q.Enqueue(Submission{Kind: "echo", Payload: json.RawMessage(`{"a":99}`)}, "same-key")
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("want ErrConflict, got %v", err)
	}
	if len(q.List(100)) != 1 {
		t.Fatal("expected only one job")
	}
}

func TestManyWorkersNeverClaimSameJob(t *testing.T) {
	q := NewQueue()
	const count = 100
	ids := make(map[string]bool, count)
	for i := 0; i < count; i++ {
		j := mustEnqueue(t, q, "echo", 3)
		ids[j.ID] = true
	}
	var wg sync.WaitGroup
	seen := make(chan string, count)
	errs := make(chan error, count)
	for w := 0; w < 12; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for {
				lease, ok, err := q.Claim(fmt.Sprintf("worker-%d", worker))
				if err != nil {
					errs <- err
					return
				}
				if !ok {
					return
				}
				seen <- lease.Job.ID
				if err = q.Complete(lease.Job.ID, lease.Token, json.RawMessage(`{"ok":true}`)); err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(seen)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	for id := range seen {
		if !ids[id] {
			t.Fatalf("duplicate or unexpected claim %s", id)
		}
		delete(ids, id)
	}
	if len(ids) != 0 {
		t.Fatalf("%d jobs not processed", len(ids))
	}
	for _, j := range q.List(100) {
		if j.State != Succeeded || j.Attempts != 1 {
			t.Fatalf("bad finished job: %#v", j)
		}
	}
}

func TestExpiredLeasesAreFencedAndRetried(t *testing.T) {
	now, advance := fakeClock()
	q := NewQueue(WithClock(now), WithLeaseDuration(5*time.Second), WithRetryBase(time.Second))
	j := mustEnqueue(t, q, "echo", 2)
	first, ok, err := q.Claim("worker-A")
	if err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}
	advance(6 * time.Second)
	_, ok, err = q.Claim("worker-B") // expires first attempt; schedules 1s retry
	if err != nil || ok {
		t.Fatalf("should be in retry backoff: %v %v", ok, err)
	}
	if err = q.Complete(j.ID, first.Token, json.RawMessage(`true`)); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale ack accepted: %v", err)
	}
	advance(time.Second)
	second, ok, err := q.Claim("worker-B")
	if err != nil || !ok {
		t.Fatalf("reclaim failed: %v %v", ok, err)
	}
	if first.Token == second.Token || second.Job.Attempts != 2 {
		t.Fatal("token fencing or attempt increment failed")
	}
	if err = q.Complete(j.ID, second.Token, json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	final, err := q.Get(j.ID)
	if err != nil || final.State != Succeeded {
		t.Fatalf("unexpected final state %#v %v", final, err)
	}
	if final.WorkerID != "" || final.LeaseUntil != nil {
		t.Fatal("lease should be cleared")
	}
}

func TestRetriesAreBoundedAndBackedOff(t *testing.T) {
	now, advance := fakeClock()
	q := NewQueue(WithClock(now), WithRetryBase(time.Second))
	j := mustEnqueue(t, q, "fail", 2)
	first, ok, err := q.Claim("w")
	if !ok || err != nil {
		t.Fatal(err)
	}
	if err = q.Fail(j.ID, first.Token, errors.New("private stack trace")); err != nil {
		t.Fatal(err)
	}
	middle, _ := q.Get(j.ID)
	if middle.State != Queued || middle.LastError != "handler execution failed" {
		t.Fatalf("bad failure state: %#v", middle)
	}
	if _, ok, _ := q.Claim("w"); ok {
		t.Fatal("claimed during backoff")
	}
	advance(time.Second)
	second, ok, err := q.Claim("w")
	if !ok || err != nil {
		t.Fatal(err)
	}
	if err = q.Fail(j.ID, second.Token, errors.New("error")); err != nil {
		t.Fatal(err)
	}
	dead, _ := q.Get(j.ID)
	if dead.State != Dead || dead.Attempts != 2 {
		t.Fatalf("expected dead after 2 attempts, got %#v", dead)
	}
	advance(time.Hour)
	if _, ok, _ := q.Claim("w"); ok {
		t.Fatal("resurrected dead job")
	}
}

func TestMaxAttemptLeaseExpiryBecomesDead(t *testing.T) {
	now, advance := fakeClock()
	q := NewQueue(WithClock(now), WithLeaseDuration(time.Second))
	j := mustEnqueue(t, q, "echo", 1)
	if _, ok, _ := q.Claim("w"); !ok {
		t.Fatal("no claim")
	}
	advance(2 * time.Second)
	if _, ok, _ := q.Claim("w2"); ok {
		t.Fatal("attempt budget exceeded")
	}
	final, _ := q.Get(j.ID)
	if final.State != Dead {
		t.Fatalf("want dead: %#v", final)
	}
}

func TestRequestAndSnapshotValidation(t *testing.T) {
	q := NewQueue()
	for _, r := range []Submission{
		{Kind: "", Payload: json.RawMessage(`{}`)},
		{Kind: "echo", Payload: json.RawMessage(`{bad}`)},
		{Kind: "echo", Payload: json.RawMessage(`{}`), MaxAttempts: 9},
	} {
		if _, _, err := q.Enqueue(r, ""); !errors.Is(err, ErrInvalid) {
			t.Fatalf("should reject %#v: %v", r, err)
		}
	}
	j := mustEnqueue(t, q, "echo", 3)
	j.Payload[0] = 'x'
	original, err := q.Get(j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(original.Payload) {
		t.Fatal("caller mutated queue memory")
	}
	if err = q.Complete(original.ID, "stolen", json.RawMessage(`true`)); !errors.Is(err, ErrLeaseLost) {
		t.Fatal("invalid token accepted")
	}
	if _, err = q.Get("missing"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}
