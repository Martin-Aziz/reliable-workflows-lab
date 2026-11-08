package jobs

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"
)

// Store is the common contract for in-memory and durable queue implementations.
// Cancellation propagates across HTTP requests and worker database operations.
type Store interface {
	Enqueue(context.Context, Submission, string) (Job, bool, error)
	Get(context.Context, string) (Job, error)
	List(context.Context, int) ([]Job, error)
	Stats(context.Context) (Stats, error)
	Claim(context.Context, string) (Lease, bool, error)
	Complete(context.Context, string, string, json.RawMessage) error
	Fail(context.Context, string, string, error) error
	Renew(context.Context, string, string) error
	Cancel(context.Context, string) (Job, error)
	Ready(context.Context) error
}

// MemoryStore adapts the tested Phase 1 implementation to the shared interface.
// In-memory processing is intentionally ephemeral; it does not survive restart.
type MemoryStore struct{ Queue *Queue }

func (m MemoryStore) Enqueue(_ context.Context, r Submission, k string) (Job, bool, error) {
	return m.Queue.Enqueue(r, k)
}
func (m MemoryStore) Get(_ context.Context, id string) (Job, error)          { return m.Queue.Get(id) }
func (m MemoryStore) List(_ context.Context, n int) ([]Job, error)           { return m.Queue.List(n), nil }
func (m MemoryStore) Stats(_ context.Context) (Stats, error)                 { return m.Queue.Stats(), nil }
func (m MemoryStore) Claim(_ context.Context, w string) (Lease, bool, error) { return m.Queue.Claim(w) }
func (m MemoryStore) Complete(_ context.Context, id, t string, r json.RawMessage) error {
	return m.Queue.Complete(id, t, r)
}
func (m MemoryStore) Fail(_ context.Context, id, t string, e error) error {
	return m.Queue.Fail(id, t, e)
}
func (m MemoryStore) Ready(ctx context.Context) error { return ctx.Err() }
func (m MemoryStore) Renew(_ context.Context, id, token string) error { return m.Queue.Renew(id, token) }
func (m MemoryStore) Cancel(_ context.Context, id string) (Job, error) { return m.Queue.Cancel(id) }

// NormalizeSubmission ensures both storage backends enforce identical validation
// and canonical idempotency fingerprints. JSON object ordering is normalized.
func NormalizeSubmission(req Submission, key string) (json.RawMessage, [32]byte, int, error) {
	var zero [32]byte
	if req.Kind == "" || !json.Valid(req.Payload) || len(req.Payload) > 64*1024 || len(key) > 128 {
		return nil, zero, 0, ErrInvalid
	}
	attempts := req.MaxAttempts
	if attempts == 0 {
		attempts = 3
	}
	if attempts < 1 || attempts > 8 {
		return nil, zero, 0, ErrInvalid
	}
	var normalized any
	if err := json.Unmarshal(req.Payload, &normalized); err != nil {
		return nil, zero, 0, ErrInvalid
	}
	canonical, err := json.Marshal(normalized)
	if err != nil {
		return nil, zero, 0, err
	}
	fingerprint := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%s", req.Kind, attempts, canonical)))
	return canonical, fingerprint, attempts, nil
}

// ProcessWorkerID identifies a worker across independent processes. Tokens,
// rather than worker names, are the authority for completing a job.
var workerIdentity = struct {
	sync.Once
	id string
}{}

func ProcessWorkerID(index int) string {
	workerIdentity.Do(func() {
		id, err := NewID()
		if err != nil { // An identity is descriptive, not authorization.
			id = strconv.Itoa(os.Getpid())
		}
		workerIdentity.id = id
	})
	return "worker-" + workerIdentity.id + "-" + strconv.Itoa(index)
}
