package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"
)

type Handler func(context.Context, json.RawMessage) (json.RawMessage, error)

type Processor struct {
	Store       Store
	Handlers    map[string]Handler
	Logger      *slog.Logger
	TaskTimeout time.Duration
}

func DefaultHandlers() map[string]Handler {
	return map[string]Handler{
		"echo": func(_ context.Context, p json.RawMessage) (json.RawMessage, error) { return p, nil },
		"sum": func(_ context.Context, p json.RawMessage) (json.RawMessage, error) {
			var input struct {
				Numbers []float64 `json:"numbers"`
			}
			if err := json.Unmarshal(p, &input); err != nil {
				return nil, errors.New("malformed sum payload")
			}
			if len(input.Numbers) == 0 || len(input.Numbers) > 1000 {
				return nil, errors.New("numbers required (1-1000)")
			}
			total := 0.0
			for _, n := range input.Numbers {
				total += n
				if math.IsInf(total, 0) || math.IsNaN(total) {
					return nil, errors.New("sum overflow")
				}
			}
			return json.Marshal(map[string]float64{"sum": total})
		},
		"fail": func(context.Context, json.RawMessage) (json.RawMessage, error) {
			return nil, errors.New("intentional demo failure")
		},
	}
}

func (p *Processor) ProcessOne(ctx context.Context, workerID string) (bool, error) {
	lease, ok, err := p.Store.Claim(ctx, workerID)
	if err != nil || !ok {
		return false, err
	}
	handler, ok := p.Handlers[lease.Job.Kind]
	if !ok {
		handler = func(context.Context, json.RawMessage) (json.RawMessage, error) {
			return nil, fmt.Errorf("unknown handler")
		}
	}
	timeout := p.TaskTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	taskCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	result, handlerErr := handler(taskCtx, lease.Job.Payload)
	if handlerErr != nil {
		if p.Logger != nil {
			p.Logger.Warn("job failed", "job_id", lease.Job.ID, "kind", lease.Job.Kind, "attempt", lease.Job.Attempts, "error", handlerErr)
		}
		return true, p.Store.Fail(ctx, lease.Job.ID, lease.Token, handlerErr)
	}
	if len(result) == 0 {
		result = json.RawMessage(`null`)
	}
	if !json.Valid(result) || len(result) > 64*1024 {
		if p.Logger != nil {
			p.Logger.Warn("handler returned invalid result", "job_id", lease.Job.ID)
		}
		return true, p.Store.Fail(ctx, lease.Job.ID, lease.Token, errors.New("invalid handler result"))
	}
	err = p.Store.Complete(ctx, lease.Job.ID, lease.Token, result)
	if err == nil && p.Logger != nil {
		p.Logger.Info("job completed", "job_id", lease.Job.ID, "kind", lease.Job.Kind, "attempt", lease.Job.Attempts)
	}
	return true, err
}

func (p *Processor) Run(ctx context.Context, workerID string, poll time.Duration) {
	if poll <= 0 {
		poll = 100 * time.Millisecond
	}
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		processed, err := p.ProcessOne(ctx, workerID)
		if err != nil && p.Logger != nil {
			p.Logger.Warn("worker operation failed", "worker_id", workerID, "error", err)
		}
		if processed {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
