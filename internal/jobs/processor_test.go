package jobs

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestProcessorSuccessAndFailure(t *testing.T) {
	now, advance := fakeClock()
	q := NewQueue(WithClock(now), WithRetryBase(time.Second))
	p := &Processor{Store: MemoryStore{Queue: q}, Handlers: DefaultHandlers(), TaskTimeout: time.Second}
	sum, _, err := q.Enqueue(Submission{Kind: "sum", Payload: json.RawMessage(`{"numbers":[1,2,3.25]}`)}, "")
	if err != nil {
		t.Fatal(err)
	}
	did, err := p.ProcessOne(context.Background(), "worker")
	if err != nil || !did {
		t.Fatalf("sum not processed: %v %v", did, err)
	}
	done, _ := q.Get(sum.ID)
	if done.State != Succeeded || string(done.Result) != `{"sum":6.25}` {
		t.Fatalf("sum unexpected %#v", done)
	}
	failed, _, err := q.Enqueue(Submission{Kind: "fail", Payload: json.RawMessage(`{}`), MaxAttempts: 2}, "")
	if err != nil {
		t.Fatal(err)
	}
	did, err = p.ProcessOne(context.Background(), "worker")
	if !did || err != nil {
		t.Fatalf("failure handler process: %v %v", did, err)
	}
	advance(time.Second)
	did, err = p.ProcessOne(context.Background(), "worker")
	if !did || err != nil {
		t.Fatalf("retry process: %v %v", did, err)
	}
	final, _ := q.Get(failed.ID)
	if final.State != Dead {
		t.Fatalf("expected dead job: %#v", final)
	}
}

func TestInvalidHandlerResultIsRejected(t *testing.T) {
	q := NewQueue()
	j := mustEnqueue(t, q, "custom", 1)
	p := &Processor{Store: MemoryStore{Queue: q}, Handlers: map[string]Handler{"custom": func(context.Context, json.RawMessage) (json.RawMessage, error) { return []byte(`{not-json}`), nil }}}
	did, err := p.ProcessOne(context.Background(), "w")
	if !did || err != nil {
		t.Fatalf("expected handled invalid result, got %v %v", did, err)
	}
	still, _ := q.Get(j.ID)
	if still.State != Dead || still.LastError != "handler execution failed" {
		t.Fatalf("invalid handler output should exhaust single attempt: %#v", still)
	}
}
