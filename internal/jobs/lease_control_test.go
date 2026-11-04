package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestMemoryLeaseRenewAndCancel(t *testing.T) {
	now, advance := fakeClock()
	q := NewQueue(WithClock(now), WithLeaseDuration(5*time.Second))
	queued := mustEnqueue(t, q, "echo", 2)
	stopped, err := q.Cancel(queued.ID)
	if err != nil || stopped.State != Canceled { t.Fatalf("cancel queued: %v %#v", err, stopped) }
	if _, ok, err := q.Claim("worker"); err != nil || ok { t.Fatalf("claimed canceled work: %v %v", ok, err) }
	again, err := q.Cancel(queued.ID)
	if err != nil || !again.UpdatedAt.Equal(stopped.UpdatedAt) { t.Fatalf("cancel replay: %v", err) }

	j := mustEnqueue(t, q, "echo", 2)
	lease, ok, err := q.Claim("worker-A")
	if err != nil || !ok { t.Fatalf("claim: %v %v", ok, err) }
	advance(4*time.Second)
	if err := q.Renew(j.ID, "wrong"); !errors.Is(err, ErrLeaseLost) { t.Fatalf("renew wrong token: %v", err) }
	if err := q.Renew(j.ID, lease.Token); err != nil { t.Fatal(err) }
	advance(4*time.Second)
	if _, ok, err := q.Claim("worker-B"); err != nil || ok { t.Fatalf("renewed lease expired: %v %v", ok, err) }
	canceled, err := q.Cancel(j.ID)
	if err != nil || canceled.State != Canceled { t.Fatalf("cancel running: %v %#v", err, canceled) }
	if err := q.Renew(j.ID, lease.Token); !errors.Is(err, ErrLeaseLost) { t.Fatalf("revoked token renewed: %v", err) }
	if err := q.Complete(j.ID, lease.Token, json.RawMessage(`true`)); !errors.Is(err, ErrLeaseLost) { t.Fatalf("stale completion: %v", err) }
	if err := q.Fail(j.ID, lease.Token, errors.New("test")); !errors.Is(err, ErrLeaseLost) { t.Fatalf("stale failure: %v", err) }
	stats := q.Stats()
	if stats.Canceled != 2 || stats.Total != 2 { t.Fatalf("unexpected stats: %#v", stats) }
	advance(time.Hour)
	if _, ok, _ := q.Claim("worker-C"); ok { t.Fatal("canceled job was reclaimed") }

	done := mustEnqueue(t, q, "echo", 1)
	accepted, ok, err := q.Claim("worker")
	if err != nil || !ok { t.Fatal(err) }
	if err := q.Complete(done.ID, accepted.Token, json.RawMessage(`true`)); err != nil { t.Fatal(err) }
	if _, err := q.Cancel(done.ID); !errors.Is(err, ErrTerminal) { t.Fatalf("terminal cancel: %v", err) }
	if _, err := q.Cancel("unknown"); !errors.Is(err, ErrNotFound) { t.Fatalf("unknown job: %v", err) }
	advance(time.Second)
	if err := q.Renew(done.ID, accepted.Token); !errors.Is(err, ErrLeaseLost) { t.Fatalf("renew succeeded job: %v", err) }
	if err := q.Renew("unknown", accepted.Token); !errors.Is(err, ErrNotFound) { t.Fatalf("renew missing: %v", err) }
}

func TestProcessorHeartbeatsLongTask(t *testing.T) {
	q := NewQueue(WithLeaseDuration(300*time.Millisecond))
	j := mustEnqueue(t, q, "slow", 1)
	started := make(chan struct{})
	done := make(chan error, 1)
	p := &Processor{
		Store: MemoryStore{Queue:q},
		LeaseRenewInterval: 30*time.Millisecond,
		TaskTimeout: 2*time.Second,
		Handlers: map[string]Handler{"slow": func(ctx context.Context, _ json.RawMessage) (json.RawMessage,error) {
			close(started)
			select {
			case <-time.After(650*time.Millisecond):
				return json.RawMessage(`true`), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}},
	}
	go func(){ _,err:=p.ProcessOne(context.Background(),"worker-A");done<-err }()
	<-started
	time.Sleep(430*time.Millisecond) // past the original 300ms lease
	if _, ok, err := q.Claim("worker-B"); err != nil || ok { t.Fatalf("lease not renewed: ok=%v err=%v", ok, err) }
	if err := <-done; err != nil { t.Fatal(err) }
	saved, _ := q.Get(j.ID)
	if saved.State != Succeeded || saved.Attempts != 1 { t.Fatalf("unexpected result: %#v", saved) }
}

func TestProcessorSeesCancellationAfterHeartbeat(t *testing.T) {
	q := NewQueue(WithLeaseDuration(100*time.Millisecond))
	j := mustEnqueue(t, q, "slow", 1)
	started := make(chan struct{})
	done := make(chan error, 1)
	p := &Processor{
		Store: MemoryStore{Queue:q}, LeaseRenewInterval:10*time.Millisecond,
		TaskTimeout:time.Second,
		Handlers: map[string]Handler{"slow":func(ctx context.Context,_ json.RawMessage)(json.RawMessage,error){
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		}},
	}
	go func(){_,err:=p.ProcessOne(context.Background(),"worker");done<-err}()
	<-started
	if _,err:=q.Cancel(j.ID);err!=nil {t.Fatal(err)}
	select {
	case err:=<-done:
		if !errors.Is(err,ErrLeaseLost) { t.Fatalf("expected heartbeat lease loss: %v",err) }
	case <-time.After(time.Second):
		t.Fatal("worker did not observe cancellation")
	}
	saved,_:=q.Get(j.ID)
	if saved.State != Canceled { t.Fatalf("cancel was overwritten: %#v",saved) }
}

func TestProcessorDeadlineRejectsLateSuccess(t *testing.T) {
	q:=NewQueue()
	j:=mustEnqueue(t,q,"slow",1)
	p:=&Processor{Store:MemoryStore{Queue:q},TaskTimeout:20*time.Millisecond,
		Handlers:map[string]Handler{"slow":func(_ context.Context,_ json.RawMessage)(json.RawMessage,error){
			time.Sleep(45*time.Millisecond) // non-cooperative handler
			return json.RawMessage(`true`),nil
		}},
	}
	if _,err:=p.ProcessOne(context.Background(),"worker");err!=nil {t.Fatal(err)}
	saved,_:=q.Get(j.ID)
	if saved.State!=Dead {t.Fatalf("late completion incorrectly accepted: %#v",saved)}
}
