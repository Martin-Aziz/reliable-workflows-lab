//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Martin-Aziz/reliable-workflows-lab/internal/jobs"
)

func TestPostgresRenewCancelAndSchemaUpgrade(t *testing.T) {
	a,b,db,_ := integrationStores(t)
	ctx,cancel:=context.WithTimeout(context.Background(),20*time.Second)
	defer cancel()
	var migrationCount int
	if err:=db.QueryRowContext(ctx,`SELECT COUNT(*) FROM workflow_schema_migrations`).Scan(&migrationCount);err!=nil||migrationCount!=2 {
		t.Fatalf("migration tracking: %d %v",migrationCount,err)
	}
	queued,_,err:=a.Enqueue(ctx,jobs.Submission{Kind:"echo",Payload:json.RawMessage(`{}`)},"")
	if err!=nil {t.Fatal(err)}
	canceled,err:=b.Cancel(ctx,queued.ID)
	if err!=nil||canceled.State!=jobs.Canceled {t.Fatalf("queued cancellation: %#v %v",canceled,err)}
	if _,err:=a.Cancel(ctx,queued.ID);err!=nil {t.Fatalf("idempotent cancellation: %v",err)}
	j,_,err:=a.Enqueue(ctx,jobs.Submission{Kind:"echo",Payload:json.RawMessage(`{}`)},"running-key")
	if err!=nil {t.Fatal(err)}
	lease,ok,err:=a.Claim(ctx,"worker-A")
	if err!=nil||!ok||lease.Job.ID!=j.ID {t.Fatalf("claim: %#v %v",lease,err)}
	if err:=a.Renew(ctx,j.ID,"bogus");!errors.Is(err,jobs.ErrLeaseLost){t.Fatalf("bad renewal: %v",err)}
	if err:=a.Renew(ctx,j.ID,lease.Token);err!=nil {t.Fatalf("valid renewal: %v",err)}
	stopped,err:=b.Cancel(ctx,j.ID)
	if err!=nil||stopped.State!=jobs.Canceled {t.Fatalf("cancel lease: %#v %v",stopped,err)}
	if err:=a.Renew(ctx,j.ID,lease.Token);!errors.Is(err,jobs.ErrLeaseLost){t.Fatalf("renew canceled: %v",err)}
	if err:=a.Complete(ctx,j.ID,lease.Token,json.RawMessage(`true`));!errors.Is(err,jobs.ErrLeaseLost){t.Fatalf("complete canceled: %v",err)}
	var outcome string
	if err:=db.QueryRowContext(ctx,`SELECT outcome FROM workflow_attempts WHERE job_id=$1 AND attempt=1`,j.ID).Scan(&outcome);err!=nil||outcome!="canceled"{
		t.Fatalf("attempt outcome %q: %v",outcome,err)
	}
	terminal,_,err:=a.Enqueue(ctx,jobs.Submission{Kind:"echo",Payload:json.RawMessage(`{}`)},"terminal-key")
	if err!=nil {t.Fatal(err)}
	doneLease,ok,err:=a.Claim(ctx,"worker-B")
	if err!=nil||!ok||doneLease.Job.ID!=terminal.ID {t.Fatalf("claim terminal: %#v %v",doneLease,err)}
	if err:=b.Complete(ctx,terminal.ID,doneLease.Token,json.RawMessage(`true`));err!=nil{t.Fatal(err)}
	if _,err:=a.Cancel(ctx,terminal.ID);!errors.Is(err,jobs.ErrTerminal){t.Fatalf("terminal canceled: %v",err)}
	stats,err:=a.Stats(ctx)
	if err!=nil||stats.Canceled!=2||stats.Succeeded!=1 {t.Fatalf("stats: %#v %v",stats,err)}
}
