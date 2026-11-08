package httpapi

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/Martin-Aziz/reliable-workflows-lab/internal/jobs"
)

func TestCancellationEndpointAndStatusCounters(t *testing.T) {
	q := jobs.NewQueue()
	h := (&API{Store: jobs.MemoryStore{Queue:q}}).Handler()
	j,_,err := q.Enqueue(jobs.Submission{Kind:"echo",Payload:json.RawMessage(`{}`)},"")
	if err!=nil {t.Fatal(err)}
	path := "/v1/jobs/"+j.ID+"/cancel"
	first:=request(t,h,"POST",path,"","")
	if first.Code != http.StatusOK {t.Fatalf("first cancel: %d %s",first.Code,first.Body.String())}
	var got jobs.Job
	if err:=json.Unmarshal(first.Body.Bytes(),&got);err!=nil||got.State!=jobs.Canceled {t.Fatalf("cancel payload: %#v %v",got,err)}
	if replay:=request(t,h,"POST",path,"",""); replay.Code!=http.StatusOK {t.Fatalf("replay: %d",replay.Code)}
	if missing:=request(t,h,"POST","/v1/jobs/not-found/cancel","","");missing.Code!=http.StatusNotFound {t.Fatalf("missing: %d",missing.Code)}
	s:=request(t,h,"GET","/v1/stats","","")
	var counts jobs.Stats
	if err:=json.Unmarshal(s.Body.Bytes(),&counts);err!=nil||counts.Canceled!=1 {t.Fatalf("stats: %#v %v",counts,err)}

	done,_,err:=q.Enqueue(jobs.Submission{Kind:"echo",Payload:json.RawMessage(`{}`)},"")
	if err!=nil {t.Fatal(err)}
	l,ok,err:=q.Claim("worker")
	if err!=nil||!ok {t.Fatal(err)}
	if err:=q.Complete(done.ID,l.Token,json.RawMessage(`true`));err!=nil {t.Fatal(err)}
	if got:=request(t,h,"POST","/v1/jobs/"+done.ID+"/cancel","",""); got.Code!=http.StatusConflict {t.Fatalf("terminal: %d",got.Code)}
}
