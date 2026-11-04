package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Martin-Aziz/reliable-workflows-lab/internal/jobs"
)

func request(t *testing.T, h http.Handler, method, path, body, key string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func TestCreateGetListAndIdempotency(t *testing.T) {
	q := jobs.NewQueue()
	h := (&API{Store: jobs.MemoryStore{Queue: q}}).Handler()
	body := `{"kind":"echo","payload":{"name":"test"}}`
	a := request(t, h, "POST", "/v1/jobs", body, "id-1")
	if a.Code != http.StatusCreated {
		t.Fatalf("create status %d: %s", a.Code, a.Body.String())
	}
	if !strings.HasPrefix(a.Header().Get("Location"), "/v1/jobs/") {
		t.Fatal("missing location")
	}
	var job jobs.Job
	if err := json.Unmarshal(a.Body.Bytes(), &job); err != nil {
		t.Fatal(err)
	}
	b := request(t, h, "POST", "/v1/jobs", body, "id-1")
	if b.Code != 200 || !bytes.Contains(b.Body.Bytes(), []byte(job.ID)) {
		t.Fatalf("replay failed: %d %s", b.Code, b.Body)
	}
	c := request(t, h, "POST", "/v1/jobs", `{"kind":"echo","payload":{"other":2}}`, "id-1")
	if c.Code != 409 {
		t.Fatalf("expected 409 got %d", c.Code)
	}
	get := request(t, h, "GET", "/v1/jobs/"+job.ID, "", "")
	if get.Code != 200 || get.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("get failed")
	}
	list := request(t, h, "GET", "/v1/jobs?limit=10", "", "")
	if list.Code != 200 {
		t.Fatal("list failed")
	}
	var result struct {
		Jobs []jobs.Job `json:"jobs"`
	}
	if err := json.Unmarshal(list.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Jobs) != 1 || result.Jobs[0].ID != job.ID {
		t.Fatalf("list wrong: %#v", result.Jobs)
	}
}

func TestRejectsInvalidInputs(t *testing.T) {
	h := (&API{Store: jobs.MemoryStore{Queue: jobs.NewQueue()}}).Handler()
	cases := []struct {
		method, path, body string
		code               int
	}{
		{"POST", "/v1/jobs", `{"kind":"shell","payload":{}}`, 400},
		{"POST", "/v1/jobs", `{"kind":"echo","payload":{invalid}}`, 400},
		{"POST", "/v1/jobs", `{"kind":"echo","payload":{},"extra":true}`, 400},
		{"POST", "/v1/jobs", `{"kind":"echo","payload":{}} {}`, 400},
		{"POST", "/v1/jobs", `{"kind":"echo","payload":{},"max_attempts":99}`, 400},
		{"POST", "/v1/jobs", `{"kind":"echo","payload":"` + strings.Repeat("a", 70*1024) + `"}`, 400},
		{"GET", "/v1/jobs?limit=-1", "", 400},
		{"GET", "/v1/jobs/missing", "", 404},
		{"DELETE", "/v1/jobs", "", 405},
	}
	for i, c := range cases {
		resp := request(t, h, c.method, c.path, c.body, "")
		if resp.Code != c.code {
			t.Errorf("case %d status %d want %d", i, resp.Code, c.code)
		}
	}
}

type unavailableStore struct{ jobs.Store }

func (unavailableStore) Ready(context.Context) error { return errors.New("unavailable") }

func TestReadinessFailureDoesNotDisableLiveness(t *testing.T) {
	h := (&API{Store: unavailableStore{Store: jobs.MemoryStore{Queue: jobs.NewQueue()}}}).Handler()
	if got := request(t, h, "GET", "/readyz", "", ""); got.Code != 503 {
		t.Fatalf("readiness=%d", got.Code)
	}
	if got := request(t, h, "GET", "/healthz", "", ""); got.Code != 200 {
		t.Fatalf("liveness=%d", got.Code)
	}
}
