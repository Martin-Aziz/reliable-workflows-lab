package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Martin-Aziz/reliable-workflows-lab/internal/jobs"
)

type API struct{ Store jobs.Store }

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := a.Store.Ready(ctx); err != nil {
			writeError(w, http.StatusServiceUnavailable, "storage unavailable")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	mux.HandleFunc("GET /v1/jobs", a.list)
	mux.HandleFunc("GET /v1/stats", func(w http.ResponseWriter, r *http.Request) {
		stats, err := a.Store.Stats(r.Context())
		if err != nil {
			writeError(w, 503, "storage unavailable")
			return
		}
		writeJSON(w, 200, stats)
	})
	mux.HandleFunc("POST /v1/jobs", a.create)
	mux.HandleFunc("GET /v1/jobs/{id}", a.get)
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func (a *API) create(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 68*1024)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req jobs.Submission
	if err := dec.Decode(&req); err != nil {
		writeError(w, 400, "invalid JSON job request")
		return
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		writeError(w, 400, "request must contain exactly one JSON object")
		return
	}
	// Phase 1's demo worker only supports these explicitly allowed tasks.
	if req.Kind != "echo" && req.Kind != "sum" && req.Kind != "fail" {
		writeError(w, 400, "kind must be echo, sum, or fail")
		return
	}
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	job, existing, err := a.Store.Enqueue(r.Context(), req, key)
	if err != nil {
		switch {
		case errors.Is(err, jobs.ErrConflict):
			writeError(w, 409, "idempotency key conflicts with existing request")
		case errors.Is(err, jobs.ErrInvalid):
			writeError(w, 400, "invalid kind, payload, max_attempts, or idempotency key")
		default:
			writeError(w, 503, "storage unavailable")
		}
		return
	}
	w.Header().Set("Location", "/v1/jobs/"+job.ID)
	if existing {
		writeJSON(w, 200, job)
		return
	}
	writeJSON(w, 201, job)
}

func (a *API) get(w http.ResponseWriter, r *http.Request) {
	job, err := a.Store.Get(r.Context(), r.PathValue("id"))
	if errors.Is(err, jobs.ErrNotFound) {
		writeError(w, 404, "job not found")
		return
	}
	if err != nil {
		writeError(w, 503, "storage unavailable")
		return
	}
	writeJSON(w, 200, job)
}

func (a *API) list(w http.ResponseWriter, r *http.Request) {
	limit := 20
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 100 {
			writeError(w, 400, "limit must be between 1 and 100")
			return
		}
		limit = n
	}
	list, err := a.Store.List(r.Context(), limit)
	if err != nil {
		writeError(w, 503, "storage unavailable")
		return
	}
	writeJSON(w, 200, map[string]any{"jobs": list})
}
