// Package downstream exposes an independent consumer as its own HTTP service.
// It is a separate process from the source database and speaks to it only over
// the public /changes endpoints.
package downstream

import (
	"encoding/json"
	"errors"
	"io"
	"lamina/internal/consumer"
	"lamina/internal/query"
	"log/slog"
	"net/http"
	"time"
)

type Server struct {
	sink *consumer.Downstream
	mux  *http.ServeMux
}

func New(sink *consumer.Downstream) *Server {
	s := &Server{sink: sink, mux: http.NewServeMux()}
	s.mux.HandleFunc("GET /v1/lamina/health", s.health)
	s.mux.HandleFunc("GET /v1/lamina/status", s.status)
	s.mux.HandleFunc("GET /v1/lamina/source", s.source)
	s.mux.HandleFunc("POST /v1/lamina/sync", s.sync)
	s.mux.HandleFunc("POST /v1/lamina/reinitialize", s.reinitialize)
	s.mux.HandleFunc("POST /v1/lamina/query", s.query)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	s.mux.ServeHTTP(w, r)
	slog.Debug("downstream request", "method", r.Method, "path", r.URL.Path, "elapsed", time.Since(started))
}

func respond(w http.ResponseWriter, status int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		http.Error(w, "response encoding failed", 500)
		return
	}
	w.WriteHeader(status)
	_, _ = w.Write(b)
}

func respondError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	code := "internal"
	message := err.Error()
	var domain *consumer.Error
	if errors.As(err, &domain) {
		code = domain.Code
		switch code {
		case "invalid":
			status = http.StatusBadRequest
		case "not_initialized", "stale_cursor", "identity_changed", "discontinuity":
			status = http.StatusConflict
		case "capacity":
			status = http.StatusTooManyRequests
		case "unavailable":
			status = http.StatusServiceUnavailable
		}
	} else {
		slog.Error("downstream request failed", "error", err)
		message = "internal downstream error"
	}
	respond(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		respondError(w, &consumer.Error{Code: "invalid", Message: "invalid request: " + err.Error()})
		return false
	}
	var trailing any
	if err := d.Decode(&trailing); err != io.EOF {
		respondError(w, &consumer.Error{Code: "invalid", Message: "exactly one JSON value required"})
		return false
	}
	return true
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	status, err := s.sink.Status()
	if err != nil {
		respondError(w, err)
		return
	}
	respond(w, 200, map[string]any{"status": "ready", "initialized": status.Initialized, "watermark": status.Watermark})
}

func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	status, err := s.sink.Status()
	if err != nil {
		respondError(w, err)
		return
	}
	respond(w, 200, status)
}

func (s *Server) source(w http.ResponseWriter, r *http.Request) {
	head, epoch, err := s.sink.Probe()
	if err != nil {
		respondError(w, err)
		return
	}
	status, err := s.sink.Status()
	if err != nil {
		respondError(w, err)
		return
	}
	respond(w, 200, map[string]any{"head": head, "epoch": epoch, "epoch_matches": epoch == status.Epoch,
		"behind_records": head - min(head, status.Watermark), "watermark": status.Watermark})
}

func (s *Server) sync(w http.ResponseWriter, r *http.Request) {
	var in struct {
		MaxPages   int     `json:"max_pages"`
		PageLimit  int     `json:"page_limit"`
		ReplayFrom *uint64 `json:"replay_from"`
	}
	if r.ContentLength != 0 {
		if !decode(w, r, &in) {
			return
		}
	}
	out, err := s.sink.Sync(consumer.SyncOptions{MaxPages: in.MaxPages, PageLimit: in.PageLimit, ReplayFrom: in.ReplayFrom})
	if err != nil {
		// A failed page never advances the durable watermark; report both the
		// error and the watermark that remains valid.
		status, _ := s.sink.Status()
		respond(w, errorStatus(err), map[string]any{"error": map[string]string{"code": errorCode(err), "message": err.Error()}, "watermark": status.Watermark, "digest": status.Digest})
		return
	}
	respond(w, 200, out)
}

func (s *Server) reinitialize(w http.ResponseWriter, r *http.Request) {
	var in struct {
		At *uint64 `json:"at"`
	}
	if r.ContentLength != 0 {
		if !decode(w, r, &in) {
			return
		}
	}
	out, err := s.sink.Reinitialize(in.At)
	if err != nil {
		respondError(w, err)
		return
	}
	respond(w, 200, out)
}

func (s *Server) query(w http.ResponseWriter, r *http.Request) {
	var in query.Request
	if !decode(w, r, &in) {
		return
	}
	out, err := s.sink.Query(in)
	if err != nil {
		respondError(w, err)
		return
	}
	respond(w, 200, out)
}

func errorCode(err error) string {
	var domain *consumer.Error
	if errors.As(err, &domain) {
		return domain.Code
	}
	return "internal"
}

func errorStatus(err error) int {
	var domain *consumer.Error
	if !errors.As(err, &domain) {
		return 500
	}
	switch domain.Code {
	case "invalid":
		return 400
	case "not_initialized", "stale_cursor", "identity_changed", "discontinuity":
		return 409
	case "capacity":
		return 429
	case "unavailable":
		return 503
	}
	return 500
}
