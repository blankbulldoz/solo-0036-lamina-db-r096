package transport

import (
	"lamina/internal/engine"
	"net/http"
	"strconv"
	"strings"
)

func optionalUint(r *http.Request, name string) (*uint64, error) {
	value := r.URL.Query().Get(name)
	if value == "" {
		return nil, nil
	}
	n, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return nil, &engine.Error{Code: "invalid", Message: "invalid " + name}
	}
	return &n, nil
}

func (s *Server) changes(w http.ResponseWriter, r *http.Request) {
	after := uint64(0)
	limit := 100
	var err error
	if value := r.URL.Query().Get("after"); value != "" {
		after, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			respondError(w, &engine.Error{Code: "invalid", Message: "invalid after"})
			return
		}
	}
	if value := r.URL.Query().Get("limit"); value != "" {
		limit, err = strconv.Atoi(value)
		if err != nil {
			respondError(w, &engine.Error{Code: "invalid", Message: "invalid limit"})
			return
		}
	}
	out, err := s.db.Changes(after, limit)
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, out)
}

// digest answers whether a claimed (sequence, digest) still identifies the
// retained history. A consumer that is reconnecting after a disconnect uses it
// to distinguish "same source, I can resume" from "same numbers, rebuilt
// history, I must reinitialize".
func (s *Server) digest(w http.ResponseWriter, r *http.Request) {
	value := r.URL.Query().Get("sequence")
	sequence, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		respondError(w, &engine.Error{Code: "invalid", Message: "invalid sequence"})
		return
	}
	digest, ok, err := s.db.DigestAt(sequence)
	if err != nil {
		respondError(w, err)
		return
	}
	identity := s.db.Identity()
	if !ok {
		write(w, 410, map[string]any{"sequence": sequence, "retained": false, "source_id": identity.SourceID, "epoch": identity.Epoch})
		return
	}
	claimed := strings.ToLower(r.URL.Query().Get("digest"))
	write(w, 200, map[string]any{"sequence": sequence, "digest": digest, "retained": true, "match": claimed == "" || claimed == digest, "source_id": identity.SourceID, "epoch": identity.Epoch})
}

func (s *Server) ack(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Consumer  string `json:"consumer"`
		Watermark uint64 `json:"watermark"`
		Digest    string `json:"digest"`
	}
	if !decode(w, r, &in) {
		return
	}
	out, err := s.db.Acknowledge(in.Consumer, in.Watermark, in.Digest)
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, out)
}

func (s *Server) consumers(w http.ResponseWriter, r *http.Request) {
	list, retention, err := s.db.Consumers()
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, map[string]any{"consumers": list, "retention": retention})
}

func (s *Server) retention(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		out, err := s.db.RetentionState()
		if err != nil {
			respondError(w, err)
			return
		}
		write(w, 200, out)
		return
	}
	var in struct {
		MaxBytes      *int64  `json:"max_bytes"`
		MaxLagRecords *uint64 `json:"max_lag_records"`
		MaxLagSeconds *uint64 `json:"max_lag_seconds"`
	}
	if !decode(w, r, &in) {
		return
	}
	current, err := s.db.RetentionState()
	if err != nil {
		respondError(w, err)
		return
	}
	maxBytes := current.MaxBytes
	maxLagRecords := current.MaxLagRecords
	maxLagSeconds := current.MaxLagSeconds
	if in.MaxBytes != nil {
		maxBytes = *in.MaxBytes
	}
	if in.MaxLagRecords != nil {
		maxLagRecords = *in.MaxLagRecords
	}
	if in.MaxLagSeconds != nil {
		maxLagSeconds = *in.MaxLagSeconds
	}
	out, err := s.db.SetRetention(maxBytes, maxLagRecords, maxLagSeconds)
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, out)
}

func (s *Server) identity(w http.ResponseWriter, r *http.Request) {
	stats, err := s.db.Stats()
	if err != nil {
		respondError(w, err)
		return
	}
	identity := s.db.Identity()
	write(w, 200, map[string]any{"source_id": identity.SourceID, "epoch": identity.Epoch, "origin": identity.Origin, "sequence": stats.Sequence, "journal_base": stats.JournalBase, "change_base": stats.ChangeBase, "change_head": stats.ChangeHead, "head_digest": s.db.HeadDigest()})
}

func (s *Server) bootstrap(w http.ResponseWriter, r *http.Request) {
	at, err := optionalUint(r, "at")
	if err == nil {
		var out engine.Bootstrap
		out, err, _ = s.db.Bootstrap(at)
		if err == nil {
			write(w, 200, out)
			return
		}
	}
	respondError(w, err)
}
