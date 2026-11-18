package transport

import (
	"net/http"
)

func (s *Server) checkpoint(w http.ResponseWriter, r *http.Request) {
	seq, err := s.db.Checkpoint()
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, map[string]uint64{"sequence": seq})
}
func (s *Server) vacuum(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Horizon uint64 `json:"horizon"`
	}
	if !decode(w, r, &in) {
		return
	}
	out, err := s.db.Vacuum(in.Horizon)
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, out)
}
func (s *Server) backup(w http.ResponseWriter, r *http.Request) {
	data, err := s.db.Backup()
	if err != nil {
		respondError(w, err)
		return
	}
	w.Header().Set("Content-Disposition", `attachment; filename="lamina-backup.lmi"`)
	w.WriteHeader(200)
	_, _ = w.Write(data)
}
func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	out, err := s.db.Stats()
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, out)
}
