package web

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"strings"
	"time"

	"dataset-processor/internal/durable"
)

func NewDurable(password, host string, r *durable.Runtime) (*Server, error) {
	s, e := New(password, host)
	if e != nil {
		return nil, e
	}
	if r == nil || r.Owner == "" {
		return nil, errors.New("durable owner runtime required")
	}
	s.runtime = r
	return s, nil
}
func safeReturn(path string) string {
	if !strings.HasPrefix(path, "/jobs/") {
		return "/"
	}
	id := strings.TrimPrefix(path, "/jobs/")
	if len(id) != 48 {
		return "/"
	}
	for _, c := range id {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return "/"
		}
	}
	return path
}
func (s *Server) submitDurable(w http.ResponseWriter, r *http.Request, csrf string, mr *multipart.Reader, part *multipart.Part) {
	b, e := io.ReadAll(io.LimitReader(part, MaxUpload+1))
	if e != nil || len(b) > MaxUpload {
		s.render(w, 413, view{SignedIn: true, CSRF: csrf, Error: "File exceeds 8 MiB or upload was interrupted. No new input was admitted."})
		return
	}
	if extra, e := mr.NextPart(); e != io.EOF || extra != nil {
		s.render(w, 400, view{SignedIn: true, CSRF: csrf, Error: "Unexpected upload fields. No input admitted."})
		return
	}
	key := r.URL.Query().Get("key")
	if key == "" || len(key) > 128 {
		http.Error(w, "Missing submission key; reload the form", 400)
		return
	}
	j, e := s.runtime.Submit(r.Context(), key, b)
	if e != nil && j.ID == "" {
		status := 503
		message := "Processing services unavailable. Retry with the same submission key."
		if errors.Is(e, durable.ErrCapacity) {
			status = 429
			w.Header().Set("Retry-After", "30")
			message = "Capacity full. No job admitted. Wait for active work to finish or inspect retained storage."
		}
		if errors.Is(e, durable.ErrConflict) {
			status = 409
			message = "Submission key already belongs to different input. Reload to start another job."
		}
		s.render(w, status, view{SignedIn: true, CSRF: csrf, RequestKey: key, Error: message})
		return
	}
	// An admitted ambiguous upload has a durable identity and reconciliation path.
	http.Redirect(w, r, "/jobs/"+j.ID, 303)
}
func stage(state string) string {
	switch state {
	case "uploading":
		return "Waiting for upload reconciliation"
	case "preparing":
		return "Preparing records"
	case "processing":
		return "Processing · committed progress"
	case "finalizing":
		return "Finalizing integrity checks"
	case "succeeded":
		return "Complete · integrity checked"
	case "cancelled":
		return "Cancelled · no result published"
	case "failed":
		return "Failed · no result published"
	}
	return "Waiting for recovery"
}
func (s *Server) job(w http.ResponseWriter, r *http.Request, csrf string) {
	pieces := strings.Split(strings.TrimPrefix(r.URL.Path, "/jobs/"), "/")
	if len(pieces) > 2 || len(pieces[0]) != 48 {
		http.NotFound(w, r)
		return
	}
	j, e := s.runtime.Store.Get(r.Context(), s.runtime.Owner, pieces[0])
	if e != nil {
		if errors.Is(e, durable.ErrNotFound) {
			http.NotFound(w, r)
		} else {
			http.Error(w, "Job status temporarily unavailable; retry this URL", 503)
		}
		return
	}
	if len(pieces) == 1 && r.Method == "GET" {
		v := view{SignedIn: true, CSRF: csrf, Job: &j, Stage: stage(j.State), CanCancel: j.State != "succeeded" && j.State != "failed" && j.State != "cancelled", Downloads: j.State == "succeeded" && time.Now().Before(j.Expires)}
		s.render(w, 200, v)
		return
	}
	if len(pieces) != 2 {
		http.NotFound(w, r)
		return
	}
	if pieces[1] == "status" && r.Method == "GET" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"state": j.State, "stage": stage(j.State), "revision": j.Revision, "committed": j.Committed, "parts": j.Parts, "accepted": j.Accepted, "rejected": j.Rejected, "error": j.Error, "updated": j.Updated.Format("2006-01-02 15:04:05 MST")})
		return
	}
	if pieces[1] == "cancel" && r.Method == "POST" {
		r.Body = http.MaxBytesReader(w, r.Body, 4096)
		if r.ParseForm() != nil || !equal(r.FormValue("csrf"), csrf) {
			http.Error(w, "Invalid request token", 403)
			return
		}
		e = s.runtime.Store.Cancel(r.Context(), s.runtime.Owner, j.ID)
		if e != nil {
			if errors.Is(e, durable.ErrConflict) {
				http.Error(w, "Job already reached a terminal outcome. Reload its page.", 409)
			} else {
				http.Error(w, "Cancellation outcome unknown. Reload to reconnect.", 503)
			}
			return
		}
		http.Redirect(w, r, "/jobs/"+j.ID, 303)
		return
	}
	if r.Method != "GET" {
		http.NotFound(w, r)
		return
	}
	name := pieces[1]
	if name != "clean.csv" && name != "rejected.csv" && name != "manifest.json" {
		http.NotFound(w, r)
		return
	}
	select {
	case s.downloads <- struct{}{}:
		defer func() { <-s.downloads }()
	default:
		w.Header().Set("Retry-After", "5")
		http.Error(w, "Download capacity full; retry in five seconds", 429)
		return
	}
	// Preflight the immutable selected versions before sending headers. At this
	// bounded milestone a second pass streams the verified parts to the response.
	if e = s.runtime.Download(r.Context(), j, name, io.Discard); e != nil {
		s.downloadError(w, e)
		return
	}
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	if name == "manifest.json" {
		w.Header().Set("Content-Type", "application/json")
	}
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": name}))
	if name == "manifest.json" {
		var b bytes.Buffer
		_ = s.runtime.Download(r.Context(), j, name, &b)
		w.Write(b.Bytes())
		return
	}
	if e = s.runtime.Download(r.Context(), j, name, w); e != nil {
		panic(http.ErrAbortHandler)
	}
}
func (s *Server) downloadError(w http.ResponseWriter, e error) {
	switch {
	case errors.Is(e, durable.ErrExpired):
		http.Error(w, "RESULT_EXPIRED: Downloads expired. Job counts remain available.", 410)
	case errors.Is(e, durable.ErrIntegrity):
		http.Error(w, "INTEGRITY_ERROR: A committed object is unavailable or corrupt. Inspect job diagnostics.", 500)
	case errors.Is(e, durable.ErrConflict):
		http.Error(w, "Result not published for this job", 409)
	default:
		http.Error(w, "STORAGE_UNAVAILABLE: Retry after storage recovers", 503)
	}
}
