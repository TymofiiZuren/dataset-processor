// Package web exposes the owner-only local reference workspace. It does not
// implement durable jobs, background workers, or cloud integrations yet.
package web

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"html/template"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"dataset-processor/internal/durable"
	"dataset-processor/internal/processor"
)

//go:embed page.html
var pageSource string

const MaxUpload = durable.MaxInput
const sample = "transaction_id,occurred_at,currency,amount\nTX-1001,2026-10-24T10:30:00+01:00,EUR,149.9\nTX-1002,2026-10-24T09:35:00Z,GBP,-24.50\nTX-1003,not-a-date,USD,19.99\nTX-1004,2026-10-24T09:40:00Z,EUR,1.234\nTX-1001,2026-10-24T09:45:00Z,USD,0\nTX-1005,2026-10-24T10:00:00Z,CAD,85\n"

type Server struct {
	loginWindow   time.Time
	loginAttempts int
	host          string
	password      [32]byte
	page          *template.Template
	mu            sync.Mutex
	session, csrf string
	expires       time.Time
	result        *result
	busy          chan struct{}
	runtime       *durable.Runtime
	downloads     chan struct{}
}
type result struct {
	id              string
	manifest        processor.Manifest
	clean, rejected []byte
	expires         time.Time
}
type view struct {
	SignedIn             bool
	CSRF, Error          string
	Result               *resultView
	Durable              bool
	Cloud                bool
	Cluster              bool
	Job                  *durable.Job
	RequestKey, ReturnTo string
	Downloads, CanCancel bool
	Stage                string
}
type resultView struct {
	ID       string
	Manifest processor.Manifest
}

func New(password, host string) (*Server, error) {
	if password == "" || host == "" {
		return nil, errors.New("owner password and exact loopback host required")
	}
	page, err := template.New("page").Parse(pageSource)
	if err != nil {
		return nil, err
	}
	return &Server{host: host, password: sha256.Sum256([]byte(password)), page: page, busy: make(chan struct{}, 1), downloads: make(chan struct{}, 2)}, nil
}
func token() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
func equal(a, b string) bool { return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1 }
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "same-origin")
	w.Header().Set("X-Frame-Options", "DENY")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'; connect-src 'self'")
	if r.Host != s.host {
		http.Error(w, "Untrusted host", http.StatusForbidden)
		return
	}
	if r.Method != "GET" && r.Method != "HEAD" {
		origin, err := url.Parse(r.Header.Get("Origin"))
		if err != nil || origin.Host != s.host || origin.Scheme != "http" {
			http.Error(w, "Untrusted origin", http.StatusForbidden)
			return
		}
	}
	if r.Method == "GET" && r.URL.Path == "/healthz" {
		w.Write([]byte("ok\n"))
		return
	}
	if r.Method == "POST" && r.URL.Path == "/login" {
		s.login(w, r)
		return
	}
	s.mu.Lock()
	cookie, err := r.Cookie("processor_session")
	signedIn := err == nil && s.session != "" && equal(cookie.Value, s.session) && time.Now().Before(s.expires)
	csrf := s.csrf
	s.mu.Unlock()
	if !signedIn {
		if r.Method == "GET" && (r.URL.Path == "/" || s.runtime != nil && safeReturn(r.URL.Path) != "/") {
			s.render(w, 200, view{ReturnTo: safeReturn(r.URL.Path)})
			return
		}
		http.Error(w, "Sign in to the owner workspace", http.StatusUnauthorized)
		return
	}
	if s.runtime != nil && strings.HasPrefix(r.URL.Path, "/jobs/") {
		s.job(w, r, csrf)
		return
	}
	switch {
	case r.Method == "GET" && r.URL.Path == "/":
		s.render(w, 200, view{SignedIn: true, CSRF: csrf})
	case r.Method == "GET" && r.URL.Path == "/sample.csv":
		w.Header().Set("Content-Type", "text/csv; charset=utf-8")
		w.Header().Set("Content-Disposition", `attachment; filename="sample-transactions.csv"`)
		w.Write([]byte(sample))
	case r.Method == "POST" && r.URL.Path == "/process":
		s.process(w, r, csrf)
	case r.Method == "POST" && r.URL.Path == "/logout":
		r.Body = http.MaxBytesReader(w, r.Body, 4096)
		if r.ParseForm() != nil || !equal(r.FormValue("csrf"), csrf) {
			http.Error(w, "Invalid request token", 403)
			return
		}
		s.mu.Lock()
		s.session = ""
		s.result = nil
		s.mu.Unlock()
		http.SetCookie(w, &http.Cookie{Name: "processor_session", Value: "", Path: "/", MaxAge: -1, HttpOnly: true, SameSite: http.SameSiteStrictMode})
		http.Redirect(w, r, "/", 303)
	case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/results/"):
		s.download(w, r)
	default:
		http.NotFound(w, r)
	}
}
func (s *Server) render(w http.ResponseWriter, status int, v view) {
	v.Durable = s.runtime != nil
	if s.runtime != nil && s.runtime.Objects != nil {
		v.Cloud = s.runtime.Objects.Cloud
		v.Cluster = s.runtime.Objects.Cluster
	}
	if v.SignedIn && v.RequestKey == "" {
		v.RequestKey = durable.ID()
	}
	var b bytes.Buffer
	if err := s.page.Execute(&b, v); err != nil {
		http.Error(w, "Unable to render workspace", 500)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	w.Write(b.Bytes())
}
func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	if time.Since(s.loginWindow) >= time.Minute {
		s.loginWindow = time.Now()
		s.loginAttempts = 0
	}
	if s.loginAttempts >= 5 {
		s.mu.Unlock()
		w.Header().Set("Retry-After", "60")
		http.Error(w, "Too many sign-in attempts. Try again in one minute.", 429)
		return
	}
	s.loginAttempts++
	s.mu.Unlock()

	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	if r.ParseForm() != nil {
		http.Error(w, "Invalid login request", 400)
		return
	}
	hash := sha256.Sum256([]byte(r.FormValue("password")))
	if subtle.ConstantTimeCompare(hash[:], s.password[:]) != 1 {
		s.render(w, 401, view{Error: "Password not recognized. Use the private password file created when this server started."})
		return
	}
	session, e := token()
	if e != nil {
		http.Error(w, "Session unavailable", 503)
		return
	}
	csrf, e := token()
	if e != nil {
		http.Error(w, "Session unavailable", 503)
		return
	}
	s.mu.Lock()
	s.session = session
	s.csrf = csrf
	s.expires = time.Now().Add(time.Hour)
	s.mu.Unlock()
	http.SetCookie(w, &http.Cookie{Name: "processor_session", Value: session, Path: "/", HttpOnly: true, SameSite: http.SameSiteStrictMode, MaxAge: 3600})
	http.Redirect(w, r, safeReturn(r.FormValue("return_to")), 303)
}
func (s *Server) process(w http.ResponseWriter, r *http.Request, csrf string) {
	// Only one request may allocate upload/output buffers at a time.
	select {
	case s.busy <- struct{}{}:
		defer func() { <-s.busy }()
	default:
		message := "A file is already processing. Wait for it to finish, then retry."
		if s.runtime != nil {
			message = "Another upload is in progress. Retry once it finishes; no job admitted."
		}
		w.Header().Set("Retry-After", "5")
		s.render(w, 429, view{SignedIn: true, CSRF: csrf, Error: message})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, MaxUpload+(64<<10))
	// MultipartReader avoids spooling uploaded data to temporary files.
	mr, err := r.MultipartReader()
	if err != nil {
		http.Error(w, "Invalid request token or upload", 403)
		return
	}
	part, err := mr.NextPart()
	if err != nil || part.FormName() != "csrf" {
		http.Error(w, "Invalid request token", 403)
		return
	}
	supplied, err := io.ReadAll(io.LimitReader(part, 257))
	if err != nil || !equal(string(supplied), csrf) {
		http.Error(w, "Invalid request token", 403)
		return
	}
	part, err = mr.NextPart()
	if err != nil || part.FormName() != "file" {
		s.render(w, 400, view{SignedIn: true, CSRF: csrf, Error: "Choose a transaction CSV to process."})
		return
	}
	if s.runtime != nil {
		s.submitDurable(w, r, csrf, mr, part)
		return
	}
	var clean, rejected bytes.Buffer
	limits := processor.DefaultLimits()
	limits.MaxFileBytes = MaxUpload
	manifest, err := processor.Process(&contextReader{ctx: r.Context(), r: part}, &clean, &rejected, limits)
	if err != nil {
		message := "Processing failed. No result was published. Check the file and retry."
		var invalid *processor.Error
		if errors.As(err, &invalid) {
			message = invalid.Error() + ". No result was published. Correct the file and start again."
		}
		s.render(w, 422, view{SignedIn: true, CSRF: csrf, Error: message})
		return
	}
	if extra, e := mr.NextPart(); e != io.EOF || extra != nil {
		s.render(w, 400, view{SignedIn: true, CSRF: csrf, Error: "Unexpected upload fields. No result was published."})
		return
	}
	id, err := token()
	if err != nil {
		http.Error(w, "Unable to publish result", 503)
		return
	}
	result := &result{id: id, manifest: manifest, clean: clean.Bytes(), rejected: rejected.Bytes(), expires: time.Now().Add(15 * time.Minute)}
	s.mu.Lock()
	s.result = result
	s.mu.Unlock()
	s.render(w, 200, view{SignedIn: true, CSRF: csrf, Result: &resultView{ID: id, Manifest: manifest}})
}
func (s *Server) download(w http.ResponseWriter, r *http.Request) {
	pieces := strings.Split(strings.TrimPrefix(r.URL.Path, "/results/"), "/")
	if len(pieces) != 2 {
		http.NotFound(w, r)
		return
	}
	s.mu.Lock()
	result := s.result
	if result != nil && time.Now().After(result.expires) {
		s.result = nil
		result = nil
	}
	s.mu.Unlock()
	if result == nil || pieces[0] != result.id {
		http.Error(w, "Result expired or replaced. Process the file again.", 410)
		return
	}
	var data []byte
	switch pieces[1] {
	case "clean.csv":
		data = result.clean
	case "rejected.csv":
		data = result.rejected
	case "manifest.json":
		data, _ = json.MarshalIndent(result.manifest, "", "  ")
	default:
		http.NotFound(w, r)
		return
	}
	contentType := "text/csv; charset=utf-8"
	if pieces[1] == "manifest.json" {
		contentType = "application/json"
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": pieces[1]}))
	w.Write(data)
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (c *contextReader) Read(b []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(b)
}
