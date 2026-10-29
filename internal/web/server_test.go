package web

import (
	"bytes"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func request(s http.Handler, method, path, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "http://processor.test"+path, strings.NewReader(body))
	r.Header.Set("Origin", "http://processor.test")
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	return w
}
func TestAccessAndOrigin(t *testing.T) {
	s, err := New("test-password", "processor.test")
	if err != nil {
		t.Fatal(err)
	}
	if got := request(s, "GET", "/", " "); got.Code != 200 || !strings.Contains(got.Body.String(), "Sign in") {
		t.Fatal("missing login")
	}
	if got := request(s, "GET", "/sample.csv", ""); got.Code != 401 {
		t.Fatalf("sample unauthenticated: %d", got.Code)
	}
	r := httptest.NewRequest("POST", "http://processor.test/login", strings.NewReader("password=test-password"))
	r.Header.Set("Origin", "http://evil.test")
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatal("cross origin login allowed")
	}
	if got := request(s, "POST", "/login", "password=wrong"); got.Code != 401 {
		t.Fatal("wrong password accepted")
	}
}
func TestUploadAndDownload(t *testing.T) {
	s, _ := New("test-password", "processor.test")
	login := request(s, "POST", "/login", "password="+url.QueryEscape("test-password"))
	if login.Code != http.StatusSeeOther {
		t.Fatalf("login %d", login.Code)
	}
	cookie := login.Result().Cookies()[0]
	pageReq := httptest.NewRequest("GET", "http://processor.test/", nil)
	pageReq.AddCookie(cookie)
	page := httptest.NewRecorder()
	s.ServeHTTP(page, pageReq)
	csrf := s.csrf
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	_ = mw.WriteField("csrf", csrf)
	part, _ := mw.CreateFormFile("file", "sample.csv")
	io.WriteString(part, "transaction_id,occurred_at,currency,amount\na,2026-10-24T00:00:00Z,EUR,1\nb,bad,EUR,1.234\n")
	mw.Close()
	r := httptest.NewRequest("POST", "http://processor.test/process", &body)
	r.Header.Set("Content-Type", mw.FormDataContentType())
	r.Header.Set("Origin", "http://processor.test")
	r.AddCookie(cookie)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "Processing complete") {
		t.Fatalf("upload %d %s", w.Code, w.Body.String())
	}
	if s.result == nil || s.result.manifest.Accepted != 1 || s.result.manifest.Rejected != 1 {
		t.Fatal("incorrect result")
	}
	download := httptest.NewRequest("GET", "http://processor.test/results/"+s.result.id+"/clean.csv", nil)
	download.AddCookie(cookie)
	dw := httptest.NewRecorder()
	s.ServeHTTP(dw, download)
	if dw.Code != 200 || !strings.Contains(dw.Body.String(), "1.00") {
		t.Fatal("download failed")
	}
	noCSRF := httptest.NewRequest("POST", "http://processor.test/process", strings.NewReader(""))
	noCSRF.AddCookie(cookie)
	noCSRF.Header.Set("Origin", "http://processor.test")
	denied := httptest.NewRecorder()
	s.ServeHTTP(denied, noCSRF)
	if denied.Code != 403 {
		t.Fatalf("csrf missing %d", denied.Code)
	}
}

func TestLoginRateLimit(t *testing.T) {
	s, _ := New("test-password", "processor.test")
	for i := 0; i < 5; i++ {
		request(s, "POST", "/login", "password=wrong")
	}
	if got := request(s, "POST", "/login", "password=wrong"); got.Code != 429 {
		t.Fatalf("want 429, got %d", got.Code)
	}
}
func TestMalformedUploadDoesNotPublish(t *testing.T) {
	s, _ := New("test-password", "processor.test")
	login := request(s, "POST", "/login", "password=test-password")
	cookie := login.Result().Cookies()[0]
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	mw.WriteField("csrf", s.csrf)
	part, _ := mw.CreateFormFile("file", "bad.csv")
	io.WriteString(part, "wrong,header\n")
	mw.Close()
	r := httptest.NewRequest("POST", "http://processor.test/process", &body)
	r.AddCookie(cookie)
	r.Header.Set("Origin", "http://processor.test")
	r.Header.Set("Content-Type", mw.FormDataContentType())
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	if w.Code != 422 || s.result != nil {
		t.Fatalf("malformed file published or wrong response %d", w.Code)
	}
}

func TestSameOriginFormsRetainOrigin(t *testing.T) {
	s, _ := New("test-password", "processor.test")
	w := request(s, "GET", "/", "")
	if got := w.Header().Get("Referrer-Policy"); got != "same-origin" {
		t.Fatalf("same-origin form POST must retain Origin, got policy %q", got)
	}
}
