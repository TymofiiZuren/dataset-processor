package web

import (
	"context"
	"github.com/jackc/pgx/v5"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"dataset-processor/internal/durable"
)

func TestDurableJobSurvivesSessionAndServer(t *testing.T) {
	dsn := os.Getenv("PROCESSOR_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("explicit durable integration profile required")
	}
	ctx := context.Background()
	store, e := durable.Open(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	if e = store.Migrate(ctx); e != nil {
		t.Fatal(e)
	}
	r := &durable.Runtime{Store: store, Owner: durable.ID()}
	j, e := store.Admit(ctx, r.Owner, durable.ID(), "test", "local:fixture")
	if e != nil {
		t.Fatal(e)
	}
	defer store.Cancel(ctx, r.Owner, j.ID)
	s, e := NewDurable("test-only-owner-value", "127.0.0.1:8087", r)
	if e != nil {
		t.Fatal(e)
	}
	request := func(s *Server, method, path, body string, cookie *http.Cookie) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, "http://127.0.0.1:8087"+path, strings.NewReader(body))
		req.Header.Set("Origin", "http://127.0.0.1:8087")
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		if cookie != nil {
			req.AddCookie(cookie)
		}
		w := httptest.NewRecorder()
		s.ServeHTTP(w, req)
		return w
	}
	path := "/jobs/" + j.ID
	if w := request(s, "GET", path, "", nil); w.Code != 200 || !strings.Contains(w.Body.String(), "Sign in") {
		t.Fatal("no login return")
	}
	w := request(s, "POST", "/login", url.Values{"password": {"test-only-owner-value"}, "return_to": {path}}.Encode(), nil)
	if w.Code != 303 || w.Header().Get("Location") != path {
		t.Fatal("return URL lost")
	}
	cookie := w.Result().Cookies()[0]
	if w = request(s, "GET", path, "", cookie); w.Code != 200 || !strings.Contains(w.Body.String(), j.ID) {
		t.Fatal("owned job missing")
	}
	if w = request(s, "POST", path+"/cancel", "csrf=invalid", cookie); w.Code != 403 {
		t.Fatal("CSRF weakened")
	}
	if w = request(s, "POST", "/logout", url.Values{"csrf": {s.csrf}}.Encode(), cookie); w.Code != 303 {
		t.Fatal("logout failed")
	}
	got, e := store.Get(ctx, r.Owner, j.ID)
	if e != nil || got.State != "uploading" {
		t.Fatal("logout changed job")
	}
	s2, e := NewDurable("test-only-owner-value", "127.0.0.1:8087", r)
	if e != nil {
		t.Fatal(e)
	}
	if w = request(s2, "GET", path, "", cookie); w.Code != 200 || !strings.Contains(w.Body.String(), "Sign in") {
		t.Fatal("old session valid after restart")
	}
	w = request(s2, "POST", "/login", url.Values{"password": {"test-only-owner-value"}, "return_to": {path}}.Encode(), nil)
	cookie = w.Result().Cookies()[0]
	if w = request(s2, "GET", path, "", cookie); w.Code != 200 || !strings.Contains(w.Body.String(), j.ID) {
		t.Fatal("job lost after API restart")
	}
	if w = request(s2, "GET", "/jobs/"+durable.ID(), "", cookie); w.Code != 404 {
		t.Fatal("missing job disclosed")
	}
}

func TestDurableHTTPExpiryAndBrokenCommittedReference(t *testing.T) {
	dsn := os.Getenv("PROCESSOR_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("explicit integration profile required")
	}
	ctx := context.Background()
	store, e := durable.Open(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	objects, e := durable.NewObjects(ctx, os.Getenv("PROCESSOR_TEST_ENDPOINT"), "processor-web-test", "processor-web-test")
	if e != nil {
		t.Fatal(e)
	}
	if e = objects.Init(ctx, "processor-web-test"); e != nil {
		t.Fatal(e)
	}
	runtime := &durable.Runtime{Store: store, Objects: objects, Owner: durable.ID()}
	input, e := os.ReadFile("../../testdata/transactions.csv")
	if e != nil {
		t.Fatal(e)
	}
	j, e := runtime.Submit(ctx, durable.ID(), input)
	if e != nil {
		t.Fatal(e)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if e = runtime.Coordinate(ctx); e != nil {
			t.Fatal(e)
		}
		j, e = store.Get(ctx, runtime.Owner, j.ID)
		if e != nil {
			t.Fatal(e)
		}
		if j.State == "processing" {
			break
		}
		if j.State == "failed" || time.Now().After(deadline) {
			t.Fatalf("preparation did not become runnable: %s", j.State)
		}
		time.Sleep(10 * time.Millisecond)
	}
	chunks, e := store.Chunks(ctx, j)
	if e != nil {
		t.Fatal(e)
	}
	for _, c := range chunks {
		if e = runtime.Work(ctx, durable.Dispatch{Job: j.ID, Preparation: j.Preparation, Index: c.Index}); e != nil {
			t.Fatal(e)
		}
	}
	if e = runtime.Coordinate(ctx); e != nil {
		t.Fatal(e)
	}
	server, e := NewDurable("test-only-owner-value", "processor.test", runtime)
	if e != nil {
		t.Fatal(e)
	}
	login := request(server, "POST", "/login", "password=test-only-owner-value")
	cookie := login.Result().Cookies()[0]
	get := func(path string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("GET", "http://processor.test"+path, nil)
		req.AddCookie(cookie)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)
		return w
	}
	path := "/jobs/" + j.ID
	if w := get(path + "/clean.csv"); w.Code != 200 {
		t.Fatalf("valid download failed: HTTP %d", w.Code)
	}
	conn, e := pgx.Connect(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close(ctx)
	if _, e = conn.Exec(ctx, "UPDATE processor_jobs SET doc=jsonb_set(doc,'{expires_at}',to_jsonb(clock_timestamp()-interval '1 second')) WHERE id=$1", j.ID); e != nil {
		t.Fatal(e)
	}
	if w := get(path); w.Code != 200 || !strings.Contains(w.Body.String(), "Downloads expired") {
		t.Fatal("expiry page lost terminal metadata")
	}
	if w := get(path + "/clean.csv"); w.Code != 410 {
		t.Fatal("expiry did not return 410")
	}
	if _, e = conn.Exec(ctx, "UPDATE processor_jobs SET doc=jsonb_set(doc,'{expires_at}',to_jsonb(clock_timestamp()+interval '1 hour')) WHERE id=$1", j.ID); e != nil {
		t.Fatal(e)
	}
	if _, e = conn.Exec(ctx, "UPDATE processor_chunks SET doc=jsonb_set(doc,'{clean,version}','\"missing-test-version\"'::jsonb) WHERE job=$1", j.ID); e != nil {
		t.Fatal(e)
	}
	if w := get(path + "/clean.csv"); w.Code != 500 || !strings.Contains(w.Body.String(), "INTEGRITY_ERROR") {
		t.Fatalf("broken committed reference misclassified: %d", w.Code)
	}
}

func TestConfiguredBackendLabelIsTruthful(t *testing.T) {
	for _, cloud := range []bool{false, true} {
		r := &durable.Runtime{Owner: "fixture-owner", Objects: &durable.Objects{Cloud: cloud}}
		server, e := NewDurable("test-only-value", "processor.test", r)
		if e != nil {
			t.Fatal(e)
		}
		w := request(server, "GET", "/", "")
		if w.Code != 200 {
			t.Fatal("mode page unavailable")
		}
		text := w.Body.String()
		if cloud && (!strings.Contains(text, "AWS-backed workspace") || strings.Contains(text, "no AWS resources are used")) {
			t.Fatal("AWS configuration mislabeled as local emulation")
		}
		if !cloud && (!strings.Contains(text, "Local durable workspace") || !strings.Contains(text, "no AWS resources are used")) {
			t.Fatal("local configuration implies real AWS")
		}
	}
}

func TestClusterBackendLabel(t *testing.T) {
	r := &durable.Runtime{Owner: "fixture-owner", Objects: &durable.Objects{Cluster: true}}
	s, e := NewDurable("test-only-value", "processor.test", r)
	if e != nil {
		t.Fatal(e)
	}
	w := request(s, "GET", "/", "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), "Local Kubernetes workspace") || !strings.Contains(w.Body.String(), "no AWS resources are used") {
		t.Fatal("cluster profile mislabeled")
	}
}
