package web

import (
	"context"
	"dataset-processor/internal/durable"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestOwnerHomeHistoryAndUnavailableState(t *testing.T) {
	dsn := os.Getenv("PROCESSOR_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("explicit integration database required")
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
	owner := durable.ID()
	other := durable.ID()
	add := func(who string) durable.Job {
		t.Helper()
		j, e := store.Admit(ctx, who, durable.ID(), "fixture", "local:fixture")
		if e != nil {
			t.Fatal(e)
		}
		if e = store.Cancel(ctx, who, j.ID); e != nil {
			t.Fatal(e)
		}
		return j
	}
	mine := add(owner)
	foreign := add(other)
	s, e := NewDurable("test-only-value", "processor.test", &durable.Runtime{Store: store, Owner: owner})
	if e != nil {
		t.Fatal(e)
	}
	s.session = "fixture-session"
	s.expires = time.Now().Add(time.Hour)
	get := func(auth bool) *httptest.ResponseRecorder {
		req := httptest.NewRequest("GET", "http://processor.test/", nil)
		if auth {
			req.AddCookie(&http.Cookie{Name: "processor_session", Value: s.session})
		}
		w := httptest.NewRecorder()
		s.ServeHTTP(w, req)
		return w
	}
	if w := get(false); w.Code != 200 || strings.Contains(w.Body.String(), mine.ID) {
		t.Fatal("history exposed before sign-in")
	}
	w := get(true)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "Recent jobs") || !strings.Contains(w.Body.String(), "/jobs/"+mine.ID) || !strings.Contains(w.Body.String(), "Cancelled · no result published") || strings.Contains(w.Body.String(), foreign.ID) {
		t.Fatal("owned recent job missing or foreign job exposed")
	}
	s.runtime.Owner = durable.ID()
	w = get(true)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "No jobs yet") {
		t.Fatal("missing empty state")
	}
	store.Close()
	w = get(true)
	if w.Code != 503 || w.Header().Get("Retry-After") == "" || !strings.Contains(w.Body.String(), "Job history is temporarily unavailable") || strings.Contains(w.Body.String(), "No jobs yet") {
		t.Fatal("history failure hidden as empty list")
	}
}
