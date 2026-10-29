package web

import (
	"context"
	"dataset-processor/internal/durable"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestJobActivityOwnerPageAndPolling(t *testing.T) {
	dsn := os.Getenv("PROCESSOR_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("explicit integration database required")
	}
	store, e := durable.Open(context.Background(), dsn)
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	if e = store.Migrate(context.Background()); e != nil {
		t.Fatal(e)
	}
	owner := durable.ID()
	j, e := store.Admit(context.Background(), owner, durable.ID(), "fixture", "local:fixture")
	if e != nil {
		t.Fatal(e)
	}
	if e = store.Cancel(context.Background(), owner, j.ID); e != nil {
		t.Fatal(e)
	}
	s, e := NewDurable("test-only-value", "processor.test", &durable.Runtime{Store: store, Owner: owner})
	if e != nil {
		t.Fatal(e)
	}
	s.session = "fixture-session"
	s.expires = time.Now().Add(time.Hour)
	get := func(path string, auth bool) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", "http://processor.test"+path, nil)
		if auth {
			r.AddCookie(&http.Cookie{Name: "processor_session", Value: s.session})
		}
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		return w
	}
	path := "/jobs/" + j.ID
	if w := get(path, true); w.Code != 200 || !strings.Contains(w.Body.String(), "Job activity") || !strings.Contains(w.Body.String(), "Cancellation confirmed") {
		t.Fatal("persisted activity not rendered")
	}
	w := get(path+"/status", true)
	var body struct {
		Revision int64        `json:"revision"`
		Activity activityView `json:"activity"`
	}
	if e = json.Unmarshal(w.Body.Bytes(), &body); e != nil || w.Code != 200 || len(body.Activity.Events) != 1 || body.Activity.Events[0].Action != "Cancellation confirmed" || body.Activity.Events[0].Revision > body.Revision {
		t.Fatal("poll activity contract failed")
	}
	if w = get(path+"/status", false); w.Code != 401 {
		t.Fatal("unauthenticated poll exposes activity")
	}
	s.runtime.Owner = durable.ID()
	if w = get(path, true); w.Code != 404 {
		t.Fatal("foreign activity page exposed")
	}
	if w = get(path+"/status", true); w.Code != 404 {
		t.Fatal("foreign poll exposed")
	}
}
func TestActivitySummaryDoesNotEchoUnknownInternalCodes(t *testing.T) {
	a := activitySummary(durable.Activity{Events: []durable.Event{{Code: "private-internal-value", State: "processing"}}})
	b, _ := json.Marshal(a)
	if strings.Contains(string(b), "private-internal-value") {
		t.Fatal("internal diagnostic echoed")
	}
}
