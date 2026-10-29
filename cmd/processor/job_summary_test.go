package main

import (
	"dataset-processor/internal/durable"
	"encoding/json"
	"strings"
	"testing"
)

func TestRecoverySummaryExcludesFencingTokens(t *testing.T) {
	v := recoverySummary(durable.Job{ID: "job", Token: "private-stage", Preparation: "private-preparation", Input: durable.Ref{Version: "input-version", SHA256: "input-hash"}}, []durable.Chunk{{Index: 0, Attempts: 2, Token: "private-chunk"}})
	b, e := json.Marshal(v)
	if e != nil {
		t.Fatal(e)
	}
	if strings.Contains(string(b), "private-") {
		t.Fatal("summary leaks fencing values")
	}
}

func TestDemonstrationCommandsRefuseAWSBeforeIO(t *testing.T) {
	for _, args := range [][]string{
		{"job", "submit", "--profile", "aws", "--input", "missing", "--key", "demo"},
		{"job", "fetch", "--profile", "aws", "--job", "missing", "--name", "clean.csv"},
		{"coordinator", "--profile", "aws", "--chunk-bytes", "80"},
		{"demo", "recovery", "--profile", "cluster"},
	} {
		if e := run(args); e == nil || !(strings.Contains(e.Error(), "refuse AWS") || strings.Contains(e.Error(), "cluster uses")) {
			t.Fatal("demonstration guard did not reject before backend configuration")
		}
	}
}
