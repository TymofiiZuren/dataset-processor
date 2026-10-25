package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestClusterInitCommand(t *testing.T) {
	if e := os.MkdirAll("../../.scratch", 0700); e != nil {
		t.Fatal(e)
	}
	d, e := os.MkdirTemp("../../.scratch", "cluster-cli-")
	if e != nil {
		t.Fatal(e)
	}
	target := filepath.Join(d, "private")
	if e = run([]string{"cluster", "init", "--output", target}); e != nil {
		t.Fatal(e)
	}
	if e = run([]string{"cluster", "init", "--output", target}); e != nil {
		t.Fatal(e)
	}
	if e = run([]string{"cluster", "init"}); e == nil {
		t.Fatal("missing explicit destination accepted")
	}
	if e = run([]string{"cluster", "unknown"}); e == nil {
		t.Fatal("unknown command accepted")
	}
}
