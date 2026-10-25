package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestVerifyAndRefuseOverwrite(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	t.Chdir(t.TempDir())
	input := filepath.Join(root, "testdata/transactions.csv")
	expected := filepath.Join(root, "testdata/expected")
	if err = run([]string{"verify", "--input", input, "--expected", expected, "--output", "result"}); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat("result/manifest.json"); err != nil {
		t.Fatal("no completion manifest")
	}
	if err = run([]string{"process", "--input", input, "--output", "result"}); err == nil {
		t.Fatal("overwrote output")
	}
}
func TestMismatchDoesNotPublish(t *testing.T) {
	root, _ := filepath.Abs("../..")
	t.Chdir(t.TempDir())
	os.Mkdir("expected", 0700)
	os.WriteFile("expected/clean.csv", []byte("wrong"), 0600)
	os.WriteFile("expected/rejected.csv", nil, 0600)
	err := run([]string{"verify", "--input", filepath.Join(root, "testdata/transactions.csv"), "--expected", "expected", "--output", "result"})
	if err == nil {
		t.Fatal("mismatch passed")
	}
	if _, err = os.Stat("result"); !os.IsNotExist(err) {
		t.Fatal("published mismatch")
	}
}
func TestPublicBindRefused(t *testing.T) {
	if err := run([]string{"serve", "--addr", "0.0.0.0:8080"}); err == nil {
		t.Fatal("public bind accepted")
	}
}

func TestAWSCommandsDoNotFallbackOrProvision(t *testing.T) {
	t.Setenv("PROCESSOR_AWS_REGION", "")
	if e := run([]string{"doctor", "--profile", "aws"}); e == nil || !strings.Contains(e.Error(), "PROCESSOR_AWS_REGION") {
		t.Fatal("cloud doctor silently fell back to local")
	}
	for _, args := range [][]string{{"local", "init", "--profile", "aws"}, {"demo", "recovery", "--profile", "aws"}, {"worker", "--profile", "aws", "--checkpoint", ".scratch/claim"}} {
		if e := run(args); e == nil {
			t.Fatal("local provisioning/debug flow accepted AWS profile")
		}
	}
}
