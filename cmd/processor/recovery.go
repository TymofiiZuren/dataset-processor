package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"dataset-processor/internal/durable"
)

func scenarioWorker(ctx context.Context, r *durable.Runtime, id, checkpoint string) error {
	j, e := r.Store.Get(ctx, r.Owner, id)
	if e != nil {
		return errors.New("scenario job unavailable")
	}
	if checkpoint != "" {
		absolute, e := filepath.Abs(checkpoint)
		if e != nil {
			return e
		}
		scratch, _ := filepath.Abs(".scratch")
		if !strings.HasPrefix(absolute, scratch+string(os.PathSeparator)) {
			return errors.New("checkpoint must be under .scratch")
		}
		c, e := r.Store.ClaimChunk(ctx, id, 0, r.Lease)
		if e != nil {
			return e
		}
		b, _ := json.Marshal(c)
		if e = os.WriteFile(absolute, b, 0600); e != nil {
			return errors.New("cannot write recovery checkpoint")
		}
		<-ctx.Done()
		return nil // the abandoned lease remains durable for takeover
	}
	chunks, e := r.Store.Chunks(ctx, j)
	if e != nil {
		return e
	}
	for i := len(chunks) - 1; i >= 0; i-- {
		if e = r.Work(ctx, durable.Dispatch{Job: id, Preparation: j.Preparation, Index: chunks[i].Index}); e != nil {
			return e
		}
	}
	return nil
}
func recoveryDemo(ctx context.Context, r *durable.Runtime, c durable.Config) error {
	// A separate owner/queue isolates the experiment from normal workspace workers.
	dir, e := os.MkdirTemp(".scratch", "recovery-")
	if e != nil {
		return e
	}
	dir, _ = filepath.Abs(dir)
	ownerPath := filepath.Join(dir, "owner")
	owner := durable.ID()
	if e = os.WriteFile(ownerPath, []byte(owner), 0600); e != nil {
		return e
	}
	queue := "processor-recovery-" + durable.ID()[:12]
	objects, e := durable.NewObjects(ctx, c.Endpoint, c.Bucket, queue)
	if e != nil {
		return e
	}
	if e = objects.Init(ctx, queue); e != nil {
		return e
	}
	runtime := &durable.Runtime{Store: r.Store, Objects: objects, Owner: owner, ChunkBytes: 80, Lease: 2 * time.Second}
	input, e := os.ReadFile("testdata/transactions.csv")
	if e != nil {
		return errors.New("run recovery demo from project root with independent fixtures available")
	}
	j, e := runtime.Submit(ctx, durable.ID(), input)
	if e != nil {
		return e
	}
	prepareDeadline := time.Now().Add(15 * time.Second)
	for {
		if e = runtime.Coordinate(ctx); e != nil {
			return e
		}
		j, e = runtime.Store.Get(ctx, owner, j.ID)
		if e != nil {
			return e
		}
		if j.State == "processing" {
			break
		}
		if j.State == "failed" || time.Now().After(prepareDeadline) {
			return errors.New("scenario could not obtain preparation capacity")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
	if j.Parts < 2 {
		return errors.New("recovery experiment did not prepare multiple chunks")
	}
	executable, e := os.Executable()
	if e != nil {
		return e
	}
	checkpoint := filepath.Join(dir, "claimed.json")
	child := func(pause bool) *exec.Cmd {
		args := []string{"worker", "--job", j.ID, "--lease", "2s"}
		if pause {
			args = append(args, "--checkpoint", checkpoint)
		}
		cmd := exec.CommandContext(ctx, executable, args...)
		cmd.Env = append(os.Environ(), "PROCESSOR_OWNER_FILE="+ownerPath, "PROCESSOR_QUEUE="+queue)
		return cmd
	}
	crashed := child(true)
	var safeOutput bytes.Buffer
	crashed.Stdout = &safeOutput
	crashed.Stderr = &safeOutput
	if e = crashed.Start(); e != nil {
		return errors.New("cannot start scenario worker")
	}
	stopped := false
	defer func() {
		if !stopped {
			_ = crashed.Process.Signal(os.Interrupt)
			_ = crashed.Wait()
		}
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, e = os.Stat(checkpoint); e == nil {
			break
		}
		if time.Now().After(deadline) {
			return errors.New("post-claim recovery checkpoint was not reached")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
	var old durable.Chunk
	b, e := os.ReadFile(checkpoint)
	if e != nil {
		return e
	}
	if e = json.Unmarshal(b, &old); e != nil {
		return e
	}
	if e = crashed.Process.Signal(os.Interrupt); e != nil {
		return e
	}
	if e = crashed.Wait(); e != nil {
		return errors.New("scenario worker did not stop cleanly at checkpoint")
	}
	stopped = true
	wait := time.Until(old.Until) + 50*time.Millisecond
	if wait > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
	recovered := child(false)
	recovered.Stdout = &safeOutput
	recovered.Stderr = &safeOutput
	if e = recovered.Run(); e != nil {
		return errors.New("replacement worker failed; experiment incomplete")
	}
	if e = runtime.Store.CommitChunk(ctx, old, durable.Ref{}, durable.Ref{}, old.Records, 0); !errors.Is(e, durable.ErrStale) {
		return errors.New("stale success was not rejected")
	}
	if e = runtime.Store.FailChunk(ctx, old, "STALE_EXPERIMENT", false); !errors.Is(e, durable.ErrStale) {
		return errors.New("stale failure was not rejected")
	}
	if e = runtime.Coordinate(ctx); e != nil {
		return e
	}
	j, e = runtime.Store.Get(ctx, owner, j.ID)
	if e != nil || j.State != "succeeded" {
		return errors.New("recovered result was not published")
	}
	for _, name := range []string{"clean.csv", "rejected.csv"} {
		var output bytes.Buffer
		if e = runtime.Download(ctx, j, name, &output); e != nil {
			return e
		}
		expected, e := os.ReadFile(filepath.Join("testdata", "expected", name))
		if e != nil || !bytes.Equal(expected, output.Bytes()) {
			return errors.New("recovery output differs from independent expected bytes")
		}
		if e = os.WriteFile(filepath.Join(dir, name), output.Bytes(), 0600); e != nil {
			return e
		}
	}
	chunks, e := runtime.Store.Chunks(ctx, j)
	if e != nil || chunks[0].Attempts != 2 {
		return errors.New("replacement attempt evidence missing")
	}
	source, e := sourceIdentity()
	if e != nil {
		return e
	}
	report := map[string]any{"result": "PASS", "job_id": j.ID, "input_version": j.Input.Version, "input_sha256": j.Input.SHA256, "parts": j.Parts, "replacement_attempts": chunks[0].Attempts, "stale_success_rejected": true, "stale_failure_rejected": true, "independent_outputs_equal": true, "source_sha256": source, "checkpoint": "durable claim before chunk commit", "interruption": "scenario-owned worker SIGINT; lease abandoned", "completion_order": "descending chunk index", "commands": []string{"worker --job ID --lease 2s --checkpoint .scratch/RUN/claimed.json", "worker --job ID --lease 2s"}, "manifest": j.Manifest, "completed_at": time.Now().UTC()}
	reportBytes, _ := json.MarshalIndent(report, "", "  ")
	if e = os.WriteFile(filepath.Join(dir, "evidence.json"), reportBytes, 0600); e != nil {
		return e
	}
	fmt.Println("Recovery: PASS · real worker restart, multi-chunk takeover, stale success/failure rejected, independent outputs equal")
	fmt.Println("Evidence:", filepath.Join(dir, "evidence.json"))
	return nil
}
func sourceIdentity() (string, error) {
	var names []string
	e := filepath.WalkDir(".", func(path string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() && d.Name() == ".scratch" {
			return filepath.SkipDir
		}
		if !d.IsDir() && (strings.HasSuffix(path, ".go") || strings.HasSuffix(path, ".html") || path == "go.mod" || path == "go.sum" || path == "compose.local.yaml") {
			names = append(names, path)
		}
		return nil
	})
	if e != nil {
		return "", e
	}
	sort.Strings(names)
	var b bytes.Buffer
	for _, name := range names {
		data, e := os.ReadFile(name)
		if e != nil {
			return "", e
		}
		b.WriteString(name)
		b.WriteByte(0)
		b.Write(data)
		b.WriteByte(0)
	}
	return durable.Hash(b.Bytes()), nil
}
