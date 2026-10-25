package durable

import (
	"bytes"
	"context"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"os"
	"testing"
	"time"
)

func testRuntime(t *testing.T) *Runtime {
	t.Helper()
	s := testStore(t)
	endpoint := os.Getenv("PROCESSOR_TEST_ENDPOINT")
	if endpoint == "" {
		t.Fatal("integration requires explicit PROCESSOR_TEST_ENDPOINT")
	}
	o, e := NewObjects(context.Background(), endpoint, "processor-test", "processor-test-"+ID()[:12])
	if e != nil {
		t.Fatal(e)
	}
	if e = o.Init(context.Background(), "processor-test-"+ID()[:12]); e != nil {
		t.Fatal(e)
	}
	return &Runtime{Store: s, Objects: o, Owner: ID(), ChunkBytes: 80, Lease: time.Second}
}
func TestPipelineRecoveryAndExactOutput(t *testing.T) {
	r := testRuntime(t)
	ctx := context.Background()
	input, e := os.ReadFile("../../testdata/transactions.csv")
	if e != nil {
		t.Fatal(e)
	}
	j, e := r.Submit(ctx, ID(), input)
	if e != nil {
		t.Fatal(e)
	}
	if e = r.Coordinate(ctx); e != nil {
		t.Fatal(e)
	}
	j, e = r.Store.Get(ctx, r.Owner, j.ID)
	if e != nil {
		t.Fatal(e)
	}
	j = advanceJob(t, r, j.ID, "processing")
	if j.Parts < 2 {
		t.Fatal("not multi-chunk")
	}
	old, e := r.Store.ClaimChunk(ctx, j.ID, 0, time.Millisecond)
	if e != nil {
		t.Fatal(e)
	}
	time.Sleep(5 * time.Millisecond)
	// A new runtime instance recovers authority from PostgreSQL and versioned objects.
	replacement := &Runtime{Store: r.Store, Objects: r.Objects, Owner: r.Owner, ChunkBytes: 80, Lease: time.Second}
	chunks, e := r.Store.Chunks(ctx, j)
	if e != nil {
		t.Fatal(e)
	}
	for i := len(chunks) - 1; i >= 0; i-- {
		if e = replacement.Work(ctx, Dispatch{Job: j.ID, Preparation: j.Preparation, Index: i}); e != nil {
			t.Fatal(e)
		}
	}
	if e = r.Store.CommitChunk(ctx, old, Ref{}, Ref{}, 1, 0); e != ErrStale {
		t.Fatal("stale success accepted")
	}
	if e = r.Store.FailChunk(ctx, old, "STALE", false); e != ErrStale {
		t.Fatal("stale failure accepted")
	}
	if e = replacement.Work(ctx, Dispatch{Job: j.ID, Preparation: j.Preparation, Index: 0}); e != ErrStale {
		t.Fatal("duplicate accepted")
	}
	if e = replacement.Coordinate(ctx); e != nil {
		t.Fatal(e)
	}
	j, e = r.Store.Get(ctx, r.Owner, j.ID)
	if e != nil || j.State != "succeeded" {
		t.Fatalf("not succeeded: %s %v", j.State, e)
	}
	for _, name := range []string{"clean.csv", "rejected.csv"} {
		var got bytes.Buffer
		if e = r.Download(ctx, j, name, &got); e != nil {
			t.Fatal(e)
		}
		want, e := os.ReadFile("../../testdata/expected/" + name)
		if e != nil {
			t.Fatal(e)
		}
		if !bytes.Equal(got.Bytes(), want) {
			t.Fatal("output differs", name)
		}
	}
	c, e := r.Store.Chunks(ctx, j)
	if e != nil || c[0].Attempts != 2 {
		t.Fatal("no recorded recovery")
	}
	again, e := r.Submit(ctx, ID(), input)
	if e != nil {
		t.Fatal(e)
	}
	if e = r.Store.Cancel(ctx, r.Owner, again.ID); e != nil {
		t.Fatal(e)
	}
	if e = r.Coordinate(ctx); e != nil {
		t.Fatal(e)
	}
	again, e = r.Store.Get(ctx, r.Owner, again.ID)
	if e != nil || again.State != "cancelled" || again.Manifest != nil {
		t.Fatal("cancel published")
	}
}

func TestPreparationAndObjectFailureNeverPublish(t *testing.T) {
	r := testRuntime(t)
	ctx := context.Background()
	for _, input := range []string{"transaction_id,occurred_at,currency,amount\nA,x,EUR,1\n\"broken", "wrong,header\n"} {
		j, e := r.Submit(ctx, ID(), []byte(input))
		if e != nil {
			t.Fatal(e)
		}
		if e = r.Coordinate(ctx); e != nil {
			t.Fatal(e)
		}
		j, e = r.Store.Get(ctx, r.Owner, j.ID)
		j = advanceJob(t, r, j.ID, "failed")
		if e != nil || j.State != "failed" || j.Manifest != nil {
			t.Fatal("malformed published")
		}
		pending, e := r.Store.Pending(ctx, r.Owner)
		if e != nil || len(pending) != 0 {
			t.Fatal("partial preparation dispatched")
		}
	}
	j, e := r.Submit(ctx, ID(), []byte("transaction_id,occurred_at,currency,amount\n"))
	if e != nil {
		t.Fatal(e)
	}
	if e = r.Coordinate(ctx); e != nil {
		t.Fatal(e)
	}
	if e = r.Coordinate(ctx); e != nil {
		t.Fatal(e)
	}
	j, e = r.Store.Get(ctx, r.Owner, j.ID)
	j = advanceJob(t, r, j.ID, "succeeded")
	if e != nil || j.State != "succeeded" || j.Total != 0 {
		t.Fatal("header-only did not succeed")
	}
	var out bytes.Buffer
	if e = r.Download(ctx, j, "clean.csv", &out); e != nil {
		t.Fatal(e)
	}
	// A replacement key cannot change a pinned version.
	ref, e := r.Objects.Put(ctx, "test/"+ID(), []byte("original"))
	if e != nil {
		t.Fatal(e)
	}
	if _, e = r.Objects.Put(ctx, ref.Key, []byte("replacement")); e != nil {
		t.Fatal(e)
	}
	got, e := r.Objects.Read(ctx, ref)
	if e != nil || string(got) != "original" {
		t.Fatal("version pin failed")
	}
	ref.SHA256 = Hash([]byte("wrong"))
	if _, e = r.Objects.Read(ctx, ref); e != ErrIntegrity {
		t.Fatal("corrupt reference accepted")
	}
	ref.Version = "null"
	if _, e = r.Objects.Read(ctx, ref); e != ErrIntegrity {
		t.Fatal("null version accepted")
	}
}

func TestAmbiguousUploadAndOutboxRedelivery(t *testing.T) {
	r := testRuntime(t)
	ctx := context.Background()
	input, e := os.ReadFile("../../testdata/transactions.csv")
	if e != nil {
		t.Fatal(e)
	}
	// Crash between versioned upload and input pin: only the durable intent remains.
	key := ID()
	j, e := r.Store.Admit(ctx, r.Owner, key, descriptor(input), r.Objects.Backend)
	if e != nil {
		t.Fatal(e)
	}
	if e = r.Store.Reserve(ctx, j.ID, int64(len(input))); e != nil {
		t.Fatal(e)
	}
	if _, e = r.Objects.Put(ctx, j.Input.Key, input); e != nil {
		t.Fatal(e)
	}
	if e = r.Coordinate(ctx); e != nil {
		t.Fatal(e)
	}
	if e = r.Coordinate(ctx); e != nil {
		t.Fatal(e)
	}
	j, e = r.Store.Get(ctx, r.Owner, j.ID)
	j = advanceJob(t, r, j.ID, "processing")
	if e != nil || j.State != "processing" {
		t.Fatal("upload not recovered")
	}
	same, e := r.Submit(ctx, key, input)
	if e != nil || same.ID != j.ID {
		t.Fatal("idempotent identity lost")
	}
	chunks, e := r.Store.Chunks(ctx, j)
	if e != nil {
		t.Fatal(e)
	}
	for _, c := range chunks {
		d := Dispatch{j.ID, j.Preparation, c.Index}
		if e = r.Objects.Send(ctx, d); e != nil {
			t.Fatal(e)
		}
		if e = r.Work(ctx, d); e != nil {
			t.Fatal(e)
		}
		if e = r.Work(ctx, d); e != ErrStale {
			t.Fatal("redelivery accepted twice")
		}
	}
	if e = r.Coordinate(ctx); e != nil {
		t.Fatal(e)
	}
	j, e = r.Store.Get(ctx, r.Owner, j.ID)
	if e != nil || j.State != "succeeded" || j.Committed != len(chunks) {
		t.Fatal("recovered output missing")
	}
}

func TestOutputBudgetIsSharedAndStopsBeforeGrowth(t *testing.T) {
	remaining := int64(5)
	var c, r bytes.Buffer
	a, b := &boundedOutput{&c, &remaining}, &boundedOutput{&r, &remaining}
	if _, e := a.Write([]byte("123")); e != nil {
		t.Fatal(e)
	}
	if _, e := b.Write([]byte("456")); e != ErrCapacity {
		t.Fatal("combined output budget not enforced")
	}
	if r.Len() != 0 || remaining != 2 {
		t.Fatal("allocated beyond budget")
	}
}

func TestActualQueueRedeliveryAfterCommitBeforeAck(t *testing.T) {
	r := testRuntime(t)
	ctx := context.Background()
	input, e := os.ReadFile("../../testdata/transactions.csv")
	if e != nil {
		t.Fatal(e)
	}
	j, e := r.Submit(ctx, ID(), input)
	if e != nil {
		t.Fatal(e)
	}
	if e = r.Coordinate(ctx); e != nil {
		t.Fatal(e)
	}
	j, e = r.Store.Get(ctx, r.Owner, j.ID)
	if e != nil {
		t.Fatal(e)
	}
	j = advanceJob(t, r, j.ID, "processing")
	first, e := r.Objects.Receive(ctx)
	if e != nil || first == nil {
		t.Fatal("no actual queue message", e)
	}
	if first.Job != j.ID {
		t.Fatal("unexpected message")
	}
	if e = r.Work(ctx, first.Dispatch); e != nil {
		t.Fatal(e)
	}
	// Emulate loss of the consumer after durable commit, before acknowledgement.
	_, e = r.Objects.SQS.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{QueueUrl: &r.Objects.Queue, ReceiptHandle: &first.Receipt, VisibilityTimeout: 0})
	if e != nil {
		t.Fatal(e)
	}
	seen := false
	for i := 0; i < j.Parts+2; i++ {
		m, e := r.Objects.Receive(ctx)
		if e != nil || m == nil {
			t.Fatal("redelivery missing", e)
		}
		e = r.Work(ctx, m.Dispatch)
		if m.Index == first.Index {
			if e != ErrStale {
				t.Fatal("redelivery counted twice")
			}
			seen = true
		} else if e != nil {
			t.Fatal(e)
		}
		if e = r.Objects.Ack(ctx, *m); e != nil {
			t.Fatal(e)
		}
		if seen {
			break
		}
	}
	if !seen {
		t.Fatal("original message not redelivered")
	}
	chunks, e := r.Store.Chunks(ctx, j)
	if e != nil {
		t.Fatal(e)
	}
	for _, c := range chunks {
		if c.State != "done" {
			if e = r.Work(ctx, Dispatch{j.ID, j.Preparation, c.Index}); e != nil {
				t.Fatal(e)
			}
		}
	}
	if e = r.Coordinate(ctx); e != nil {
		t.Fatal(e)
	}
	j, e = r.Store.Get(ctx, r.Owner, j.ID)
	if e != nil || j.State != "succeeded" || j.Accepted != 3 || j.Rejected != 3 {
		t.Fatal("redelivery reconciliation failed")
	}
}

// Preparation is globally bounded: another integration package may own its slot.
// Wait for authoritative progress instead of assuming one coordinator tick admits it.
func advanceJob(t *testing.T, r *Runtime, id, state string) Job {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		if e := r.Coordinate(ctx); e != nil {
			t.Fatal(e)
		}
		j, e := r.Store.Get(ctx, r.Owner, id)
		if e != nil {
			t.Fatal(e)
		}
		if j.State == state {
			return j
		}
		if terminal(j.State) {
			t.Fatalf("unexpected terminal state %s", j.State)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("job did not reach %s", state)
		case <-time.After(10 * time.Millisecond):
		}
	}
}
