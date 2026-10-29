package processor

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

func TestPartsPreserveReferenceBytes(t *testing.T) {
	for _, input := range []string{
		"transaction_id,occurred_at,currency,amount\r\nA,2026-10-24T09:35:00Z,GBP,-24.50\r\n\"multi\nline\",bad,CAD,1.234\r\nA,2026-10-24T09:35:00Z,USD,0",
		"transaction_id,occurred_at,currency,amount\n",
	} {
		var wantC, wantR, gotC, gotR bytes.Buffer
		want, e := Process(strings.NewReader(input), &wantC, &wantR, DefaultLimits())
		if e != nil {
			t.Fatal(e)
		}
		c, r := OutputHeaders()
		gotC.Write(c)
		gotR.Write(r)
		var records, accepted, rejected int64
		m, e := Prepare(strings.NewReader(input), DefaultLimits(), 30, func(p Part) error {
			var c, r bytes.Buffer
			pm, e := ProcessPart(bytes.NewReader(p.Data), &c, &r, DefaultLimits(), p.FirstOrdinal)
			if e != nil {
				return e
			}
			if pm.Records != p.Records {
				t.Fatal("part count")
			}
			gotC.Write(c.Bytes())
			gotR.Write(r.Bytes())
			records += pm.Records
			accepted += pm.Accepted
			rejected += pm.Rejected
			return nil
		})
		if e != nil {
			t.Fatal(e)
		}
		if m.InputSHA256 != want.InputSHA256 || m.InputBytes != want.InputBytes || records != want.Records || accepted != want.Accepted || rejected != want.Rejected || !bytes.Equal(gotC.Bytes(), wantC.Bytes()) || !bytes.Equal(gotR.Bytes(), wantR.Bytes()) {
			t.Fatal("parts differ from reference")
		}
	}
}
func TestPartsMatchIndependentFixture(t *testing.T) {
	input, e := os.ReadFile("../../testdata/transactions.csv")
	if e != nil {
		t.Fatal(e)
	}
	var c, r bytes.Buffer
	ch, rh := OutputHeaders()
	c.Write(ch)
	r.Write(rh)
	_, e = Prepare(bytes.NewReader(input), DefaultLimits(), 80, func(p Part) error {
		_, e := ProcessPart(bytes.NewReader(p.Data), &c, &r, DefaultLimits(), p.FirstOrdinal)
		return e
	})
	if e != nil {
		t.Fatal(e)
	}
	for name, data := range map[string][]byte{"clean.csv": c.Bytes(), "rejected.csv": r.Bytes()} {
		want, e := os.ReadFile("../../testdata/expected/" + name)
		if e != nil {
			t.Fatal(e)
		}
		if !bytes.Equal(data, want) {
			t.Fatal(name)
		}
	}
}
func TestPrepareRejectsMalformedAndPreservesRecordLimit(t *testing.T) {
	_, e := Prepare(strings.NewReader("transaction_id,occurred_at,currency,amount\nA,x,EUR,1\n\"broken"), DefaultLimits(), 1, func(Part) error { return nil })
	if e == nil {
		t.Fatal("malformed preparation succeeded")
	}
	_, e = ProcessPart(strings.NewReader("A,x,EUR,1\r\n"), &bytes.Buffer{}, &bytes.Buffer{}, Limits{100, 10}, 7)
	if e == nil {
		t.Fatal("encoded CRLF record limit lost")
	}
}

type rejectingWriter struct{}

func (rejectingWriter) Write([]byte) (int, error) { return 0, os.ErrPermission }
func TestPartWriterFailurePreventsSuccess(t *testing.T) {
	_, e := ProcessPart(strings.NewReader("A,2026-10-24T09:35:00Z,EUR,1\n"), rejectingWriter{}, &bytes.Buffer{}, DefaultLimits(), 9)
	if e == nil {
		t.Fatal("writer failure reported success")
	}
}
