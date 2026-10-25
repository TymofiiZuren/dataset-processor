package processor

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestKnownResults(t *testing.T) {
	input := "transaction_id,occurred_at,currency,amount\na,2026-10-24T12:30:00+01:00,EUR,12.3\na,2026-10-24T11:30:00Z,USD,-0.00\nb,bad,eur,1.234\n"
	var clean, rejected bytes.Buffer
	got, err := Process(strings.NewReader(input), &clean, &rejected, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	wantClean := "source_ordinal,transaction_id,occurred_at,currency,amount\n1,a,2026-10-24T11:30:00Z,EUR,12.30\n2,a,2026-10-24T11:30:00Z,USD,0.00\n"
	wantRejected := "source_ordinal,transaction_id,occurred_at,currency,amount,reasons\n3,b,bad,eur,1.234,invalid_timestamp;invalid_currency;invalid_amount\n"
	if clean.String() != wantClean {
		t.Fatalf("clean mismatch: %q", clean.String())
	}
	if rejected.String() != wantRejected {
		t.Fatalf("rejections mismatch: %q", rejected.String())
	}
	if got.Accepted != 2 || got.Rejected != 1 || got.Records != 3 || got.InputBytes != int64(len(input)) {
		t.Fatalf("counts: %+v", got)
	}
	if len(got.InputSHA256) != 64 || len(got.CleanSHA256) != 64 {
		t.Fatal("missing hashes")
	}
}

func TestAmount(t *testing.T) {
	for _, tc := range []struct {
		in, want string
		ok       bool
	}{
		{"0", "0.00", true}, {"-0.0", "0.00", true}, {"01.2", "1.20", true},
		{"92233720368547758.07", "92233720368547758.07", true},
		{"-92233720368547758.08", "-92233720368547758.08", true},
		{"92233720368547758.08", "", false}, {"-92233720368547758.09", "", false},
		{"1.234", "", false}, {"+1", "", false}, {"1e2", "", false}, {" 1", "", false}, {".1", "", false}, {"1.", "", false},
	} {
		t.Run(tc.in, func(t *testing.T) {
			got, err := normalizeAmount(tc.in)
			if (err == nil) != tc.ok || got != tc.want {
				t.Fatalf("got %q, %v", got, err)
			}
		})
	}
}

func TestFileFailures(t *testing.T) {
	header := "transaction_id,occurred_at,currency,amount\n"
	for _, tc := range []struct {
		name, input, code string
		limits            Limits
	}{
		{"empty", "", "missing_header", DefaultLimits()},
		{"BOM", "\ufeff" + header, "invalid_header", DefaultLimits()},
		{"wrong header", "id,time,currency,amount\n", "invalid_header", DefaultLimits()},
		{"blank", header + "\n", "invalid_csv", DefaultLimits()},
		{"quotes", header + "\"unterminated", "invalid_csv", DefaultLimits()},
		{"field count", header + "a,b,c\n", "invalid_csv", DefaultLimits()},
		{"UTF8", header + "\xff,b,c,d\n", "invalid_utf8", DefaultLimits()},
		{"record limit", header + strings.Repeat("x", 65), "record_too_large", Limits{MaxFileBytes: 1000, MaxRecordBytes: 64}},
		{"file limit", header, "file_too_large", Limits{MaxFileBytes: 10, MaxRecordBytes: 64}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var a, b bytes.Buffer
			_, err := Process(strings.NewReader(tc.input), &a, &b, tc.limits)
			var failure *Error
			if !errors.As(err, &failure) || failure.Code != tc.code {
				t.Fatalf("got %v, want %s", err, tc.code)
			}
		})
	}
}
func TestQuotedMultilineAndHeaderOnly(t *testing.T) {
	for _, input := range []string{"transaction_id,occurred_at,currency,amount\n", "transaction_id,occurred_at,currency,amount\r\n\"a\nline\",2026-10-24T00:00:00Z,EUR,1\r\n"} {
		var a, b bytes.Buffer
		got, err := Process(strings.NewReader(input), &a, &b, DefaultLimits())
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(input, "line") && (got.Rejected != 1 || !strings.Contains(b.String(), "invalid_transaction_id")) {
			t.Fatal("multiline field must parse, then reject control character")
		}
	}
}
func TestRecordLimitStopsReading(t *testing.T) {
	r := &countReader{remaining: 1 << 20}
	var a, b bytes.Buffer
	_, err := Process(r, &a, &b, Limits{MaxFileBytes: 1 << 20, MaxRecordBytes: 64})
	var failure *Error
	if !errors.As(err, &failure) || failure.Code != "record_too_large" {
		t.Fatal(err)
	}
	if r.read > 4096 {
		t.Fatalf("consumed %d bytes beyond bounded read buffer", r.read)
	}
}

type countReader struct{ remaining, read int }

func (r *countReader) Read(p []byte) (int, error) {
	n := min(len(p), r.remaining)
	for i := 0; i < n; i++ {
		p[i] = 'x'
	}
	r.remaining -= n
	r.read += n
	return n, nil
}

func FuzzProcess(f *testing.F) {
	f.Add("transaction_id,occurred_at,currency,amount\na,2026-10-24T00:00:00Z,EUR,1\n")
	f.Add("\"a\"\"b\",c\n")
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 8192 {
			t.Skip()
		}
		var a, b bytes.Buffer
		_, _ = Process(strings.NewReader(input), &a, &b, Limits{MaxFileBytes: 8192, MaxRecordBytes: 1024})
	})
}
