// Package processor implements the deterministic, bounded reference processor.
// It makes no distributed-processing or AWS correctness claims.
package processor

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const Version = "transactions-v1"

var header = []string{"transaction_id", "occurred_at", "currency", "amount"}
var timestampPattern = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?(Z|[+-]\d{2}:\d{2})$`)
var amountPattern = regexp.MustCompile(`^-?[0-9]+(\.[0-9]{1,2})?$`)

type Limits struct {
	MaxFileBytes   int64
	MaxRecordBytes int
}

func DefaultLimits() Limits { return Limits{1 << 30, 64 << 10} }

type Error struct {
	Code   string `json:"code"`
	Record int64  `json:"record"`
}

func (e *Error) Error() string { return fmt.Sprintf("%s at source record %d", e.Code, e.Record) }

type Manifest struct {
	Version        string `json:"transformation_version"`
	Records        int64  `json:"records"`
	Accepted       int64  `json:"accepted"`
	Rejected       int64  `json:"rejected"`
	InputBytes     int64  `json:"input_bytes"`
	InputSHA256    string `json:"input_sha256"`
	CleanSHA256    string `json:"clean_sha256"`
	RejectedSHA256 string `json:"rejected_sha256"`
}

// Process streams one bounded record at a time. Outputs may be partial on error;
// callers must publish them only after a successful return and verified counts.
func Process(input io.Reader, clean, rejected io.Writer, limits Limits) (Manifest, error) {
	return process(input, clean, rejected, limits, true, 1)
}

// ProcessPart consumes original encoded data records, without an input header.
// Its outputs have no headers and carry global source ordinals.
func ProcessPart(input io.Reader, clean, rejected io.Writer, limits Limits, firstOrdinal int64) (Manifest, error) {
	if firstOrdinal < 1 {
		return Manifest{}, errors.New("first ordinal must be positive")
	}
	return process(input, clean, rejected, limits, false, firstOrdinal)
}

func OutputHeaders() ([]byte, []byte) {
	var c, r bytes.Buffer
	cw, rw := csv.NewWriter(&c), csv.NewWriter(&r)
	_ = cw.Write(append([]string{"source_ordinal"}, header...))
	_ = rw.Write(append(append([]string{"source_ordinal"}, header...), "reasons"))
	cw.Flush()
	rw.Flush()
	return c.Bytes(), r.Bytes()
}

// scan owns framing, encoding, field count and input identity for both paths.
func scan(input io.Reader, limits Limits, hasHeader bool, firstOrdinal int64, visit func([]byte, []string, int64) error) (Manifest, error) {
	m := Manifest{Version: Version}
	if limits.MaxFileBytes <= 0 || limits.MaxRecordBytes <= 0 {
		return m, errors.New("limits must be positive")
	}
	ih := sha256.New()
	counter := &countingReader{r: io.TeeReader(input, ih)}
	reader := bufio.NewReaderSize(&io.LimitedReader{R: counter, N: limits.MaxFileBytes + 1}, 4096)
	needHeader := hasHeader
	for {
		raw, err := readRecord(reader, limits.MaxRecordBytes)
		if counter.n > limits.MaxFileBytes {
			return m, &Error{"file_too_large", m.Records}
		}
		if err == io.EOF {
			break
		}
		ordinal := firstOrdinal + m.Records
		if err != nil {
			var failure *Error
			if errors.As(err, &failure) {
				failure.Record = ordinal
			}
			return m, err
		}
		if !utf8.Valid(raw) {
			return m, &Error{"invalid_utf8", ordinal}
		}
		cr := csv.NewReader(bytes.NewReader(raw))
		cr.FieldsPerRecord = 4
		row, err := cr.Read()
		if err != nil || len(row) != 4 {
			return m, &Error{"invalid_csv", ordinal}
		}
		if needHeader {
			if strings.Join(row, "\x00") != strings.Join(header, "\x00") {
				return m, &Error{"invalid_header", 0}
			}
			needHeader = false
			continue
		}
		if err = visit(raw, row, ordinal); err != nil {
			return m, err
		}
		m.Records++
	}
	if needHeader {
		return m, &Error{"missing_header", 0}
	}
	m.InputBytes = counter.n
	m.InputSHA256 = digest(ih)
	return m, nil
}
func process(input io.Reader, clean, rejected io.Writer, limits Limits, hasHeader bool, firstOrdinal int64) (Manifest, error) {
	ch, rh := sha256.New(), sha256.New()
	cw, rw := csv.NewWriter(io.MultiWriter(clean, ch)), csv.NewWriter(io.MultiWriter(rejected, rh))
	if hasHeader {
		_ = cw.Write(append([]string{"source_ordinal"}, header...))
		_ = rw.Write(append(append([]string{"source_ordinal"}, header...), "reasons"))
	}
	var accepted, rejectedCount int64
	m, err := scan(input, limits, hasHeader, firstOrdinal, func(_ []byte, row []string, ordinal int64) error {
		normalized, reasons := transform(row)
		if len(reasons) == 0 {
			accepted++
			return cw.Write(append([]string{strconv.FormatInt(ordinal, 10)}, normalized...))
		}
		rejectedCount++
		record := append([]string{strconv.FormatInt(ordinal, 10)}, row...)
		return rw.Write(append(record, strings.Join(reasons, ";")))
	})
	if err != nil {
		return m, err
	}
	cw.Flush()
	rw.Flush()
	if err = errors.Join(cw.Error(), rw.Error()); err != nil {
		return m, err
	}
	m.Accepted = accepted
	m.Rejected = rejectedCount
	m.CleanSHA256 = digest(ch)
	m.RejectedSHA256 = digest(rh)
	return m, nil
}

// Part retains original record bytes; callers must keep all staged parts private
// until Prepare has validated the entire input and selected its generation.
type Part struct {
	Data                  []byte
	FirstOrdinal, Records int64
}

func Prepare(input io.Reader, limits Limits, targetBytes int, emit func(Part) error) (Manifest, error) {
	if targetBytes <= 0 {
		return Manifest{}, errors.New("chunk target must be positive")
	}
	var data []byte
	var first, count int64
	flush := func() error {
		if count == 0 {
			return nil
		}
		if err := emit(Part{data, first, count}); err != nil {
			return err
		}
		data = nil
		count = 0
		return nil
	}
	m, err := scan(input, limits, true, 1, func(raw []byte, _ []string, ordinal int64) error {
		if count > 0 && len(data)+len(raw) > targetBytes {
			if err := flush(); err != nil {
				return err
			}
		}
		if count == 0 {
			first = ordinal
		}
		data = append(data, raw...)
		count++
		return nil
	})
	if err != nil {
		return m, err
	}
	if err = flush(); err != nil {
		return m, err
	}
	return m, nil
}
func digest(h hash.Hash) string { return hex.EncodeToString(h.Sum(nil)) }

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, e := c.r.Read(p)
	c.n += int64(n)
	return n, e
}

// Bounded lexical framing precedes encoding/csv so oversized quoted fields
// cannot make that parser allocate an arbitrarily large record buffer.
func readRecord(r *bufio.Reader, maxBytes int) ([]byte, error) {
	const (
		start      = 0
		unquoted   = 1
		quoted     = 2
		afterQuote = 3
	)
	state := start
	record := make([]byte, 0, min(maxBytes, 4096))
	for {
		b, err := r.ReadByte()
		if err == io.EOF {
			if len(record) == 0 {
				return nil, io.EOF
			}
			if state == quoted {
				return nil, &Error{Code: "invalid_csv"}
			}
			return record, nil
		}
		if err != nil {
			return nil, err
		}
		if len(record) >= maxBytes {
			return nil, &Error{Code: "record_too_large"}
		}
		record = append(record, b)
		if state == quoted {
			if b == '"' {
				state = afterQuote
			}
			continue
		}
		if b == '\n' {
			if len(record) == 1 {
				return nil, &Error{Code: "invalid_csv"}
			}
			return record, nil
		}
		if b == '\r' {
			next, e := r.ReadByte()
			if e != nil || next != '\n' {
				return nil, &Error{Code: "invalid_csv"}
			}
			if len(record) >= maxBytes {
				return nil, &Error{Code: "record_too_large"}
			}
			record = append(record, next)
			if len(record) == 2 {
				return nil, &Error{Code: "invalid_csv"}
			}
			return record, nil
		}
		switch state {
		case start:
			if b == '"' {
				state = quoted
			} else if b != ',' {
				state = unquoted
			}
		case unquoted:
			if b == '"' {
				return nil, &Error{Code: "invalid_csv"}
			}
			if b == ',' {
				state = start
			}
		case afterQuote:
			if b == '"' {
				state = quoted
			} else if b == ',' {
				state = start
			} else {
				return nil, &Error{Code: "invalid_csv"}
			}
		}
	}
}
func transform(row []string) ([]string, []string) {
	out := append([]string(nil), row...)
	var reasons []string
	idOK := row[0] != "" && utf8.RuneCountInString(row[0]) <= 128
	for _, r := range row[0] {
		if unicode.IsControl(r) {
			idOK = false
		}
	}
	if !idOK {
		reasons = append(reasons, "invalid_transaction_id")
	}
	instant, err := time.Parse(time.RFC3339Nano, row[1])
	if err != nil || !timestampPattern.MatchString(row[1]) || !validOffset(row[1]) || instant.UTC().Year() < 0 || instant.UTC().Year() > 9999 {
		reasons = append(reasons, "invalid_timestamp")
	} else {
		out[1] = instant.UTC().Format(time.RFC3339Nano)
	}
	if row[2] != "EUR" && row[2] != "GBP" && row[2] != "USD" {
		reasons = append(reasons, "invalid_currency")
	}
	amount, err := normalizeAmount(row[3])
	if err != nil {
		reasons = append(reasons, "invalid_amount")
	} else {
		out[3] = amount
	}
	return out, reasons
}
func validOffset(s string) bool {
	if strings.HasSuffix(s, "Z") {
		return true
	}
	if len(s) < 6 {
		return false
	}
	tail := s[len(s)-6:]
	h, e1 := strconv.Atoi(tail[1:3])
	m, e2 := strconv.Atoi(tail[4:6])
	return e1 == nil && e2 == nil && h < 24 && m < 60
}
func normalizeAmount(s string) (string, error) {
	if !amountPattern.MatchString(s) {
		return "", errors.New("invalid amount")
	}
	negative := strings.HasPrefix(s, "-")
	body := strings.TrimPrefix(s, "-")
	whole, fraction, _ := strings.Cut(body, ".")
	fraction = (fraction + "00")[:2]
	digits := strings.TrimLeft(whole+fraction, "0")
	if digits == "" {
		digits = "0"
	}
	magnitude, err := strconv.ParseUint(digits, 10, 64)
	maxValue := uint64(1<<63 - 1)
	if negative {
		maxValue++
	}
	if err != nil || magnitude > maxValue {
		return "", errors.New("amount overflow")
	}
	prefix := ""
	if negative && magnitude != 0 {
		prefix = "-"
	}
	return fmt.Sprintf("%s%d.%02d", prefix, magnitude/100, magnitude%100), nil
}
