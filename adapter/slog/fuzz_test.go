package slog

import (
	"bytes"
	"context"
	"encoding/json"
	stdslog "log/slog"
	"math"
	"strings"
	"testing"

	"github.com/happytoolin/unolog"
)

// FuzzSinkWritesValidJSON: for any keys and values, the bridge must not
// panic, and slog's JSON handler must write one valid line in which no
// member appears twice and the last write of each key wins.
func FuzzSinkWritesValidJSON(f *testing.F) {
	f.Add("s", "v", int64(7), 1.5)
	f.Add("\x00\"", " \xff\\", int64(math.MinInt64), math.Inf(1))
	f.Add("", "é😀", int64(-1), math.NaN())
	f.Add("msg", "user msg", int64(1), 0.5)
	f.Add("level", "user level", int64(2), 2.5)
	f.Add("fields.msg", "aliased", int64(3), 3.5)
	f.Add("op.outcome", "user outcome", int64(4), 4.5)
	f.Fuzz(func(t *testing.T, key, value string, i int64, fl float64) {
		var buf bytes.Buffer
		// No value limit: the check compares the rendered value with the
		// written one, and the limits have their own fuzz target.
		rt := unolog.MustCompile(unolog.Config{Sink: New(stdslog.New(stdslog.NewJSONHandler(&buf, nil))), SamplingRate: 1, MaxValueBytes: -1})
		op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainJob, Name: "fuzz"})
		ctx := op.Context()
		unolog.Add(ctx, key, "first", "i", i, "f", fl)
		unolog.AddFields(ctx, unolog.String(key, value))
		op.End(nil)

		line := buf.Bytes()
		if bytes.Count(line, []byte("\n")) != 1 || !json.Valid(line) {
			t.Fatalf("not one valid JSON line: %q", line)
		}
		seen := map[string]bool{}
		for _, k := range memberKeys(t, line) {
			if seen[k] {
				t.Fatalf("member %q appears twice: %s", k, line)
			}
			seen[k] = true
		}
		// unolog drops empty keys, and End writes these keys itself.
		if key == "" || key == unolog.KeyOpOutcome || key == unolog.KeyDurationMS {
			return
		}
		var payload map[string]any
		if err := json.Unmarshal(line, &payload); err != nil {
			t.Fatal(err)
		}
		if got, want := payload[memberName(sanitize(key))], sanitize(value); got != want {
			t.Fatalf("last write lost: got %q, want %q", got, want)
		}
	})
}

// memberName is the member name of a user key: slog's own time/level/msg/source
// are renamed to "fields.<key>".
func memberName(key string) string {
	switch key {
	case "time", "level", "msg", "source":
		return "fields." + key
	}
	return key
}

// memberKeys returns the top-level member names of a JSON object line,
// in order and with duplicates.
func memberKeys(t *testing.T, line []byte) []string {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(line))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		t.Fatalf("line is not an object: %s", line)
	}
	var keys []string
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			t.Fatal(err)
		}
		key, _ := tok.(string)
		keys = append(keys, key)
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			t.Fatal(err)
		}
	}
	return keys
}

// sanitize is the JSON round trip of s: each invalid UTF-8 byte becomes
// U+FFFD, as the encoders write it.
func sanitize(s string) string {
	var b strings.Builder
	for _, r := range s {
		b.WriteRune(r)
	}
	return b.String()
}
