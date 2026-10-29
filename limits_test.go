package unolog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestLimitsDefaults(t *testing.T) {
	rt := MustCompile(Config{})
	if rt.limits != defaultLimits {
		t.Fatalf("zero config limits = %+v, want %+v", rt.limits, defaultLimits)
	}
	rt = MustCompile(Config{MaxFields: -1, MaxValueBytes: -5})
	if rt.limits.maxFields <= 1<<40 || rt.limits.maxValueBytes <= 1<<40 {
		t.Fatalf("negative limits must mean unlimited: %+v", rt.limits)
	}
	rt = MustCompile(Config{MaxFields: 3, MaxValueBytes: 7})
	if rt.limits != (limits{maxFields: 3, maxValueBytes: 7}) {
		t.Fatalf("explicit limits = %+v", rt.limits)
	}
	var nilRT *Runtime
	if nilRT.eventLimits() != defaultLimits || (&Runtime{}).eventLimits() != defaultLimits {
		t.Fatal("nil and uncompiled runtimes must use the default limits")
	}
}

func TestMaxFieldsDropsNewKeysAndCounts(t *testing.T) {
	rt, ts := testRT(t, func(c *Config) { c.MaxFields = 4 })
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j"})
	for i := range 10 {
		Add(op.Context(), fmt.Sprintf("k%d", i), i)
	}
	if !op.End(nil) {
		t.Fatal("event dropped")
	}
	ev := ts.Events()[0]
	for i := range 10 {
		_, ok := ev.Lookup(fmt.Sprintf("k%d", i))
		if want := i < 4; ok != want {
			t.Fatalf("k%d present = %v, want %v", i, ok, want)
		}
	}
	if v, _ := ev.Lookup(KeyDroppedFields); v != int64(6) {
		t.Fatalf("%s = %v, want 6", KeyDroppedFields, v)
	}
	if _, ok := ev.Lookup(KeyTruncatedValues); ok {
		t.Fatal("truncated counter written with no truncation")
	}
}

// A progress counter updated in a loop must not grow the event past the
// limit, and its final value must survive.
func TestMaxFieldsReplacesExistingKeyInPlace(t *testing.T) {
	rt, ts := testRT(t, func(c *Config) { c.MaxFields = 3 })
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j"})
	Add(op.Context(), "first", 1)
	for i := range 10000 {
		Add(op.Context(), "processed", i)
	}
	if n := len(op.ev.fields); n != 3 {
		t.Fatalf("WAL holds %d fields, want 3", n)
	}
	op.End(nil)
	ev := ts.Events()[0]
	if v, _ := ev.Lookup("processed"); v != int64(9999) {
		t.Fatalf("processed = %v, want 9999", v)
	}
	if _, ok := ev.Lookup(KeyDroppedFields); ok {
		t.Fatal("in-place replacement counted as a drop")
	}
	var line map[string]any
	if err := json.Unmarshal(recOf(ev.Level(), ev.Message(), ev.Fields()...).Encoded(), &line); err != nil {
		t.Fatal(err)
	}
	if line["processed"] != float64(9999) {
		t.Fatalf("wire processed = %v", line["processed"])
	}
}

// Canonical fields written after the limit is reached must still land:
// the integrations write http.status, the route, and the error last.
func TestMaxFieldsNeverDropsCanonicalKeys(t *testing.T) {
	rt, ts := testRT(t, func(c *Config) { c.MaxFields = 2 })
	op := Start(context.Background(), rt, OperationStart{Domain: DomainHTTP, Name: "request"})
	ctx := op.Context()
	Add(ctx, "a", 1, "b", 2, "c", 3)
	Add(ctx, KeyHTTPRoute, "/orders/{id}", KeyHTTPStatus, 503)
	for range 100 {
		Add(ctx, KeyHTTPStatus, 503) // repeated canonical writes stay bounded
	}
	err := errors.New("db down")
	op.End(&err)

	ev := ts.Events()[0]
	if n := len(ev.Fields()); n > 2+len(canonicalKeysForTest())+2 {
		t.Fatalf("record holds %d fields: canonical keys are not bounded", n)
	}
	for key, want := range map[string]any{KeyHTTPRoute: "/orders/{id}", KeyHTTPStatus: int64(503), KeyOpOutcome: "failure"} {
		if v, _ := ev.Lookup(key); v != want {
			t.Fatalf("%s = %v, want %v", key, v, want)
		}
	}
	if _, ok := ev.Lookup(KeyError); !ok {
		t.Fatal("error field dropped at the limit")
	}
	if v, _ := ev.Lookup(KeyDroppedFields); v != int64(1) {
		t.Fatalf("%s = %v, want 1", KeyDroppedFields, v)
	}
}

func canonicalKeysForTest() []string {
	return []string{
		KeyOpDomain, KeyOpName, KeyOpID, KeyOpSource, KeyOpAttempt, KeyOpMaxAttempts,
		KeyOpCode, KeyOpOutcome, KeyHTTPMethod, KeyHTTPPath, KeyHTTPRoute, KeyHTTPStatus,
		KeyDurationMS, KeyError, KeyPanic, KeyJobScheduledAt,
	}
}

func TestIsCanonicalKeyCoversEveryKeyConstant(t *testing.T) {
	for _, key := range canonicalKeysForTest() {
		if !isCanonicalKey(key) {
			t.Fatalf("%q is not canonical", key)
		}
	}
	for _, key := range []string{"", "user_id", "http.status ", KeyDroppedFields} {
		if isCanonicalKey(key) {
			t.Fatalf("%q must not be canonical", key)
		}
	}
}

func TestMaxValueBytesTruncatesStrings(t *testing.T) {
	rt, ts := testRT(t, func(c *Config) { c.MaxValueBytes = 8 })
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j"})
	ctx := op.Context()
	Add(ctx, "short", "12345678")
	Add(ctx, "long", "123456789abc")
	AddFields(ctx, String("typed", "abcdefghijk"))
	Add(ctx, "utf8", "1234567é") // é is 2 bytes at offset 7: cut before it
	SetMessage(ctx, "message-too-long")
	op.End(nil)

	ev := ts.Events()[0]
	for key, want := range map[string]string{"short": "12345678", "long": "12345678", "typed": "abcdefgh", "utf8": "1234567"} {
		if v, _ := ev.Lookup(key); v != want {
			t.Fatalf("%s = %q, want %q", key, v, want)
		}
	}
	if ev.Message() != "message-" {
		t.Fatalf("message = %q", ev.Message())
	}
	if v, _ := ev.Lookup(KeyTruncatedValues); v != int64(4) {
		t.Fatalf("%s = %v, want 4", KeyTruncatedValues, v)
	}
}

func TestMaxValueBytesCopiesMapsBeforeCutting(t *testing.T) {
	rt, ts := testRT(t, func(c *Config) { c.MaxValueBytes = 4 })
	user := map[string]any{"body": "0123456789", "n": 42}
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j"})
	Add(op.Context(), "payload", user)
	err := errors.New(strings.Repeat("x", 100))
	op.End(&err)

	if user["body"] != "0123456789" {
		t.Fatal("the caller's map was mutated")
	}
	ev := ts.Events()[0]
	payload, _ := ev.Lookup("payload")
	if m, _ := payload.(map[string]any); m["body"] != "0123" || m["n"] != 42 {
		t.Fatalf("payload = %#v", payload)
	}
	errField, _ := ev.Lookup(KeyError)
	if m, _ := errField.(map[string]any); m["message"] != "xxxx" {
		t.Fatalf("error = %#v", errField)
	}
}

func TestMaxValueBytesCutsPanicValue(t *testing.T) {
	rt, ts := testRT(t, func(c *Config) { c.MaxValueBytes = 16 })
	func() {
		defer func() { _ = recover() }()
		op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j"})
		defer op.End(nil)
		panic(strings.Repeat("p", 1<<20))
	}()
	panicField, _ := ts.Events()[0].Lookup(KeyPanic)
	m, _ := panicField.(map[string]any)
	if v, _ := m["value"].(string); len(v) != 16 {
		t.Fatalf("panic value length = %d, want 16", len(v))
	}
}

func TestUnlimitedKeepsEverything(t *testing.T) {
	rt, ts := testRT(t, unlimited)
	big := strings.Repeat("v", DefaultMaxValueBytes*2)
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j"})
	for i := range DefaultMaxFields * 2 {
		Add(op.Context(), fmt.Sprintf("k%d", i), big)
	}
	op.End(nil)
	ev := ts.Events()[0]
	if v, _ := ev.Lookup(fmt.Sprintf("k%d", DefaultMaxFields*2-1)); v != big {
		t.Fatal("unlimited event lost or cut a value")
	}
	if _, ok := ev.Lookup(KeyDroppedFields); ok {
		t.Fatal("unlimited event reported drops")
	}
}

// The default limit must bound the WAL of a runaway loop.
func TestDefaultMaxFieldsBoundsRunawayLoop(t *testing.T) {
	rt, ts := testRT(t, nil)
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j"})
	for i := range 100_000 {
		Add(op.Context(), fmt.Sprintf("k%d", i), i)
	}
	if n := len(op.ev.fields); n != DefaultMaxFields {
		t.Fatalf("WAL holds %d fields, want %d", n, DefaultMaxFields)
	}
	op.End(nil)
	if v, _ := ts.Events()[0].Lookup(KeyDroppedFields); v != int64(100_000-DefaultMaxFields) {
		t.Fatalf("%s = %v", KeyDroppedFields, v)
	}
}

func TestTruncateUTF8(t *testing.T) {
	cases := []struct {
		in   string
		n    int
		want string
	}{
		{"", 0, ""},
		{"abc", 3, "abc"},
		{"abc", 0, ""},
		{"abc", 2, "ab"},
		{"aé", 2, "a"}, // é = c3 a9
		{"a€", 3, "a"}, // € = e2 82 ac
		{"a😀", 4, "a"}, // 😀 = f0 9f 98 80
		{"a😀b", 5, "a😀"},
		{"\x80\x80\x80\x80\x80", 4, "\x80"}, // invalid run: bounded backoff
	}
	for _, c := range cases {
		if got := truncateUTF8(c.in, c.n); got != c.want {
			t.Fatalf("truncateUTF8(%q, %d) = %q, want %q", c.in, c.n, got, c.want)
		}
	}
}

// FuzzLimits drives random writes through random limits and checks the
// invariants: the WAL stays bounded, every string fits the byte limit
// and stays valid UTF-8 when the input was, the line is valid JSON,
// and the counters are consistent with a reference model.
func FuzzLimits(f *testing.F) {
	f.Add(uint8(4), uint8(8), []byte("k1=v1;k2=vvvvvvvvvvvvvvvv;k1=x;k3=é€😀"))
	f.Add(uint8(1), uint8(1), []byte("http.status=5;a=b;error=e"))
	f.Add(uint8(0), uint8(0), []byte(""))
	f.Fuzz(func(t *testing.T, maxFields, maxBytes uint8, program []byte) {
		cfg := func(c *Config) {
			c.MaxFields = int(maxFields%32) + 1
			c.MaxValueBytes = int(maxBytes%64) + 1
		}
		rt, ts := testRT(t, cfg)
		lim := rt.limits
		op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "fuzz"})
		ctx := op.Context()

		// Reference model: the set of keys held and the drop count.
		held := map[string]bool{}
		order := 0
		var dropped, truncated int64
		for stmt := range strings.SplitSeq(string(program), ";") {
			key, value, _ := strings.Cut(stmt, "=")
			if key == "" {
				continue
			}
			kept := true
			switch {
			case order < lim.maxFields:
				order++
				held[key] = true
			case held[key]:
			case isCanonicalKey(key):
				held[key] = true
			default:
				kept = false
				dropped++
			}
			if kept && len(value) > lim.maxValueBytes {
				truncated++
			}
			if len(stmt)%2 == 0 {
				Add(ctx, key, value)
			} else {
				AddFields(ctx, String(key, value))
			}
		}
		written := len(op.ev.fields)
		if written > lim.maxFields+len(canonicalKeysForTest()) {
			t.Fatalf("WAL holds %d fields over a limit of %d", written, lim.maxFields)
		}
		op.End(nil)

		ev := ts.Events()[0]
		gotDropped, _ := ev.Lookup(KeyDroppedFields)
		if (dropped == 0 && gotDropped != nil) || (dropped > 0 && gotDropped != dropped) {
			t.Fatalf("dropped = %v, want %d", gotDropped, dropped)
		}
		gotTruncated, _ := ev.Lookup(KeyTruncatedValues)
		if (truncated == 0 && gotTruncated != nil) || (truncated > 0 && gotTruncated != truncated) {
			t.Fatalf("truncated = %v, want %d", gotTruncated, truncated)
		}
		for key := range held {
			if _, ok := ev.Lookup(key); !ok {
				t.Fatalf("held key %q missing", key)
			}
		}
		// The completion fields End appends after the written ones are
		// unolog's own and are not limited.
		for _, field := range ev.Fields()[:written] {
			if s, ok := field.Str(); ok {
				if len(s) > lim.maxValueBytes {
					t.Fatalf("%q holds %d bytes over a limit of %d", field.Key(), len(s), lim.maxValueBytes)
				}
				if utf8.ValidString(string(program)) && !utf8.ValidString(s) {
					t.Fatalf("%q was cut inside a rune: %q", field.Key(), s)
				}
			}
		}
		if !json.Valid(recOf(ev.Level(), ev.Message(), ev.Fields()...).Encoded()) {
			t.Fatal("encoded line is not valid JSON")
		}
	})
}
