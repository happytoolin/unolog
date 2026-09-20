package unolog

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

// TestOperationTypedAddMatchesAdd pins the typed family against Add:
// same value, same wire kind, same drop rules. A typed method that
// filled the wrong kind word, skipped the payload zeroing, or lost the
// generation guard would fail here.
func TestOperationTypedAddMatchesAdd(t *testing.T) {
	timeValue := time.Date(2026, 9, 20, 1, 2, 3, 4, time.UTC)
	errValue := errors.New("boom")
	cases := []struct {
		name string
		add  func(op *Operation)
		key  string
		want any
		kind FieldKind
	}{
		{"string", func(op *Operation) { op.AddString("k", "v") }, "k", "v", KindString},
		{"int", func(op *Operation) { op.AddInt("k", -42) }, "k", int64(-42), KindInt},
		{"uint", func(op *Operation) { op.AddUint("k", 42) }, "k", uint64(42), KindUint},
		{"float64", func(op *Operation) { op.AddFloat64("k", 1.5) }, "k", 1.5, KindFloat},
		{"bool", func(op *Operation) { op.AddBool("k", true) }, "k", true, KindBool},
		{"duration", func(op *Operation) { op.AddDuration("k", 1500*time.Millisecond) }, "k", 1500 * time.Millisecond, KindDuration},
		{"time", func(op *Operation) { op.AddTime("k", timeValue) }, "k", timeValue, KindTime},
		{"error", func(op *Operation) { op.AddError("k", errValue) }, "k", errValue, KindErr},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rt, ts := testRT(t, nil)
			op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j"})
			tc.add(&op)
			if !op.End(nil) {
				t.Fatal("event not emitted")
			}
			events := ts.Events()
			if len(events) != 1 {
				t.Fatalf("events = %d, want 1", len(events))
			}
			f := fieldByKey(t, events[0], tc.key)
			if f.Kind() != tc.kind {
				t.Fatalf("kind = %v, want %v", f.Kind(), tc.kind)
			}
			got, ok := events[0].Lookup(tc.key)
			if !ok {
				t.Fatalf("lookup(%q) not found", tc.key)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("value = %#v, want %#v", got, tc.want)
			}
		})
	}
}

// TestOperationTypedAddDrops pins the drop rules the typed methods
// inherit from Add: nil handle, zero Operation, empty key, nil error,
// and a write after End.
func TestOperationTypedAddDrops(t *testing.T) {
	var nilOp *Operation
	nilOp.AddString("k", "v")
	var zero Operation
	zero.AddString("k", "v")
	zero.AddTime("k", time.Now())
	zero.AddError("k", errors.New("x"))

	rt, ts := testRT(t, nil)
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j"})
	op.AddString("", "dropped")
	op.AddInt("", 1)
	op.AddError("e", nil)
	op.AddString("kept", "yes")
	if !op.End(nil) {
		t.Fatal("event not emitted")
	}
	op.AddString("after", "dropped")

	events := ts.Events()
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	for _, k := range []string{"", "e", "after"} {
		if _, ok := events[0].Lookup(k); ok {
			t.Fatalf("field %q should have been dropped", k)
		}
	}
	if got, ok := events[0].Lookup("kept"); !ok || got != "yes" {
		t.Fatalf("kept = %#v, %v; want \"yes\", true", got, ok)
	}
}

// TestAddStringsMatchesAdd pins the batch string form against the pair
// rules Add documents: insertion order, empty-key skip, odd trailing
// drop, silent no-op with no event and after End.
func TestAddStringsMatchesAdd(t *testing.T) {
	rt, ts := testRT(t, nil)
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j"})
	ctx := op.Context()
	AddStrings(ctx, "a", "1", "b", "2", "", "skipped", "d", "4", "trailing")
	if !op.End(nil) {
		t.Fatal("event not emitted")
	}
	events := ts.Events()
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	// The user fields are the WAL prefix; End appends the canonical
	// start/outcome annotations after them.
	want := []string{"a", "b", "d"}
	all := events[0].Fields()
	if len(all) < len(want) {
		t.Fatalf("fields = %d, want at least %d", len(all), len(want))
	}
	got := make([]string, 0, len(want))
	for _, f := range all[:len(want)] {
		got = append(got, f.Key())
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("leading keys = %v, want %v", got, want)
	}
	for k, v := range map[string]string{"a": "1", "b": "2", "d": "4"} {
		got, ok := events[0].Lookup(k)
		if !ok || got != v {
			t.Fatalf("lookup(%q) = %#v, %v; want %q, true", k, got, ok, v)
		}
	}
	if _, ok := events[0].Lookup("trailing"); ok {
		t.Fatal("odd trailing value should have been dropped")
	}
	if _, ok := events[0].Lookup(""); ok {
		t.Fatal("empty key should have been skipped")
	}

	// After End every later write is dropped: no panic, no new event.
	AddStrings(ctx, "late", "x")
	if n := len(ts.Events()); n != 1 {
		t.Fatalf("events after End = %d, want 1", n)
	}
	// A nil context carries no operation and is a silent no-op.
	AddStrings(nil, "none", "x")
}

func fieldByKey(t *testing.T, rec CapturedEvent, key string) Field {
	t.Helper()
	for _, f := range rec.Fields() {
		if f.Key() == key {
			return f
		}
	}
	t.Fatalf("field %q not found", key)
	return Field{}
}
