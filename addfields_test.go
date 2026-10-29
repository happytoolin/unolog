package unolog

import (
	"bytes"
	"context"
	"errors"
	"math"
	"strconv"
	"testing"
	"time"
)

// Each constructor must produce the field Add produces for the same
// value, so AddFields and Add are interchangeable on the wire.
func TestFieldConstructorsMatchAdd(t *testing.T) {
	now := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	err := errors.New("boom")
	cases := []struct {
		typed Field
		value any
	}{
		{String("k", "v"), "v"},
		{Int("k", -42), -42},
		{Int64("k", math.MinInt64), int64(math.MinInt64)},
		{Uint64("k", math.MaxUint64), uint64(math.MaxUint64)},
		{Float64("k", 0.1), 0.1},
		{Bool("k", true), true},
		{Time("k", now), now},
		{Duration("k", 1500*time.Millisecond), 1500 * time.Millisecond},
		{Any("k", err), err},
		{Any("k", float32(0.1)), float32(0.1)},
		{Any("k", map[string]any{"a": 1}), nil}, // compared by encoding below
	}
	for _, c := range cases {
		want := fieldOf("k", c.value)
		if c.value == nil {
			want = fieldOf("k", map[string]any{"a": 1})
		}
		got := recOf(LevelInfo, "m", c.typed).Encoded()
		exp := recOf(LevelInfo, "m", want).Encoded()
		if !bytes.Equal(got, exp) {
			t.Fatalf("%v: typed line %s, Add line %s", c.typed.Kind(), got, exp)
		}
		if c.typed.Kind() != want.Kind() {
			t.Fatalf("kind %v, want %v", c.typed.Kind(), want.Kind())
		}
	}
}

func TestAddFieldsLifecycle(t *testing.T) {
	rt, ts := testRT(t, nil)
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j"})
	ctx := op.Context()
	AddFields(ctx, String("user", "u1"), Field{}, Int("", 3), Int("n", 7))
	AddFields(ctx) // no fields: no-op
	op.End(nil)
	AddFields(ctx, String("late", "x")) // sealed: dropped

	ev := ts.Events()[0]
	if v, _ := ev.Lookup("user"); v != "u1" {
		t.Fatalf("user = %v", v)
	}
	if v, _ := ev.Lookup("n"); v != int64(7) {
		t.Fatalf("n = %v", v)
	}
	for _, f := range ev.Fields() {
		if f.Key() == "" || f.Key() == "late" {
			t.Fatalf("unexpected field %q", f.Key())
		}
	}
	AddFields(context.Background(), String("no", "event")) // no WAL: no-op
	AddFields(nil, String("nil", "ctx"))                   //nolint:staticcheck // nil ctx is a documented no-op
}

var sinkDynamic = strconv.Itoa(987654321)

// AddFields must not allocate for dynamic values; Add allocates one box
// per non-constant value. The difference is the reason AddFields exists.
func TestAddFieldsDoesNotAllocate(t *testing.T) {
	op := Start(context.Background(), MustCompile(Config{Sink: sinkFunc(func(context.Context, *Record) {}), SamplingRate: 1}), OperationStart{})
	defer op.End(nil)
	ctx := op.Context()
	n := len(sinkDynamic)
	ratio := float64(n) / 3
	// Fill past the first growth so append does not allocate below.
	for range 8 {
		AddFields(ctx, String("warm", sinkDynamic))
	}
	op.ev.fields = op.ev.fields[:0]
	allocs := testing.AllocsPerRun(100, func() {
		AddFields(ctx, String("user", sinkDynamic), Int("n", n), Float64("ratio", ratio))
		op.ev.fields = op.ev.fields[:0]
	})
	if allocs != 0 {
		t.Fatalf("AddFields allocated %.1f times per call, want 0", allocs)
	}
}
