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

// Each getter returns the value only for its own kind. Sinks such as the
// slog and zap adapters pick the target type from the first getter that
// reports ok, so a getter that accepts another kind changes their output.
func TestFieldGettersMatchKind(t *testing.T) {
	now := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	boom := errors.New("boom")
	type got struct {
		str string
		i   int64
		u   uint64
		fl  float64
		b   bool
		tm  time.Time
		d   time.Duration
		err error
		any any
	}
	read := func(f Field) (g got, ok []FieldKind) {
		var hit bool
		if g.str, hit = f.Str(); hit {
			ok = append(ok, KindString)
		}
		if g.i, hit = f.Int(); hit {
			ok = append(ok, KindInt)
		}
		if g.u, hit = f.Uint(); hit {
			ok = append(ok, KindUint)
		}
		if g.fl, hit = f.Float(); hit {
			ok = append(ok, KindFloat)
		}
		if g.b, hit = f.Bool(); hit {
			ok = append(ok, KindBool)
		}
		if g.tm, hit = f.Time(); hit {
			ok = append(ok, KindTime)
		}
		if g.d, hit = f.Duration(); hit {
			ok = append(ok, KindDuration)
		}
		if g.err, hit = f.Err(); hit {
			ok = append(ok, KindErr)
		}
		if g.any = f.Any(); g.any != nil {
			ok = append(ok, KindAny)
		}
		return g, ok
	}
	cases := []struct {
		f      Field
		getter FieldKind // the one getter that reports ok
		want   got
	}{
		{String("k", "v"), KindString, got{str: "v"}},
		{Int64("k", math.MinInt64), KindInt, got{i: math.MinInt64}},
		{Uint64("k", math.MaxUint64), KindUint, got{u: math.MaxUint64}},
		{Float64("k", -0.5), KindFloat, got{fl: -0.5}},
		{Any("k", float32(0.25)), KindFloat, got{fl: 0.25}},
		{Bool("k", true), KindBool, got{b: true}},
		{Time("k", now), KindTime, got{tm: now}},
		{Duration("k", -time.Second), KindDuration, got{d: -time.Second}},
		{Any("k", boom), KindErr, got{err: boom}},
		{Any("k", []int{1}), KindAny, got{}},
		{Field{kind: KindErr, val: "not an error"}, KindInvalid, got{}},
		{Field{}, KindInvalid, got{}},
	}
	for _, c := range cases {
		g, ok := read(c.f)
		switch {
		case c.getter == KindInvalid && len(ok) != 0:
			t.Fatalf("kind %v: getters %v report ok, want none", c.f.Kind(), ok)
		case c.getter != KindInvalid && (len(ok) != 1 || ok[0] != c.getter):
			t.Fatalf("kind %v: getters %v report ok, want only %v", c.f.Kind(), ok, c.getter)
		}
		if c.getter == KindAny {
			if v, isSlice := g.any.([]int); !isSlice || len(v) != 1 || v[0] != 1 {
				t.Fatalf("Any() = %#v, want []int{1}", g.any)
			}
			g.any = nil
		}
		if !g.tm.Equal(c.want.tm) || g.err != c.want.err { //nolint:errorlint // identity check
			t.Fatalf("kind %v: got %+v, want %+v", c.f.Kind(), g, c.want)
		}
		g.tm, c.want.tm = time.Time{}, time.Time{}
		if g != c.want {
			t.Fatalf("kind %v: got %+v, want %+v", c.f.Kind(), g, c.want)
		}
	}
}

// Add maps every Go integer width to one kind, and Lookup returns the
// widened value.
func TestFieldOfMapsEveryType(t *testing.T) {
	now := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		value any
		kind  FieldKind
		back  any // valueOf result
	}{
		{"s", KindString, "s"},
		{int(-1), KindInt, int64(-1)},
		{int8(math.MinInt8), KindInt, int64(math.MinInt8)},
		{int16(math.MinInt16), KindInt, int64(math.MinInt16)},
		{int32(math.MinInt32), KindInt, int64(math.MinInt32)},
		{int64(math.MinInt64), KindInt, int64(math.MinInt64)},
		{uint(math.MaxUint), KindUint, uint64(math.MaxUint)},
		{uint8(math.MaxUint8), KindUint, uint64(math.MaxUint8)},
		{uint16(math.MaxUint16), KindUint, uint64(math.MaxUint16)},
		{uint32(math.MaxUint32), KindUint, uint64(math.MaxUint32)},
		{uint64(math.MaxUint64), KindUint, uint64(math.MaxUint64)},
		{float32(0.5), KindFloat32, float32(0.5)},
		{0.25, KindFloat, 0.25},
		{true, KindBool, true},
		{now, KindTime, now},
		{time.Minute, KindDuration, time.Minute},
		{struct{}{}, KindAny, struct{}{}},
	}
	for _, c := range cases {
		f := fieldOf("k", c.value)
		if f.Kind() != c.kind {
			t.Fatalf("%T: kind %v, want %v", c.value, f.Kind(), c.kind)
		}
		if got := valueOf(f); got != c.back {
			t.Fatalf("%T: valueOf = %#v, want %#v", c.value, got, c.back)
		}
	}
	if got := valueOf(Field{}); got != nil {
		t.Fatalf("valueOf(Field{}) = %#v, want nil", got)
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

// End seals the event before it runs a custom sampler, so a write from
// inside the sampler (same generation, End not yet returned) is dropped.
func TestWriteFromSamplerIsDropped(t *testing.T) {
	var ctx context.Context
	rt, ts := testRT(t, func(c *Config) {
		c.Sampler = func(SampleInput) bool {
			Add(ctx, "late", "x")
			AddFields(ctx, String("late2", "y"))
			return true
		}
	})
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j"})
	ctx = op.Context()
	op.End(nil)

	ev := ts.Events()[0]
	for _, key := range []string{"late", "late2"} {
		if v, ok := ev.Lookup(key); ok {
			t.Fatalf("%s = %v: a write after the seal reached the record", key, v)
		}
	}
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
