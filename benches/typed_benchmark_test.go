package benches_test

import (
	"context"
	"testing"

	"github.com/happytoolin/unolog"
)

// benchmarkFieldsOpTyped is benchmarkFieldsOp through the typed entry
// points: the same twelve-field corpus with the kind fixed at each call
// site, so no value crosses as an `any`.
func benchmarkFieldsOpTyped(op *unolog.Operation) {
	op.AddString("http.method", "GET")
	op.AddString("http.path", "/api/v1/orders/12345")
	op.AddString("http.route", "/api/v1/orders/:id")
	op.AddInt("http.status", 200)
	op.AddString("op.domain", "http")
	op.AddString("op.name", "GET /api/v1/orders/:id")
	op.AddString("op.outcome", "success")
	op.AddInt("op.code", 200)
	op.AddInt("duration_ms", 12)
	op.AddString("request_id", "req_01HZX4T7W8Y3N2M1K0J9Z8X7V6")
	op.AddString("user_id", "usr_77451")
	op.AddBool("cache.hit", true)
}

// BenchmarkOperationLifecycle12FieldsTyped is the 12-field lifecycle
// with every field written through a typed method. Compare with
// BenchmarkOperationLifecycle12Fields: the delta is what the `any`
// boundary costs on this corpus.
func BenchmarkOperationLifecycle12FieldsTyped(b *testing.B) {
	rt := runtimeFor()
	b.ReportAllocs()
	for b.Loop() {
		op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainHTTP, Name: "GET /api/v1/orders/:id"})
		benchmarkFieldsOpTyped(&op)
		op.End(nil)
	}
}

// BenchmarkOperationLifecycle12FieldsMixed is the realistic adoption
// shape: the scalar fields typed, the rarer bool and the two ints still
// on Add. It is the gate for mixing the two forms in one operation.
func BenchmarkOperationLifecycle12FieldsMixed(b *testing.B) {
	rt := runtimeFor()
	b.ReportAllocs()
	for b.Loop() {
		op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainHTTP, Name: "GET /api/v1/orders/:id"})
		op.AddString("http.method", "GET")
		op.AddString("http.path", "/api/v1/orders/12345")
		op.AddString("http.route", "/api/v1/orders/:id")
		op.Add("http.status", 200)
		op.AddString("op.domain", "http")
		op.AddString("op.name", "GET /api/v1/orders/:id")
		op.AddString("op.outcome", "success")
		op.Add("op.code", 200)
		op.Add("duration_ms", 12)
		op.AddString("request_id", "req_01HZX4T7W8Y3N2M1K0J9Z8X7V6")
		op.AddString("user_id", "usr_77451")
		op.Add("cache.hit", true)
		op.End(nil)
	}
}
