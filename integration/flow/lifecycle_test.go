package flow

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/happytoolin/unolog"
)

func TestResolveStatus(t *testing.T) {
	boom := errors.New("boom")
	for _, c := range []struct {
		name string
		in   StatusInput
		want int
	}{
		{"unwritten", StatusInput{}, http.StatusOK},
		{"committed", StatusInput{Committed: http.StatusCreated}, http.StatusCreated},
		{"panic before response", StatusInput{Committed: http.StatusOK, Recovered: "x"}, http.StatusInternalServerError},
		{"panic after response", StatusInput{Committed: http.StatusAccepted, Recovered: "x", ResponseStarted: true}, http.StatusAccepted},
		{"error without status", StatusInput{Err: boom}, http.StatusInternalServerError},
		{"error with 4xx status", StatusInput{Err: boom, ErrorStatus: http.StatusBadRequest}, http.StatusBadRequest},
		{"error with non-error status", StatusInput{Err: boom, ErrorStatus: 399}, http.StatusInternalServerError},
		{"error after response", StatusInput{Committed: http.StatusOK, Err: boom, ErrorStatus: http.StatusNotFound, ResponseStarted: true}, http.StatusOK},
		{"error status without error", StatusInput{ErrorStatus: http.StatusNotFound}, http.StatusOK},
	} {
		if got := ResolveStatus(c.in); got != c.want {
			t.Errorf("%s: ResolveStatus = %d, want %d", c.name, got, c.want)
		}
	}
}

func TestPanicField(t *testing.T) {
	got := PanicField(errors.New("boom"))
	if got["type"] != "*errors.errorString" || got["value"] != "boom" {
		t.Fatalf("PanicField = %v", got)
	}
}

func lookup(t *testing.T, ev unolog.CapturedEvent, key string) any {
	t.Helper()
	v, ok := ev.Lookup(key)
	if !ok {
		t.Fatalf("event has no %s field", key)
	}
	return v
}

func errorText(t *testing.T, ev unolog.CapturedEvent) string {
	t.Helper()
	field, _ := lookup(t, ev, unolog.KeyError).(map[string]any)
	msg, ok := field["message"].(string)
	if !ok {
		t.Fatalf("error field %v has no message", field)
	}
	return msg
}

func TestFinalizeRequest(t *testing.T) {
	handlerErr := errors.New("handler failed")
	for _, c := range []struct {
		name      string
		route     string
		status    int
		err       error
		recovered any
		check     func(t *testing.T, ev unolog.CapturedEvent)
	}{
		{name: "route", route: "/users/{id}", status: http.StatusOK, check: func(t *testing.T, ev unolog.CapturedEvent) {
			t.Helper()
			if lookup(t, ev, unolog.KeyHTTPRoute) != "/users/{id}" || lookup(t, ev, unolog.KeyOpName) != "/users/{id}" {
				t.Fatal("route or op.name is not the route template")
			}
			if lookup(t, ev, unolog.KeyOpOutcome) != string(unolog.OutcomeSuccess) {
				t.Fatal("outcome is not success")
			}
		}},
		{name: "no route", status: http.StatusNotFound, check: func(t *testing.T, ev unolog.CapturedEvent) {
			t.Helper()
			if _, ok := ev.Lookup(unolog.KeyHTTPRoute); ok {
				t.Fatal("event has a route without one")
			}
			if lookup(t, ev, unolog.KeyOpName) != "request" {
				t.Fatal("op.name is not the start name")
			}
		}},
		{name: "error", status: http.StatusInternalServerError, err: handlerErr, check: func(t *testing.T, ev unolog.CapturedEvent) {
			t.Helper()
			if errorText(t, ev) != "handler failed" || lookup(t, ev, unolog.KeyOpOutcome) != string(unolog.OutcomeFailure) {
				t.Fatal("error path lost the handler error")
			}
		}},
		{name: "panic", status: http.StatusInternalServerError, recovered: "boom", check: func(t *testing.T, ev unolog.CapturedEvent) {
			t.Helper()
			if errorText(t, ev) != "panic: boom" || lookup(t, ev, unolog.KeyOpOutcome) != string(unolog.OutcomePanic) {
				t.Fatal("panic path has the wrong error or outcome")
			}
			if p, _ := lookup(t, ev, unolog.KeyPanic).(map[string]any); p["value"] != "boom" {
				t.Fatalf("panic field = %v", p)
			}
		}},
		{name: "panic with error", status: http.StatusInternalServerError, err: handlerErr, recovered: "boom", check: func(t *testing.T, ev unolog.CapturedEvent) {
			t.Helper()
			if errorText(t, ev) != "handler failed" || lookup(t, ev, unolog.KeyOpOutcome) != string(unolog.OutcomePanic) {
				t.Fatal("the handler error does not win over the panic text")
			}
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			sink := unolog.NewTestSink()
			rt := unolog.MustCompile(unolog.Config{Sink: sink, SamplingRate: 1})
			op := StartRequest(context.Background(), rt, http.MethodGet, "/users/7")
			FinalizeRequest(op, c.route, c.status, c.err, c.recovered)
			events := sink.Events()
			if len(events) != 1 {
				t.Fatalf("events = %d, want 1", len(events))
			}
			ev := events[0]
			if lookup(t, ev, unolog.KeyHTTPMethod) != http.MethodGet || lookup(t, ev, unolog.KeyHTTPPath) != "/users/7" {
				t.Fatal("start fields are missing")
			}
			if lookup(t, ev, unolog.KeyHTTPStatus) != int64(c.status) {
				t.Fatalf("status = %v, want %d", lookup(t, ev, unolog.KeyHTTPStatus), c.status)
			}
			c.check(t, ev)
		})
	}
	FinalizeRequest(nil, "/r", http.StatusOK, nil, nil) // nil operation: no-op
}
