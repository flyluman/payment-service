package middleware

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/crownroutes/payment-service/internal/ports"
)

type captureLog struct {
	mu    sync.Mutex
	calls []logCall
}

type logCall struct {
	event  string
	fields map[string]any
}

func (c *captureLog) Info(event string, fields map[string]any) {
	c.mu.Lock()
	c.calls = append(c.calls, logCall{event: event, fields: fields})
	c.mu.Unlock()
}
func (c *captureLog) Warn(string, map[string]any)  {}
func (c *captureLog) Error(string, map[string]any, error) {}
func (c *captureLog) Debug(string, map[string]any) {}
func (c *captureLog) Trace(string, map[string]any) {}
func (c *captureLog) With(map[string]any) ports.Logger { return c }

func (c *captureLog) last() logCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.calls) == 0 {
		return logCall{}
	}
	return c.calls[len(c.calls)-1]
}

func TestRequestLog_DefaultStatus(t *testing.T) {
	log := &captureLog{}
	mw := RequestLog(log)

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})

	handler := mw(inner)
	req := httptest.NewRequest(http.MethodGet, "/test/path?foo=bar", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	call := log.last()
	if call.event != "http.request" {
		t.Fatalf("expected http.request event, got %s", call.event)
	}
	if call.fields["status"] != http.StatusOK {
		t.Fatalf("expected status 200, got %v", call.fields["status"])
	}
	if call.fields["method"] != http.MethodGet {
		t.Fatalf("expected GET, got %v", call.fields["method"])
	}
	if call.fields["path"] != "/test/path" {
		t.Fatalf("expected /test/path, got %v", call.fields["path"])
	}
	if call.fields["query"] != "foo=bar" {
		t.Fatalf("expected foo=bar, got %v", call.fields["query"])
	}
}

func TestRequestLog_ForwardedStatus(t *testing.T) {
	log := &captureLog{}
	mw := RequestLog(log)

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})

	handler := mw(inner)
	req := httptest.NewRequest(http.MethodGet, "/missing", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	call := log.last()
	if call.fields["status"] != http.StatusNotFound {
		t.Fatalf("expected 404, got %v", call.fields["status"])
	}
}

func TestRequestLog_DurationMeasured(t *testing.T) {
	log := &captureLog{}
	mw := RequestLog(log)

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})

	handler := mw(inner)
	req := httptest.NewRequest(http.MethodGet, "/fast", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	call := log.last()
	if _, ok := call.fields["duration_ms"]; !ok {
		t.Fatal("expected duration_ms field")
	}
}

func TestRequestLog_Flush(t *testing.T) {
	log := &captureLog{}
	mw := RequestLog(log)

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	})

	handler := mw(inner)
	req := httptest.NewRequest(http.MethodGet, "/flush", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)
}
