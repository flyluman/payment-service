package outbox

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/ports"
)

type spyHandler struct {
	called int
	events []ports.PendingEvent
	err    error
}

func (s *spyHandler) Handle(_ context.Context, event ports.PendingEvent) error {
	s.called++
	s.events = append(s.events, event)
	return s.err
}

func pendingEvent(eventType string) ports.PendingEvent {
	return ports.PendingEvent{
		ID:        uuid.New(),
		EventType: eventType,
		Payload:   []byte(`{}`),
	}
}

func TestExactMatch(t *testing.T) {
	h := &spyHandler{}
	router := NewRouter(Route{EventType: "TRANSACTION_CREATED", Handler: h})

	err := router.Publish(context.Background(), pendingEvent("TRANSACTION_CREATED"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h.called != 1 {
		t.Fatalf("expected handler to be called once, got %d", h.called)
	}
}

func TestWildcardMatch(t *testing.T) {
	h := &spyHandler{}
	router := NewRouter(Route{EventType: "*", Handler: h})

	err := router.Publish(context.Background(), pendingEvent("ANY_EVENT"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h.called != 1 {
		t.Fatalf("expected handler to be called once, got %d", h.called)
	}
}

func TestNoMatch(t *testing.T) {
	h := &spyHandler{}
	router := NewRouter(Route{EventType: "TRANSACTION_CREATED", Handler: h})

	err := router.Publish(context.Background(), pendingEvent("SOMETHING_ELSE"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h.called != 0 {
		t.Fatalf("handler should not have been called, got %d", h.called)
	}
}

func TestMultipleMatchingRoutes(t *testing.T) {
	h1 := &spyHandler{}
	h2 := &spyHandler{}
	router := NewRouter(
		Route{EventType: "TRANSACTION_CREATED", Handler: h1},
		Route{EventType: "TRANSACTION_CREATED", Handler: h2},
	)

	err := router.Publish(context.Background(), pendingEvent("TRANSACTION_CREATED"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h1.called != 1 || h2.called != 1 {
		t.Fatalf("both handlers should be called once, got h1=%d h2=%d", h1.called, h2.called)
	}
}

func TestErrorShortCircuits(t *testing.T) {
	h1 := &spyHandler{err: errors.New("boom")}
	h2 := &spyHandler{}
	router := NewRouter(
		Route{EventType: "TRANSACTION_CREATED", Handler: h1},
		Route{EventType: "TRANSACTION_CREATED", Handler: h2},
	)

	err := router.Publish(context.Background(), pendingEvent("TRANSACTION_CREATED"))
	if err == nil || err.Error() != "boom" {
		t.Fatalf("expected 'boom' error, got %v", err)
	}
	if h2.called != 0 {
		t.Fatalf("second handler should not be called after error, got %d", h2.called)
	}
}

func TestHandlerFuncAdapter(t *testing.T) {
	called := false
	h := HandlerFunc(func(_ context.Context, _ ports.PendingEvent) error {
		called = true
		return nil
	})

	router := NewRouter(Route{EventType: "*", Handler: h})
	_ = router.Publish(context.Background(), pendingEvent("TEST"))

	if !called {
		t.Fatal("HandlerFunc was not called")
	}
}

func TestSpecificRouteBeforeWildcardBlocksWildcardOnError(t *testing.T) {
	// The relay relies on side-effect routes being registered before the "*"
	// SNS fan-out: if a side effect fails, the fan-out must not run so the
	// retry does not publish a duplicate SNS message.
	sideEffect := &spyHandler{err: errors.New("gateway call failed")}
	base := &spyHandler{}
	router := NewRouter(
		Route{EventType: "GATEWAY_INITIATE", Handler: sideEffect},
		Route{EventType: "*", Handler: base},
	)

	err := router.Publish(context.Background(), pendingEvent("GATEWAY_INITIATE"))
	if err == nil {
		t.Fatal("expected side-effect error to propagate")
	}
	if sideEffect.called != 1 {
		t.Fatalf("side-effect handler should run once, got %d", sideEffect.called)
	}
	if base.called != 0 {
		t.Fatalf("wildcard fan-out must not run after side-effect error, got %d calls", base.called)
	}
}

func TestEmptyRouter(t *testing.T) {
	router := NewRouter()
	err := router.Publish(context.Background(), pendingEvent("TEST"))
	if err != nil {
		t.Fatalf("empty router should return nil, got %v", err)
	}
}
