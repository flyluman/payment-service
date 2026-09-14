package outbox

import (
	"context"

	"github.com/crownroutes/payment-service/internal/ports"
)

// Handler processes an outbox event.
type Handler interface {
	Handle(ctx context.Context, event ports.PendingEvent) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(ctx context.Context, event ports.PendingEvent) error

func (f HandlerFunc) Handle(ctx context.Context, event ports.PendingEvent) error {
	return f(ctx, event)
}

// Route binds an event type pattern to a Handler.
// EventType "*" matches all events.
type Route struct {
	EventType string
	Handler   Handler
}

// Router dispatches events to matching Route handlers.
// Implements relay.Publisher.
type Router struct {
	routes []Route
}

func NewRouter(routes ...Route) *Router {
	return &Router{routes: routes}
}

func (r *Router) Publish(ctx context.Context, event ports.PendingEvent) error {
	for _, route := range r.routes {
		if route.EventType != "*" && route.EventType != event.EventType {
			continue
		}
		if err := route.Handler.Handle(ctx, event); err != nil {
			return err
		}
	}
	return nil
}
