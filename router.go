package lymbo

import (
	"context"
	"errors"
	"sync"
)

// Router manages ticket type to handler mappings.
type Router struct {
	mu              sync.RWMutex
	routingTable    map[string]Handler
	notFoundHandler Handler
}

// NewRouter creates a new Router instance.
func NewRouter() *Router {
	return &Router{
		routingTable: make(map[string]Handler),
	}
}

// DefaultRouter is the default global router instance.
var DefaultRouter = NewRouter()

// HandlerFunc is an adapter to allow ordinary functions to be used as handlers.
type HandlerFunc func(context.Context, Ticket) error

// ProcessTicket implements the Handler interface for HandlerFunc.
func (f HandlerFunc) ProcessTicket(ctx context.Context, t Ticket) error {
	return f(ctx, t)
}

// Handler processes tickets.
type Handler interface {
	ProcessTicket(ctx context.Context, ticket Ticket) error
}

// Handler returns the handler for the given ticket.
// If no handler is registered for the ticket type, returns the not-found handler.
func (r *Router) Handler(t *Ticket) Handler {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if handler, exists := r.routingTable[t.Type]; exists {
		return handler
	}
	return r.NotFoundHandler()
}

// NotFoundHandler returns the handler to use when no route matches.
func (r *Router) NotFoundHandler() Handler {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.notFoundHandler != nil {
		return r.notFoundHandler
	}
	return NotFoundHandler()
}

// HandleFunc registers a handler function for the given route.
func (r *Router) HandleFunc(route string, handler func(context.Context, Ticket) error) error {
	return r.register(route, HandlerFunc(handler))
}

// Handle registers a handler for the given route.
func (r *Router) Handle(route string, handler Handler) error {
	return r.register(route, handler)
}

// NotFound sets the handler to use when no route matches.
// Panics if handler is nil.
func (r *Router) NotFound(handler Handler) {
	if handler == nil {
		panic("kharon: nil not found handler")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.notFoundHandler = handler
}

// NotFoundFunc sets a handler function to use when no route matches.
// Panics if handler is nil.
func (r *Router) NotFoundFunc(handler func(context.Context, Ticket) error) {
	if handler == nil {
		panic("kharon: nil not found handler")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.notFoundHandler = HandlerFunc(handler)
}

func (r *Router) register(route string, handler Handler) error {
	if route == "" {
		return errors.New("route cannot be empty")
	}
	if handler == nil {
		return errors.New("handler cannot be nil")
	}
	if f, ok := handler.(HandlerFunc); ok && f == nil {
		return errors.New("handler cannot be nil")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.routingTable[route]; exists {
		return errors.New("route already registered: " + route)
	}
	r.routingTable[route] = handler
	return nil
}

// Handle registers a handler for the given route on the default router.
func Handle(route string, handler Handler) error {
	return DefaultRouter.register(route, handler)
}

// HandleFunc registers a handler function for the given route on the default router.
func HandleFunc(route string, handler func(context.Context, Ticket) error) error {
	return DefaultRouter.register(route, HandlerFunc(handler))
}

// NotFound is the default not-found handler function.
func NotFound(context.Context, Ticket) error {
	return ErrHandlerNotFound
}

// NotFoundHandler returns a Handler that returns ErrHandlerNotFound.
func NotFoundHandler() Handler {
	return HandlerFunc(NotFound)
}
