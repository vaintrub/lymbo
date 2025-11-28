package lymbo

import (
	"context"
	"errors"
	"sync"
)

type Router struct {
	mu              sync.RWMutex
	routingTable    map[string]Handler
	notFoundHandler Handler
}

func NewRouter() *Router {
	return &Router{
		routingTable: make(map[string]Handler),
	}
}

var DefaultRouter = NewRouter()

type HandlerFunc func(context.Context, Ticket) error

func (f HandlerFunc) ProcessTicket(ctx context.Context, t Ticket) error {
	return f(ctx, t)
}

type Handler interface {
	ProcessTicket(ctx context.Context, ticket Ticket) error
}

// Returns handler for given Ticket
func (r *Router) Handler(t *Ticket) Handler {
	typ := t.Type
	r.mu.RLock()
	defer r.mu.RUnlock()
	if handler, exists := r.routingTable[typ]; exists {
		return handler
	}
	return r.NotFoundHandler()
}

func (r *Router) NotFoundHandler() Handler {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.notFoundHandler != nil {
		return r.notFoundHandler
	}
	return NotFoundHandler()
}

// Register a handler function for given route
func (r *Router) HandleFunc(route string, handler func(context.Context, Ticket) error) error {
	return r.register(route, HandlerFunc(handler))
}

// Register a handler for given route
func (r *Router) Handle(route string, handler Handler) error {
	return r.register(route, handler)
}

func (r *Router) NotFound(handler Handler) {
	if handler == nil {
		panic("kharon: nil not found handler")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.notFoundHandler = handler
}

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
		return errors.New("kharon: route cannot be empty")
	}
	if handler == nil {
		return errors.New("kharon: nil handler")
	}
	if f, ok := handler.(HandlerFunc); ok && f == nil {
		return errors.New("kharon: nil handler")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	_, exists := r.routingTable[route]
	if exists {
		return errors.New("route already registered: " + route)
	}
	r.routingTable[route] = handler
	return nil
}

func Handle(route string, handler Handler) error {
	return DefaultRouter.register(route, handler)
}

func HandleFunc(route string, handler func(context.Context, Ticket) error) error {
	return DefaultRouter.register(route, HandlerFunc(handler))
}

func NotFound(context.Context, Ticket) error {
	return ErrHandlerNotFound
}

func NotFoundHandler() Handler { return HandlerFunc(NotFound) }
