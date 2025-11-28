# Kharon

A simple and efficient Go library for delayed task processing and state reconciliation.

## Overview

Kharon is a task orchestration library that allows you to schedule, process, and manage tickets (tasks) with built-in retry logic, expiration, and priority handling. Perfect for background job processing, state reconciliation, and delayed task execution.

## Installation

```bash
go get github.com/ochaton/lymbo
```

## Quick Start

```go
package main

import (
    "context"
    "log/slog"
    "time"

    "github.com/ochaton/lymbo"
    "github.com/ochaton/lymbo/store"
)

func main() {
    ctx := context.Background()
    logger := slog.Default()

    // Create Kharon with default settings
    settings := lymbo.DefaultSettings()
    kh := lymbo.NewKharon(store.NewMemoryStore(), settings, logger)

    // Create a router to handle different ticket types
    r := lymbo.NewRouter()
    r.HandleFunc("example", func(ctx context.Context, t lymbo.Ticket) error {
        logger.InfoContext(ctx, "processing ticket", "id", t.ID, "payload", t.Payload)
        // Do your work here
        return kh.Done(ctx, t.ID)
    })

    // Start processing tickets
    go kh.Run(ctx, r)

    // Add a ticket
    ticket, _ := lymbo.NewTicket(lymbo.TicketId("task-1"), "example")
    kh.Add(ctx, *ticket)
}
```

## Usage

### Creating Tickets

Tickets are the unit of work in Kharon.

```go
// Basic ticket
ticket, err := lymbo.NewTicket(lymbo.TicketId("unique-id"), "task-type")

// Ticket with payload
ticket = ticket.WithPayload([]byte(`{"key": "value"}`))

// Ticket with priority (nice value: lower = higher priority)
ticket = ticket.WithNice(5)

// Ticket with delayed execution
ticket = ticket.WithRunat(time.Now().Add(1 * time.Hour))

// Add ticket to Kharon
err = kh.Add(ctx, *ticket)
```

### Handling Tickets

Use the Router to register handlers for different ticket types:

```go
r := lymbo.NewRouter()

// Register a handler for a specific type
r.HandleFunc("email", func(ctx context.Context, t lymbo.Ticket) error {
    // Process email ticket
    sendEmail(t.Payload)
    return kh.Done(ctx, t.ID)
})

// Handle unknown ticket types
r.NotFoundFunc(func(ctx context.Context, t lymbo.Ticket) error {
    return kh.Fail(ctx, t.ID, lymbo.WithErrorReason("unsupported type"))
})
```

### Managing Ticket State

```go
// Mark ticket as successfully completed
kh.Done(ctx, ticketID)

// Mark with expiration time
kh.Done(ctx, ticketID, lymbo.WithExpireIn(10*time.Second))

// Mark ticket as failed
kh.Fail(ctx, ticketID, lymbo.WithErrorReason("processing failed"))

// Cancel a ticket
kh.Cancel(ctx, ticketID)

// Delete a ticket
kh.Delete(ctx, ticketID)

// Get ticket status
ticket, err := kh.Get(ctx, ticketID)
```

## Configuration

### Default Settings

```go
settings := lymbo.DefaultSettings()
```

Default values:

- **Workers**: 4 concurrent workers
- **BatchSize**: 10 tickets per poll
- **ProcessTime**: 30 seconds (time-to-run before retry)
- **MaxPollInterval**: 15 seconds (max time between polls)
- **MinPollInterval**: 10 milliseconds (min time between polls)
- **EnableExpiration**: true (automatic cleanup)

### Customizing Settings

```go
settings := lymbo.DefaultSettings().
    WithWorkers(10).                          // 10 concurrent workers
    WithBatchSize(20).                        // Poll up to 20 tickets at once
    WithProcessTime(5 * time.Minute).         // 5 minutes before retry
    WithExpiration()                          // Enable automatic expiration cleanup

kh := lymbo.NewKharon(store.NewMemoryStore(), settings, logger)
```

### Configuration Options

| Method | Description | Default |
|--------|-------------|---------|
| `WithWorkers(n)` | Number of concurrent ticket processors | 4 |
| `WithBatchSize(n)` | Max tickets to poll at once (capped at workers) | 10 |
| `WithProcessTime(d)` | Time before a ticket can be retried | 30s |
| `WithExpiration()` | Enable automatic cleanup of expired tickets | true |
| `WithoutExpiration()` | Disable automatic cleanup | - |

### Ticket Options

When marking tickets as done, failed or cancelled, you can use these options:

```go
// Expire ticket after duration
lymbo.WithExpireIn(10 * time.Second)

// Keep ticket in store (for audit trail)
lymbo.WithKeep()

// Set error reason for failed tickets
lymbo.WithErrorReason("connection timeout")
```

## Storage

Kharon supports pluggable storage backends:

```go
// In-memory store (built-in)
store := store.NewMemoryStore()

// Custom store (implement the Store interface)
type Store interface {
    Add(ctx context.Context, t Ticket) error
    Get(ctx context.Context, id TicketId) (Ticket, error)
    Update(ctx context.Context, id TicketId, fn func(context.Context, *Ticket) error) error
    Delete(ctx context.Context, id TicketId) error
    PollPending(batchSize int, now time.Time, processTime, maxBackoff time.Duration) (PollResult, error)
    ExpireTickets(batchSize int, now time.Time) error
}
```

## Examples

See the [examples/basic](examples/basic/main.go) directory for a complete HTTP API example with:

- Creating tickets via HTTP POST
- Querying ticket status
- Cancelling tickets
- Deleting tickets

## License

MIT
