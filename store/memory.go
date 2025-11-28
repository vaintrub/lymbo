package store

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/ochaton/lymbo"
	"github.com/ochaton/lymbo/status"
)

const MaxBackoffDelay = 15 * time.Second

type MemoryStore struct {
	mu   sync.RWMutex
	data map[lymbo.TicketId]lymbo.Ticket
}

var _ lymbo.Store = (*MemoryStore)(nil)

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		data: make(map[lymbo.TicketId]lymbo.Ticket),
	}
}

func (m *MemoryStore) Get(id lymbo.TicketId) (lymbo.Ticket, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ticket, exists := m.data[id]

	if !exists {
		return lymbo.Ticket{}, lymbo.ErrTicketNotFound
	}
	return ticket, nil
}

func (m *MemoryStore) Add(t lymbo.Ticket) error {
	if t.ID == "" {
		return lymbo.ErrTicketIDEmpty
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	t.Status = status.Pending
	m.data[t.ID] = t

	return nil
}

func (m *MemoryStore) Delete(id lymbo.TicketId) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.data, id)
	return nil
}

func (m *MemoryStore) Done(tid lymbo.TicketId, result any) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// check that exists:
	t, exists := m.data[tid]
	if !exists {
		return lymbo.ErrTicketNotFound
	}

	if t.Status != status.Pending {
		return errors.Join(
			fmt.Errorf("invalid status transition from %s to %s", t.Status, status.Done),
			lymbo.ErrInvalidStatusTransition,
		)
	}

	t.Status = status.Done
	tm := time.Now()
	t.Mtime = &tm

	m.data[tid] = t

	return nil
}

func (m *MemoryStore) Cancel(tid lymbo.TicketId, reason any) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// check that exists:
	t, exists := m.data[tid]
	if !exists {
		return lymbo.ErrTicketNotFound
	}

	if t.Status != status.Pending {
		return errors.Join(
			fmt.Errorf("invalid status transition from %s to %s", t.Status, status.Cancelled),
			lymbo.ErrInvalidStatusTransition,
		)
	}

	t.Status = status.Cancelled
	t.ErrorReason = reason
	tm := time.Now()
	t.Mtime = &tm

	m.data[tid] = t

	return nil
}

func (m *MemoryStore) Fail(tid lymbo.TicketId, reason any) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// check that exists:
	t, exists := m.data[tid]
	if !exists {
		return lymbo.ErrTicketNotFound
	}

	if t.Status != status.Pending {
		return errors.Join(
			fmt.Errorf("invalid status transition from %s to %s", t.Status, status.Failed),
			lymbo.ErrInvalidStatusTransition,
		)
	}

	t.Status = status.Failed
	t.ErrorReason = reason
	tm := time.Now()
	t.Mtime = &tm
	m.data[tid] = t

	return nil
}

func (m *MemoryStore) Poll(limit int, now time.Time, blockFor time.Duration) (lymbo.PollResult, error) {
	if limit <= 0 {
		return lymbo.PollResult{
			Tickets:    nil,
			SleepUntil: nil,
		}, lymbo.ErrLimitInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	var closest *time.Time
	var ready []lymbo.Ticket
	for _, t := range m.data {
		if t.Status != status.Pending {
			continue
		}
		if t.Runat.After(now) {
			if closest == nil || t.Runat.Before(*closest) {
				runat := t.Runat
				closest = &runat
			}
			continue
		}
		// we have to fetch all ready tickets first, because they are unordered
		ready = append(ready, t)
	}

	// if no ready tickets, return closest runat (can be nil)
	if len(ready) == 0 {
		return lymbo.PollResult{
			Tickets:    nil,
			SleepUntil: closest,
		}, nil
	}

	// Sort ready tickets by {runat, nice}
	sort.Slice(ready, func(i, j int) bool {
		if ready[i].Runat.Equal(ready[j].Runat) {
			return ready[i].Nice < ready[j].Nice
		}
		return ready[i].Runat.Before(ready[j].Runat)
	})

	// Now take up to limit tickets
	ready = ready[:min(limit, len(ready))]

	// And finally, update their runat to now + blockFor to avoid re-polling them immediately
	for _, t := range ready {
		delay := min(MaxBackoffDelay, time.Duration(math.Pow(1.5, float64(t.Attempts))))
		// TODO: randomize delay a bit
		delay += blockFor
		t.Runat = now.Add(delay)
		t.Attempts += 1
		m.data[t.ID] = t
	}

	return lymbo.PollResult{
		Tickets:    ready,
		SleepUntil: nil,
	}, nil
}
