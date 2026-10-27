package rt

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"time"
)

// MemStore is an in-memory Store.
//
// It exists so workflow logic can be unit tested without a database, and it is
// the store the runtime's own tests run against. It implements the same
// contract as pgstore, leases and fencing included, so the tests do not care
// which store they run against. It is not durable: use pgstore for anything
// that has to survive the process.
//
// Its clock is replaceable (SetClock), which is how the lease tests expire a
// lease without sleeping.
type MemStore struct {
	mu    sync.Mutex
	runs  map[string]*memRun
	order []string // ids in creation order: Go map iteration order is random
	now   func() time.Time
}

type memRun struct {
	row         WorkflowRow
	journal     map[int]JournalEntry
	lockedBy    string
	lockedUntil time.Time // zero: no lease
	nextAttempt time.Time
}

func NewMemStore() *MemStore {
	return &MemStore{runs: make(map[string]*memRun), now: time.Now}
}

// SetClock replaces the store's notion of now. Test helper.
func (m *MemStore) SetClock(now func() time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.now = now
}

var _ Store = (*MemStore)(nil)

func (m *MemStore) CreateWorkflow(ctx context.Context, id, workflowType string, input []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.runs[id]; exists {
		return fmt.Errorf("workflow %s already exists", id) // uuids make this effectively impossible
	}
	now := m.now()
	m.runs[id] = &memRun{
		row: WorkflowRow{
			Type:      workflowType,
			Status:    StatusPending,
			Input:     append([]byte(nil), input...),
			CreatedAt: now,
			UpdatedAt: now,
		},
		journal:     make(map[int]JournalEntry),
		nextAttempt: now,
	}
	m.order = append(m.order, id)
	return nil
}

func (m *MemStore) LoadWorkflow(ctx context.Context, id string) (WorkflowRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.runs[id]
	if !ok {
		return WorkflowRow{}, ErrWorkflowNotFound
	}
	row := r.row
	row.Input = append([]byte(nil), r.row.Input...)
	row.Output = append([]byte(nil), r.row.Output...)
	return row, nil
}

func (m *MemStore) LoadJournal(ctx context.Context, id string) (map[int]JournalEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.runs[id]
	if !ok {
		return nil, ErrWorkflowNotFound
	}
	return maps.Clone(r.journal), nil
}

func (m *MemStore) Acquire(ctx context.Context, id string) (Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.runs[id]
	if !ok {
		return Lease{}, ErrWorkflowNotFound
	}
	if terminal(r.row.Status) {
		return Lease{}, ErrLeaseLost
	}
	r.row.Epoch++
	r.row.Status = StatusRunning
	r.row.UpdatedAt = m.now()
	r.lockedBy, r.lockedUntil = "", time.Time{}
	return Lease{WorkflowID: id, Epoch: r.row.Epoch}, nil
}

// claimable is the single definition of "a worker may take this run now".
// Caller holds m.mu.
func (m *MemStore) claimable(r *memRun, now time.Time) bool {
	switch r.row.Status {
	case StatusPending:
		return !now.Before(r.nextAttempt)
	case StatusRunning:
		return r.lockedUntil.IsZero() || now.After(r.lockedUntil)
	}
	return false
}

func (m *MemStore) Claim(ctx context.Context, owner string, ttl time.Duration) (Lease, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	for _, id := range m.order {
		r := m.runs[id]
		if !m.claimable(r, now) {
			continue
		}
		r.row.Epoch++
		r.row.Status = StatusRunning
		r.row.UpdatedAt = now
		r.lockedBy, r.lockedUntil = owner, now.Add(ttl)
		return Lease{WorkflowID: id, Epoch: r.row.Epoch, Owner: owner}, true, nil
	}
	return Lease{}, false, nil
}

// owned returns the run if l still owns it. Caller holds m.mu.
func (m *MemStore) owned(l Lease) (*memRun, error) {
	r, ok := m.runs[l.WorkflowID]
	if !ok {
		return nil, ErrWorkflowNotFound
	}
	if r.row.Epoch != l.Epoch || r.row.Status != StatusRunning {
		return nil, ErrLeaseLost
	}
	return r, nil
}

func (m *MemStore) Heartbeat(ctx context.Context, l Lease, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, err := m.owned(l)
	if err != nil {
		return err
	}
	r.lockedUntil = m.now().Add(ttl)
	return nil
}

func (m *MemStore) CommitStep(ctx context.Context, l Lease, seq int, name string, result []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, err := m.owned(l)
	if err != nil {
		return err
	}
	if _, exists := r.journal[seq]; exists {
		return ErrStepAlreadyCommitted
	}
	r.journal[seq] = JournalEntry{Name: name, Result: append([]byte(nil), result...)}
	return nil
}

func (m *MemStore) Complete(ctx context.Context, l Lease, output []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, err := m.owned(l)
	if err != nil {
		return err
	}
	r.row.Status = StatusCompleted
	r.row.Output = append([]byte(nil), output...)
	r.row.UpdatedAt = m.now()
	r.lockedBy, r.lockedUntil = "", time.Time{}
	return nil
}

func (m *MemStore) Fail(ctx context.Context, l Lease, lastErr string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, err := m.owned(l)
	if err != nil {
		return err
	}
	r.row.Status = StatusFailed
	r.row.LastError = lastErr
	r.row.UpdatedAt = m.now()
	r.lockedBy, r.lockedUntil = "", time.Time{}
	return nil
}

func (m *MemStore) Release(ctx context.Context, l Lease, lastErr string, delay time.Duration, attempts int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, err := m.owned(l)
	if err != nil {
		return err
	}
	now := m.now()
	r.row.Status = StatusPending
	r.row.LastError = lastErr
	r.row.Attempts = attempts
	r.row.UpdatedAt = now
	r.nextAttempt = now.Add(delay)
	r.lockedBy, r.lockedUntil = "", time.Time{}
	return nil
}

func (m *MemStore) Retry(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.runs[id]
	if !ok {
		return ErrWorkflowNotFound
	}
	if r.row.Status != StatusFailed {
		return nil // only failed runs are retried; anything else is a no-op
	}
	now := m.now()
	r.row.Status = StatusPending
	r.row.Attempts = 0
	r.row.UpdatedAt = now
	r.nextAttempt = now
	return nil
}

func (m *MemStore) Runnable(ctx context.Context) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	var ids []string
	for _, id := range m.order {
		if m.claimable(m.runs[id], now) {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

// Output returns a finished workflow's encoded output. Test helper.
func (m *MemStore) Output(id string) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := m.runs[id]; ok {
		return append([]byte(nil), r.row.Output...)
	}
	return nil
}
