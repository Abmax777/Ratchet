package rt

import (
	"context"
	"sync"
)

// MemStore is an in-memory Store.
//
// It exists so workflow logic can be unit tested without a database — and it is
// the store the runtime's own tests run against, which is why those tests need
// no fixtures and finish in milliseconds. It is not durable: use pgstore for
// anything that has to survive the process.
//
// This is task 2, and it is deliberately the easy one: no SQL, no generics,
// just the Store contract expressed in maps. Getting it right first means every
// later task has a working substrate to test against.
//
// Two things to hold on to while writing it:
//
//   - CommitStep must refuse a second commit for the same (workflowID, seq)
//     rather than overwriting, returning ErrStepAlreadyCommitted. Postgres will
//     enforce this with a primary key; here you enforce it by hand. Same
//     semantics, so the tests do not care which store they run against.
//   - Runnable returns ids oldest-first, and Go map iteration order is random.
//     You will need to remember insertion order yourself.
//
// See TUTORIAL part 2, and LEARNING.md task 2.
type MemStore struct {
	mu sync.Mutex

	// TODO(task 2): what state does an in-memory store of workflows and their
	// journals actually need? WorkflowRow and JournalEntry in store.go are the
	// shapes the interface deals in; you may want a struct that bundles a row
	// with its steps.
}

func NewMemStore() *MemStore {
	panic("TODO(task 2): implement NewMemStore — see LEARNING.md")
}

func (m *MemStore) CreateWorkflow(ctx context.Context, id, workflowType string, input []byte) error {
	panic("TODO(task 2): implement CreateWorkflow")
}

func (m *MemStore) LoadWorkflow(ctx context.Context, id string) (WorkflowRow, error) {
	panic("TODO(task 2): implement LoadWorkflow")
}

func (m *MemStore) SetStatus(ctx context.Context, id, status string, output []byte, lastErr string) error {
	panic("TODO(task 2): implement SetStatus")
}

func (m *MemStore) LoadJournal(ctx context.Context, id string) (map[int]JournalEntry, error) {
	panic("TODO(task 2): implement LoadJournal")
}

func (m *MemStore) CommitStep(ctx context.Context, id string, seq int, name string, result []byte) error {
	panic("TODO(task 2): implement CommitStep")
}

func (m *MemStore) Runnable(ctx context.Context) ([]string, error) {
	panic("TODO(task 2): implement Runnable")
}

// Output returns a finished workflow's encoded output. Test helper.
func (m *MemStore) Output(id string) []byte {
	panic("TODO(task 2): implement Output")
}
