package rt

import (
	"context"
	"errors"
)

// ErrWorkflowNotFound is returned by a Store when the id does not exist.
var ErrWorkflowNotFound = errors.New("workflow not found")

// ErrStepAlreadyCommitted means a step was committed twice for the same
// (workflowID, seq). With one runner that cannot happen; once stage 2 adds
// workers it means two of them executed the same workflow concurrently, which
// should be loud rather than silently overwritten.
var ErrStepAlreadyCommitted = errors.New("step already committed")

// WorkflowRow is the persisted state of one run.
type WorkflowRow struct {
	Type   string
	Status string
	Input  []byte
}

// JournalEntry is one committed step.
type JournalEntry struct {
	Name   string
	Result []byte
}

// Store is everything the runtime needs from a database.
//
// Keeping it an interface is not ceremony: it lets workflow logic be unit
// tested with MemStore and no database at all, and it is where stage 2's
// lease-aware claim methods will slot in without touching the replay code.
//
// Implementations must guarantee one thing above all: CommitStep for a given
// (workflowID, seq) succeeds at most once, ever. In Postgres that is the
// primary key doing the work.
type Store interface {
	CreateWorkflow(ctx context.Context, id, workflowType string, input []byte) error
	LoadWorkflow(ctx context.Context, id string) (WorkflowRow, error)
	SetStatus(ctx context.Context, id, status string, output []byte, lastErr string) error

	LoadJournal(ctx context.Context, id string) (map[int]JournalEntry, error)
	CommitStep(ctx context.Context, id string, seq int, name string, result []byte) error

	// Runnable lists workflows that are not finished, oldest first.
	Runnable(ctx context.Context) ([]string, error)
}
