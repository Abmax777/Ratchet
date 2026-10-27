package rt

import (
	"context"
	"errors"
	"time"
)

// ErrWorkflowNotFound is returned by a Store when the id does not exist.
var ErrWorkflowNotFound = errors.New("workflow not found")

// ErrStepAlreadyCommitted means a step was committed twice for the same
// (workflowID, seq). With leases and fencing this should not happen; if it
// does, two runners executed the same step at once, and that must be loud
// rather than silently overwritten.
var ErrStepAlreadyCommitted = errors.New("step already committed")

// ErrLeaseLost is returned by every fenced write when the caller no longer owns
// the run: its lease expired and another worker claimed it (bumping the epoch),
// or the run already reached a terminal state. The caller must stop. Anything it
// writes from here on would be a zombie write, and the store refuses it.
var ErrLeaseLost = errors.New("lease lost: run is owned by someone else")

// Workflow statuses. The database CHECK constraint allows exactly these four.
const (
	StatusPending   = "pending"
	StatusRunning   = "running"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
)

// WorkflowRow is the persisted state of one run.
type WorkflowRow struct {
	Type      string
	Status    string
	Input     []byte
	Output    []byte
	LastError string

	// Epoch is the fencing token. Every claim of the run increments it, and
	// every write a runner makes must carry the epoch it claimed with.
	Epoch int64
	// Attempts counts consecutive failed attempts that made no progress. It
	// drives the retry policy, and resets once an attempt commits a step.
	Attempts int

	CreatedAt time.Time
	UpdatedAt time.Time
}

// JournalEntry is one committed step.
type JournalEntry struct {
	Name   string
	Result []byte
}

// Lease is proof of ownership of one run. It is what a runner holds while it
// executes, and what every fenced write checks.
type Lease struct {
	WorkflowID string
	Epoch      int64
	// Owner is the worker that claimed the run. Empty for a direct Engine.Run,
	// which takes the run without an expiring lease (stage 1 behaviour).
	Owner string
}

// Store is everything the runtime needs from a database.
//
// Two guarantees carry the whole design:
//
//  1. CommitStep for a given (workflowID, seq) succeeds at most once, ever.
//     In Postgres that is the primary key on steps.
//  2. Every write that changes a run's progress is fenced: it carries the
//     Lease the writer claimed with, and is refused with ErrLeaseLost if the
//     run's epoch has moved on. A worker that stalls past its lease can wake
//     up, but it cannot corrupt a run another worker now owns.
type Store interface {
	CreateWorkflow(ctx context.Context, id, workflowType string, input []byte) error
	LoadWorkflow(ctx context.Context, id string) (WorkflowRow, error)
	LoadJournal(ctx context.Context, id string) (map[int]JournalEntry, error)

	// Acquire takes a run for a direct, single-process Engine.Run: it marks it
	// running and bumps the epoch, with no expiring lease. It ignores
	// next_attempt_at (an explicit Run means "now"). Returns ErrLeaseLost if the
	// run is already terminal.
	Acquire(ctx context.Context, id string) (Lease, error)

	// Claim atomically picks the oldest runnable run, marks it running, gives
	// it a lease of ttl owned by owner, and bumps its epoch. ok is false when
	// nothing is runnable. Concurrent claimers never get the same run.
	Claim(ctx context.Context, owner string, ttl time.Duration) (l Lease, ok bool, err error)

	// Heartbeat extends a held lease by ttl from now.
	Heartbeat(ctx context.Context, l Lease, ttl time.Duration) error

	// CommitStep journals one step. Fenced.
	CommitStep(ctx context.Context, l Lease, seq int, name string, result []byte) error

	// Complete marks the run completed with its output. Fenced.
	Complete(ctx context.Context, l Lease, output []byte) error
	// Fail parks the run as failed: terminal until someone calls Retry. Fenced.
	Fail(ctx context.Context, l Lease, lastErr string) error
	// Release hands the run back to the queue as pending, runnable again after
	// delay, with the given attempt count. Fenced.
	Release(ctx context.Context, l Lease, lastErr string, delay time.Duration, attempts int) error

	// Retry moves a failed run back to pending with a fresh attempt budget. Its
	// journal is untouched, so it resumes from the step that failed.
	Retry(ctx context.Context, id string) error

	// Runnable lists runs a worker could claim right now, oldest first: pending
	// runs whose retry delay has passed, and running runs whose lease has
	// expired or which never had one (a direct Run that died with its process).
	Runnable(ctx context.Context) ([]string, error)
}
