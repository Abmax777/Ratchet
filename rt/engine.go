package rt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// Engine holds the store and the registry of workflow types.
//
// Stage 1 has no queue, no workers and no leases: Run executes a workflow in
// the calling goroutine. That is enough to demonstrate durable replay, and it
// leaves an obvious seam for stage 2 (SELECT ... FOR UPDATE SKIP LOCKED,
// leases, heartbeats, fencing tokens).
type Engine struct {
	store Store

	mu       sync.RWMutex
	registry map[string]runner
}

type runner func(*Context, json.RawMessage) (json.RawMessage, error)

func New(store Store) *Engine {
	return &Engine{store: store, registry: make(map[string]runner)}
}

// Register binds a workflow function to a type name.
//
// CONTRACT: after Register(e, "foo", fn), the engine can run a workflow of type
// "foo" knowing nothing about fn's input or output types. It holds only bytes.
//
// This is the type-erasure boundary, and it is worth understanding rather than
// copying. The engine stores runners in a map[string]runner — one concrete
// function type — but callers register functions with all sorts of input and
// output types. Something has to bridge those, and that something is the
// closure you write here: it captures I and O at registration time, so the map
// can hold a uniform func while each entry still knows how to decode its own
// input and encode its own output.
//
// Note also why this is a package-level function and not a method on Engine:
// Go does not let methods declare their own type parameters. Try writing it as
// `func (e *Engine) Register[I, O any](...)` once, read the compiler error, and
// you will remember the rule.
//
// Errors from decoding a caller's input should say so plainly — that failure
// means the stored input does not match the registered type, which is a
// deployment mistake, not a workflow failure.
//
// See TUTORIAL part 2, and LEARNING.md task 5.
func Register[I, O any](e *Engine, name string, fn func(*Context, I) (O, error)) {
	panic("TODO(task 5): implement Register — see LEARNING.md")
}

// Start records a new run and returns its id. Nothing executes until Run or
// RunPending picks it up.
//
// Left implemented as the worked example of talking to the Store.
func (e *Engine) Start(ctx context.Context, workflowType string, input any) (string, error) {
	encoded, err := json.Marshal(input)
	if err != nil {
		return "", fmt.Errorf("encoding workflow input: %w", err)
	}
	id := newID()
	if err := e.store.CreateWorkflow(ctx, id, workflowType, encoded); err != nil {
		return "", fmt.Errorf("starting workflow: %w", err)
	}
	return id, nil
}

// Result reports how a run ended.
type Result struct {
	Status   string          // completed | failed | pending
	Output   json.RawMessage // set when completed
	Executed int             // steps that ran on this attempt
	Replayed int             // steps served from the journal
}

// Run executes, or resumes, one workflow to completion.
//
// CONTRACT
//
//   - A workflow that has already reached a terminal state is a no-op: report
//     its status, run nothing.
//   - An unknown id must surface as ErrWorkflowNotFound (errors.Is-able), and
//     an unregistered workflow type as a plain error naming the type.
//   - Before invoking the workflow function, Run loads the journal for this id
//     and builds the rt.Context the steps will use.
//   - Three outcomes, three different persisted states:
//     success   -> completed, with the encoded output stored.
//     nondeterminism (errors.Is ErrNondeterministic) -> failed. TERMINAL:
//     retrying cannot help, a human has to look.
//     any other error -> back to pending, journal untouched, so a later Run
//     resumes from the last committed step. Record the error either way.
//   - The returned Result carries the status and the Executed / Replayed
//     counters from the Context, whichever way the run ended.
//
// Deciding what the persisted status should be at each moment is most of the
// work here. Write the state transitions down before you write the code.
//
// See TUTORIAL part 5, and LEARNING.md task 6.
func (e *Engine) Run(ctx context.Context, workflowID string) (Result, error) {
	panic("TODO(task 6): implement Run — see LEARNING.md")
}

// RunPending executes every unfinished workflow, oldest first, and returns the
// ids that reached a terminal state.
//
// CONTRACT
//
//   - Ask the store which workflows are runnable, run each one, and collect
//     the ids that finished (completed or failed).
//   - A workflow that went back to pending is not finished, and does not
//     belong in the returned slice.
//
// The subtlety is in Store.Runnable rather than here: a SIGKILLed process
// leaves its workflow marked 'running' with nobody executing it, so "runnable"
// cannot mean "pending" alone or a killed run is stranded forever. Telling
// "running somewhere" apart from "abandoned" properly needs leases — that is
// stage 2. Until then, one runner at a time.
//
// See LEARNING.md task 7.
func (e *Engine) RunPending(ctx context.Context) ([]string, error) {
	panic("TODO(task 7): implement RunPending — see LEARNING.md")
}

// Keep the compiler quiet about imports the contracts above refer to but the
// panicking stubs do not yet use. Delete this line once Run and Register are
// written.
var (
	_ = errors.Is
	_ = fmt.Errorf
	_ = json.Marshal
)
