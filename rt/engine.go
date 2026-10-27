package rt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Engine holds the store and the registry of workflow types.
//
// It runs workflows two ways:
//
//   - Run executes one workflow in the calling goroutine, without a lease. That
//     is stage 1: enough to demonstrate durable replay, and still the simplest
//     way to drive a run from a test or a script.
//   - Work (worker.go) runs a pool of workers that claim runs from the store
//     with expiring leases. That is stage 2: many processes, any of which can
//     die at any moment.
//
// Both go through attempt, which is where stage 3's retry decisions live.
type Engine struct {
	store Store

	mu       sync.RWMutex
	registry map[string]registration
}

type runner func(*Context, json.RawMessage) (json.RawMessage, error)

type registration struct {
	run      runner
	policy   RetryPolicy
	deadline time.Duration
}

func New(store Store) *Engine {
	return &Engine{store: store, registry: make(map[string]registration)}
}

// RegisterOption configures a workflow type.
type RegisterOption func(*registration)

// WithRetryPolicy sets the retry policy for every step of this workflow that
// does not set its own.
func WithRetryPolicy(p RetryPolicy) RegisterOption {
	return func(r *registration) { r.policy = p }
}

// WithDeadline bounds the whole workflow, measured from when it was started,
// across every attempt and every retry. Past the deadline the run is parked as
// failed rather than retried, and an attempt in flight sees its context
// cancelled when the deadline passes.
func WithDeadline(d time.Duration) RegisterOption {
	return func(r *registration) { r.deadline = d }
}

// Register binds a workflow function to a type name.
//
// After Register(e, "foo", fn), the engine can run a workflow of type "foo"
// knowing nothing about fn's input or output types. It holds only bytes.
//
// This is the type-erasure boundary. The registry is a map of one concrete
// function type, but callers register functions with all sorts of input and
// output types. The closure below bridges them: it captures I and O at
// registration time, so the map holds a uniform func while each entry still
// knows how to decode its own input and encode its own output.
//
// It is a package-level function, not a method, because Go methods cannot
// declare their own type parameters.
func Register[I, O any](e *Engine, name string, fn func(*Context, I) (O, error), opts ...RegisterOption) {
	reg := registration{policy: DefaultRetryPolicy}
	for _, o := range opts {
		o(&reg)
	}
	reg.run = func(c *Context, raw json.RawMessage) (json.RawMessage, error) {
		var in I
		if err := json.Unmarshal(raw, &in); err != nil {
			// The stored input does not match the registered type: a
			// deployment mistake, not something a retry fixes.
			return nil, Permanent(fmt.Errorf("decoding input for workflow %q (stored input does not match the registered type): %w", name, err))
		}
		out, err := fn(c, in)
		if err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(out)
		if err != nil {
			return nil, Permanent(fmt.Errorf("encoding output of workflow %q: %w", name, err))
		}
		return encoded, nil
	}

	e.mu.Lock()
	e.registry[name] = reg
	e.mu.Unlock()
}

func (e *Engine) lookup(workflowType string) (registration, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	r, ok := e.registry[workflowType]
	return r, ok
}

// Start records a new run and returns its id. Nothing executes until Run, a
// worker, or RunPending picks it up.
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

// Result reports how one attempt ended.
type Result struct {
	Status   string          // completed | failed | pending | running (owned elsewhere)
	Output   json.RawMessage // set when completed
	Executed int             // steps that ran on this attempt
	Replayed int             // steps served from the journal
}

func terminal(status string) bool {
	return status == StatusCompleted || status == StatusFailed
}

// Run executes, or resumes, one workflow in the calling goroutine.
//
//   - A run already in a terminal state is a no-op: report its status.
//   - An unknown id surfaces as ErrWorkflowNotFound; an unregistered type as a
//     plain error naming the type, with the run left untouched.
//   - Otherwise Run acquires the run (bumping its epoch, so any worker still
//     holding an old lease is fenced out), loads the journal and executes.
//     Success completes it; a permanent error parks it as failed; anything else
//     sends it back to pending under the retry policy.
//
// Run ignores the retry delay: calling it means "now". A retryable failure
// is returned as an error alongside a pending Result.
func (e *Engine) Run(ctx context.Context, workflowID string) (Result, error) {
	row, err := e.store.LoadWorkflow(ctx, workflowID)
	if err != nil {
		return Result{}, fmt.Errorf("run %s: %w", workflowID, err)
	}
	if terminal(row.Status) {
		return Result{Status: row.Status, Output: row.Output}, nil
	}
	reg, ok := e.lookup(row.Type)
	if !ok {
		return Result{Status: row.Status}, fmt.Errorf("run %s: no workflow registered for type %q", workflowID, row.Type)
	}

	lease, err := e.store.Acquire(ctx, workflowID)
	if errors.Is(err, ErrLeaseLost) {
		// It reached a terminal state between the load and the acquire.
		row, err = e.store.LoadWorkflow(ctx, workflowID)
		if err != nil {
			return Result{}, fmt.Errorf("run %s: %w", workflowID, err)
		}
		return Result{Status: row.Status, Output: row.Output}, nil
	}
	if err != nil {
		return Result{}, fmt.Errorf("run %s: acquiring: %w", workflowID, err)
	}
	return e.attempt(ctx, row, reg, lease)
}

// errShutdown is the cancellation cause a worker uses when it stops. An attempt
// cut short by it is handed back without counting as a failure.
var errShutdown = errors.New("worker shutting down")

// attempt runs one attempt of a run the caller already owns, and records how it
// ended. This is the single place that decides between completed, failed and
// pending.
func (e *Engine) attempt(ctx context.Context, row WorkflowRow, reg registration, lease Lease) (Result, error) {
	id := lease.WorkflowID

	// Writes that record the outcome must land even if ctx was just cancelled
	// (shutdown, deadline). Fencing, not cancellation, is what stops a stale
	// runner from writing.
	wctx, wcancel := context.WithTimeout(context.WithoutCancel(ctx), commitTimeout)
	defer wcancel()

	runCtx := ctx
	var deadline time.Time
	if reg.deadline > 0 && !row.CreatedAt.IsZero() {
		deadline = row.CreatedAt.Add(reg.deadline)
		if !time.Now().Before(deadline) {
			msg := fmt.Sprintf("workflow deadline of %s exceeded before this attempt", reg.deadline)
			if err := e.store.Fail(wctx, lease, msg); err != nil {
				return Result{Status: StatusRunning}, fmt.Errorf("run %s: %w", id, err)
			}
			return Result{Status: StatusFailed}, fmt.Errorf("run %s: %s", id, msg)
		}
		var cancel context.CancelFunc
		runCtx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}

	journal, err := e.store.LoadJournal(runCtx, id)
	if err != nil {
		// Could not even start. Hand it back untouched for someone to retry.
		_ = e.store.Release(wctx, lease, "loading journal: "+err.Error(), time.Second, row.Attempts)
		return Result{Status: StatusPending}, fmt.Errorf("run %s: loading journal: %w", id, err)
	}

	c := &Context{
		ctx:        runCtx,
		store:      e.store,
		workflowID: id,
		lease:      lease,
		journal:    journal,
	}
	output, runErr := reg.run(c, row.Input)
	res := Result{Executed: c.Executed, Replayed: c.Replayed}

	switch {
	case runErr == nil:
		if err := e.store.Complete(wctx, lease, output); err != nil {
			res.Status = StatusRunning
			return res, fmt.Errorf("run %s: recording completion: %w", id, err)
		}
		res.Status, res.Output = StatusCompleted, output
		return res, nil

	case errors.Is(runErr, ErrLeaseLost) || errors.Is(context.Cause(ctx), ErrLeaseLost):
		// Someone else owns the run now. Write nothing.
		res.Status = StatusRunning
		return res, fmt.Errorf("run %s: %w", id, ErrLeaseLost)

	case errors.Is(context.Cause(ctx), errShutdown):
		// Not the run's fault: give it straight back, attempt count unchanged.
		if err := e.store.Release(wctx, lease, "interrupted by worker shutdown", 0, row.Attempts); err != nil {
			res.Status = StatusRunning
			return res, fmt.Errorf("run %s: releasing on shutdown: %w", id, err)
		}
		res.Status = StatusPending
		return res, fmt.Errorf("run %s: %w", id, errShutdown)

	case !deadline.IsZero() && !time.Now().Before(deadline):
		return e.fail(wctx, lease, res, fmt.Errorf("workflow deadline of %s exceeded: %w", reg.deadline, runErr))

	case IsPermanent(runErr):
		return e.fail(wctx, lease, res, runErr)
	}

	// Retryable. Count consecutive failures without progress: an attempt that
	// committed at least one step starts the count again, so a long workflow
	// that keeps moving forward is not penalised for earlier hiccups.
	failures := row.Attempts + 1
	if c.Executed > 0 {
		failures = 1
	}
	policy := reg.policy
	if c.failedPolicy != nil {
		policy = *c.failedPolicy
	}
	if policy.exhausted(failures) {
		return e.fail(wctx, lease, res, fmt.Errorf("gave up after %d attempts: %w", failures, runErr))
	}

	delay := policy.Backoff(failures)
	if hint, ok := retryAfterHint(runErr); ok && hint > delay {
		delay = hint
	}
	if err := e.store.Release(wctx, lease, runErr.Error(), delay, failures); err != nil {
		res.Status = StatusRunning
		return res, fmt.Errorf("run %s: releasing for retry: %w", id, err)
	}
	res.Status = StatusPending
	return res, fmt.Errorf("run %s: attempt %d failed, retrying in %s: %w", id, failures, delay.Round(time.Millisecond), runErr)
}

func (e *Engine) fail(ctx context.Context, lease Lease, res Result, cause error) (Result, error) {
	if err := e.store.Fail(ctx, lease, cause.Error()); err != nil {
		res.Status = StatusRunning
		return res, fmt.Errorf("run %s: recording failure: %w", lease.WorkflowID, err)
	}
	res.Status = StatusFailed
	return res, fmt.Errorf("run %s: failed: %w", lease.WorkflowID, cause)
}

// RunPending executes every runnable workflow in the calling goroutine, oldest
// first, and returns the ids that reached a terminal state.
//
// "Runnable" includes runs left 'running' with no lease, or with an expired
// one: that is what a SIGKILLed process leaves behind. A run that fails an
// attempt and goes back to pending is not finished and is not returned. Errors
// from individual attempts are normal outcomes and are not returned either;
// only infrastructure errors (the store unreachable) are.
func (e *Engine) RunPending(ctx context.Context) ([]string, error) {
	ids, err := e.store.Runnable(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing runnable workflows: %w", err)
	}
	var done []string
	var errs []error
	for _, id := range ids {
		res, err := e.Run(ctx, id)
		if err != nil && res.Status == "" {
			errs = append(errs, err)
			continue
		}
		if terminal(res.Status) {
			done = append(done, id)
		}
	}
	return done, errors.Join(errs...)
}

// RetryFailed moves a failed run back to pending with a fresh attempt budget.
// Its journal is untouched, so it resumes from the step that failed rather than
// from the start.
func (e *Engine) RetryFailed(ctx context.Context, workflowID string) error {
	if err := e.store.Retry(ctx, workflowID); err != nil {
		return fmt.Errorf("retrying %s: %w", workflowID, err)
	}
	return nil
}
