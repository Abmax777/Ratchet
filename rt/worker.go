package rt

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

// WorkerOptions configures Engine.Work (stage 2).
type WorkerOptions struct {
	// ID names this worker in the database (workflows.locked_by). Default: a
	// random id.
	ID string
	// Concurrency is how many runs this worker executes at once. Default 1.
	Concurrency int
	// LeaseTTL is how long a claim lasts without a heartbeat. The worker
	// heartbeats every TTL/3, so a run is only reclaimed after roughly three
	// missed heartbeats. Shorter means faster recovery from a dead worker and
	// more database writes. Default 30s.
	LeaseTTL time.Duration
	// PollInterval is the idle wait between empty claims, jittered by ±50% so
	// a fleet of idle workers does not poll in lockstep. Default 500ms.
	PollInterval time.Duration
	// ShutdownGrace is how long in-flight runs get to finish after shutdown
	// begins, before they are cancelled and handed back. Default 10s.
	ShutdownGrace time.Duration
	// OnResult, if set, is called after every attempt. For logs and metrics.
	OnResult func(workflowID string, res Result, err error)
	// OnError, if set, is called when claiming fails (database down, say).
	OnError func(err error)
}

func (o *WorkerOptions) defaults() {
	if o.ID == "" {
		o.ID = "worker-" + newID()[:8]
	}
	if o.Concurrency < 1 {
		o.Concurrency = 1
	}
	if o.LeaseTTL <= 0 {
		o.LeaseTTL = 30 * time.Second
	}
	if o.PollInterval <= 0 {
		o.PollInterval = 500 * time.Millisecond
	}
	if o.ShutdownGrace <= 0 {
		o.ShutdownGrace = 10 * time.Second
	}
}

// Work claims and executes runs until ctx is cancelled, then shuts down
// gracefully: it stops claiming at once, gives in-flight runs ShutdownGrace to
// finish, then cancels what is left. A cancelled run is released back to the
// queue immediately, without counting as a failed attempt.
//
// Any number of Work loops, in any number of processes, can share one store.
// The store's Claim guarantees no two of them get the same run, the lease plus
// heartbeat lets a dead worker's runs be reclaimed, and the epoch fences out a
// worker that stalls past its lease and wakes up later.
func (e *Engine) Work(ctx context.Context, opts WorkerOptions) error {
	opts.defaults()

	// In-flight runs execute under runCtx, which outlives ctx: shutdown stops
	// new claims first and only cancels running work after the grace period.
	runCtx, cancelRuns := context.WithCancelCause(context.WithoutCancel(ctx))
	defer cancelRuns(nil)

	var wg sync.WaitGroup
	for i := 0; i < opts.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.workLoop(ctx, runCtx, opts)
		}()
	}

	<-ctx.Done()
	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(opts.ShutdownGrace):
		cancelRuns(errShutdown)
		<-finished
	}
	return nil
}

func (e *Engine) workLoop(claimCtx, runCtx context.Context, opts WorkerOptions) {
	for claimCtx.Err() == nil {
		lease, ok, err := e.store.Claim(claimCtx, opts.ID, opts.LeaseTTL)
		if err != nil && claimCtx.Err() == nil && opts.OnError != nil {
			opts.OnError(fmt.Errorf("worker %s: claiming: %w", opts.ID, err))
		}
		if err != nil || !ok {
			sleepJittered(claimCtx, opts.PollInterval)
			continue
		}
		res, err := e.process(runCtx, lease, opts.LeaseTTL)
		if opts.OnResult != nil {
			opts.OnResult(lease.WorkflowID, res, err)
		}
	}
}

func sleepJittered(ctx context.Context, d time.Duration) {
	j := d/2 + time.Duration(rand.Int64N(int64(d)+1))
	t := time.NewTimer(j)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// process runs one claimed run with a heartbeat keeping its lease alive. If a
// heartbeat finds the lease gone, the run's context is cancelled with cause
// ErrLeaseLost so its steps stop, and nothing more is written.
func (e *Engine) process(base context.Context, lease Lease, ttl time.Duration) (Result, error) {
	ctx, cancel := context.WithCancelCause(base)
	defer cancel(nil)

	stop := make(chan struct{})
	beating := make(chan struct{})
	go func() {
		defer close(beating)
		t := time.NewTicker(ttl / 3)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				hctx, hcancel := context.WithTimeout(context.WithoutCancel(ctx), ttl/3)
				err := e.store.Heartbeat(hctx, lease, ttl)
				hcancel()
				if errors.Is(err, ErrLeaseLost) {
					cancel(ErrLeaseLost)
					return
				}
				// Any other error: keep trying. If it persists, the lease
				// expires and another worker takes over, which is the point.
			}
		}
	}()
	defer func() {
		close(stop)
		<-beating
	}()

	return e.runClaimed(ctx, lease)
}

// runClaimed loads a claimed run and attempts it, turning a panic in workflow
// code into a retryable failure instead of a dead worker.
func (e *Engine) runClaimed(ctx context.Context, lease Lease) (res Result, err error) {
	wctx, wcancel := context.WithTimeout(context.WithoutCancel(ctx), commitTimeout)
	defer wcancel()

	row, err := e.store.LoadWorkflow(ctx, lease.WorkflowID)
	if err != nil {
		_ = e.store.Release(wctx, lease, "loading workflow: "+err.Error(), time.Second, 0)
		return Result{}, fmt.Errorf("run %s: %w", lease.WorkflowID, err)
	}

	reg, ok := e.lookup(row.Type)
	if !ok {
		// Possibly a newer deployment knows this type. Back off and hand it
		// back rather than failing it here.
		failures := row.Attempts + 1
		delay := DefaultRetryPolicy.Backoff(failures)
		msg := fmt.Sprintf("no workflow registered for type %q on worker %s", row.Type, lease.Owner)
		_ = e.store.Release(wctx, lease, msg, delay, failures)
		return Result{Status: StatusPending}, fmt.Errorf("run %s: %s", lease.WorkflowID, msg)
	}

	defer func() {
		if p := recover(); p != nil {
			perr := fmt.Errorf("panic in workflow %q: %v", row.Type, p)
			failures := row.Attempts + 1
			if reg.policy.exhausted(failures) {
				_ = e.store.Fail(wctx, lease, perr.Error())
				res, err = Result{Status: StatusFailed}, perr
				return
			}
			_ = e.store.Release(wctx, lease, perr.Error(), reg.policy.Backoff(failures), failures)
			res, err = Result{Status: StatusPending}, perr
		}
	}()

	return e.attempt(ctx, row, reg, lease)
}
