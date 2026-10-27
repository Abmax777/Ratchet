package main

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/abmax777/ratchet/rt"
)

var errDead = errors.New("process is dead")

// killable wraps one simulated worker's view of the store, so the harness can
// do to it what the real world does to processes:
//
//   - kill: from this moment the "process" can reach nothing. Every store call
//     fails, so it never releases its claims. Its leases just stop being
//     renewed, exactly like a SIGKILLed process.
//   - stall: the process freezes for a while (a long GC pause, a suspended
//     VM). Heartbeats stop and its next write blocks until the freeze ends.
//     Meanwhile its lease expires and someone else claims the run, so when the
//     frozen worker wakes up and writes, it is a zombie, and it is fenced.
type killable struct {
	inner       rt.Store
	dead        atomic.Bool
	stallUntil  atomic.Int64 // unix nanos
	fencedCount *atomic.Int64
}

func (k *killable) kill()                 { k.dead.Store(true) }
func (k *killable) stall(d time.Duration) { k.stallUntil.Store(time.Now().Add(d).UnixNano()) }
func (k *killable) stalled() bool         { return time.Now().UnixNano() < k.stallUntil.Load() }

// freeze blocks a write until the stall ends, if one is in progress.
func (k *killable) freeze() {
	if until := k.stallUntil.Load(); time.Now().UnixNano() < until {
		time.Sleep(time.Until(time.Unix(0, until)))
	}
}

func (k *killable) count(err error) error {
	if errors.Is(err, rt.ErrLeaseLost) {
		k.fencedCount.Add(1)
	}
	return err
}

func (k *killable) CreateWorkflow(ctx context.Context, id, typ string, input []byte) error {
	if k.dead.Load() {
		return errDead
	}
	return k.inner.CreateWorkflow(ctx, id, typ, input)
}

func (k *killable) LoadWorkflow(ctx context.Context, id string) (rt.WorkflowRow, error) {
	if k.dead.Load() {
		return rt.WorkflowRow{}, errDead
	}
	return k.inner.LoadWorkflow(ctx, id)
}

func (k *killable) LoadJournal(ctx context.Context, id string) (map[int]rt.JournalEntry, error) {
	if k.dead.Load() {
		return nil, errDead
	}
	return k.inner.LoadJournal(ctx, id)
}

func (k *killable) Acquire(ctx context.Context, id string) (rt.Lease, error) {
	if k.dead.Load() {
		return rt.Lease{}, errDead
	}
	return k.inner.Acquire(ctx, id)
}

func (k *killable) Claim(ctx context.Context, owner string, ttl time.Duration) (rt.Lease, bool, error) {
	if k.dead.Load() {
		return rt.Lease{}, false, errDead
	}
	return k.inner.Claim(ctx, owner, ttl)
}

func (k *killable) Heartbeat(ctx context.Context, l rt.Lease, ttl time.Duration) error {
	if k.dead.Load() {
		return errDead
	}
	if k.stalled() {
		return nil // the worker believes it heartbeated; the lease was not extended
	}
	return k.count(k.inner.Heartbeat(ctx, l, ttl))
}

func (k *killable) CommitStep(ctx context.Context, l rt.Lease, seq int, name string, result []byte) error {
	if k.dead.Load() {
		return errDead
	}
	k.freeze()
	return k.count(k.inner.CommitStep(ctx, l, seq, name, result))
}

func (k *killable) Complete(ctx context.Context, l rt.Lease, output []byte) error {
	if k.dead.Load() {
		return errDead
	}
	k.freeze()
	return k.count(k.inner.Complete(ctx, l, output))
}

func (k *killable) Fail(ctx context.Context, l rt.Lease, lastErr string) error {
	if k.dead.Load() {
		return errDead
	}
	k.freeze()
	return k.count(k.inner.Fail(ctx, l, lastErr))
}

func (k *killable) Release(ctx context.Context, l rt.Lease, lastErr string, delay time.Duration, attempts int) error {
	if k.dead.Load() {
		return errDead
	}
	k.freeze()
	return k.count(k.inner.Release(ctx, l, lastErr, delay, attempts))
}

func (k *killable) Retry(ctx context.Context, id string) error {
	if k.dead.Load() {
		return errDead
	}
	return k.inner.Retry(ctx, id)
}

func (k *killable) Runnable(ctx context.Context) ([]string, error) {
	if k.dead.Load() {
		return nil, errDead
	}
	return k.inner.Runnable(ctx)
}
