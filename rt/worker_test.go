package rt_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/abmax777/ratchet/rt"
)

// waitFor polls until cond holds or the deadline passes.
func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

func status(t *testing.T, s rt.Store, id string) rt.WorkflowRow {
	t.Helper()
	row, err := s.LoadWorkflow(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

// A worker pool drains the queue, and nothing runs twice.
func TestWorkersDrainTheQueue(t *testing.T) {
	store := rt.NewMemStore()
	e := rt.New(store)
	var calls atomic.Int64
	rt.Register(e, "two_steps", func(c *rt.Context, i in) (out, error) {
		a, err := rt.Step(c, "a", func() (int, error) { calls.Add(1); return i.N, nil })
		if err != nil {
			return out{}, err
		}
		b, err := rt.Step(c, "b", func() (int, error) { calls.Add(1); return a * 2, nil })
		return out{Sum: b}, err
	})

	ctx := context.Background()
	var ids []string
	for i := 0; i < 50; i++ {
		id, _ := e.Start(ctx, "two_steps", in{N: i})
		ids = append(ids, id)
	}

	wctx, stop := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for _, name := range []string{"w1", "w2", "w3"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = e.Work(wctx, rt.WorkerOptions{ID: name, Concurrency: 4, LeaseTTL: time.Second, PollInterval: 5 * time.Millisecond})
		}()
	}
	waitFor(t, 5*time.Second, func() bool {
		for _, id := range ids {
			if status(t, store, id).Status != rt.StatusCompleted {
				return false
			}
		}
		return true
	})
	stop()
	wg.Wait()

	if got := calls.Load(); got != 100 {
		t.Fatalf("%d step executions for 50 two-step runs, want 100", got)
	}
}

// A worker dies holding a run: it committed step one and then stopped
// heartbeating. Once the lease expires, another worker takes over, replays step
// one, and finishes. The dead worker's late write is refused.
func TestDeadWorkersRunIsTakenOver(t *testing.T) {
	store := rt.NewMemStore()
	e := rt.New(store)
	var oneCalls, twoCalls atomic.Int64
	rt.Register(e, "flow", func(c *rt.Context, i in) (out, error) {
		if _, err := rt.Step(c, "one", func() (int, error) { oneCalls.Add(1); return 1, nil }); err != nil {
			return out{}, err
		}
		v, err := rt.Step(c, "two", func() (int, error) { twoCalls.Add(1); return 2, nil })
		return out{Sum: v}, err
	})

	ctx := context.Background()
	id, _ := e.Start(ctx, "flow", in{})

	// The doomed worker: claims with a short lease, does step one by hand,
	// then goes silent.
	dead, ok, err := store.Claim(ctx, "dead", 100*time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}
	if err := store.CommitStep(ctx, dead, 1, "one", []byte("1")); err != nil {
		t.Fatal(err)
	}
	oneCalls.Add(1)

	wctx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = e.Work(wctx, rt.WorkerOptions{ID: "alive", LeaseTTL: time.Second, PollInterval: 10 * time.Millisecond})
	}()
	waitFor(t, 3*time.Second, func() bool { return status(t, store, id).Status == rt.StatusCompleted })
	stop()
	<-done

	if oneCalls.Load() != 1 || twoCalls.Load() != 1 {
		t.Fatalf("step one ran %d times, step two %d times; want 1 and 1", oneCalls.Load(), twoCalls.Load())
	}
	if err := store.CommitStep(ctx, dead, 2, "two", []byte("99")); !errors.Is(err, rt.ErrLeaseLost) {
		t.Fatalf("the dead worker's late commit: got %v, want ErrLeaseLost", err)
	}
}

// Shutdown stops claiming, gives in-flight work its grace period, then cancels
// it and hands the run back without charging it an attempt.
func TestGracefulShutdownReleasesInFlightRun(t *testing.T) {
	store := rt.NewMemStore()
	e := rt.New(store)
	started := make(chan struct{})
	rt.Register(e, "slow", func(c *rt.Context, i in) (out, error) {
		_, err := rt.Step(c, "wait", func() (int, error) {
			close(started)
			<-c.Ctx().Done() // a well-behaved step honours cancellation
			return 0, c.Ctx().Err()
		})
		return out{}, err
	})
	id, _ := e.Start(context.Background(), "slow", in{})

	wctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = e.Work(wctx, rt.WorkerOptions{ID: "w", LeaseTTL: time.Second, ShutdownGrace: 50 * time.Millisecond, PollInterval: 10 * time.Millisecond})
	}()
	<-started
	stop()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Work did not return after shutdown")
	}

	row := status(t, store, id)
	if row.Status != rt.StatusPending || row.Attempts != 0 {
		t.Fatalf("after shutdown: status %q attempts %d, want pending with 0 attempts", row.Status, row.Attempts)
	}
}

// A panic in workflow code costs that run an attempt, not the worker its life.
func TestPanicIsRetriedNotFatal(t *testing.T) {
	store := rt.NewMemStore()
	e := rt.New(store)
	var tries atomic.Int64
	rt.Register(e, "panicky", func(c *rt.Context, i in) (out, error) {
		if tries.Add(1) == 1 {
			panic("first attempt blows up")
		}
		return out{Sum: 7}, nil
	}, rt.WithRetryPolicy(rt.RetryPolicy{MaxAttempts: 5, Initial: time.Millisecond, Multiplier: 1}))
	id, _ := e.Start(context.Background(), "panicky", in{})

	wctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = e.Work(wctx, rt.WorkerOptions{ID: "w", LeaseTTL: time.Second, PollInterval: 5 * time.Millisecond})
	}()
	waitFor(t, 3*time.Second, func() bool { return status(t, store, id).Status == rt.StatusCompleted })
	stop()
	<-done
}
