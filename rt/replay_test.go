package rt_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/abmax777/ratchet/rt"
)

type in struct {
	N int `json:"n"`
}

type out struct {
	Sum int `json:"sum"`
}

// A journaled step must never run its function again, however many times the
// workflow is executed.
func TestCompletedStepsAreNotReexecuted(t *testing.T) {
	ctx := context.Background()
	store := rt.NewMemStore()
	e := rt.New(store)

	var calls int
	rt.Register(e, "counting", func(c *rt.Context, i in) (out, error) {
		a, err := rt.Step(c, "a", func() (int, error) { calls++; return i.N + 1, nil })
		if err != nil {
			return out{}, err
		}
		b, err := rt.Step(c, "b", func() (int, error) { calls++; return a * 2, nil })
		if err != nil {
			return out{}, err
		}
		return out{Sum: b}, nil
	})

	id, err := e.Start(ctx, "counting", in{N: 4})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	res, err := e.Run(ctx, id)
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	if res.Status != "completed" || res.Executed != 2 || res.Replayed != 0 {
		t.Fatalf("first run: got %+v, want completed with 2 executed / 0 replayed", res)
	}
	if calls != 2 {
		t.Fatalf("first run made %d calls, want 2", calls)
	}

	var got out
	if err := json.Unmarshal(res.Output, &got); err != nil {
		t.Fatalf("decoding output: %v", err)
	}
	if got.Sum != 10 {
		t.Fatalf("sum = %d, want 10", got.Sum)
	}

	// Running a finished workflow again does nothing at all.
	if _, err := e.Run(ctx, id); err != nil {
		t.Fatalf("second run: %v", err)
	}
	if calls != 2 {
		t.Fatalf("after re-run: %d calls total, want 2 — steps were re-executed", calls)
	}
}

// The load-bearing case. A workflow dies partway; its committed steps stay in
// the journal; resuming replays them instead of re-running them, and only the
// unfinished work actually executes.
func TestResumeReplaysCommittedStepsOnly(t *testing.T) {
	ctx := context.Background()
	store := rt.NewMemStore()
	e := rt.New(store)

	var aCalls, bCalls int
	upstreamDown := true

	rt.Register(e, "flaky", func(c *rt.Context, i in) (out, error) {
		a, err := rt.Step(c, "a", func() (int, error) { aCalls++; return i.N + 1, nil })
		if err != nil {
			return out{}, err
		}
		b, err := rt.Step(c, "b", func() (int, error) {
			if upstreamDown {
				return 0, errors.New("upstream is down")
			}
			bCalls++
			return a * 10, nil
		})
		if err != nil {
			return out{}, err
		}
		return out{Sum: b}, nil
	})

	id, err := e.Start(ctx, "flaky", in{N: 2})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	res, err := e.Run(ctx, id)
	if err == nil {
		t.Fatal("expected the first attempt to fail")
	}
	if res.Status != "pending" {
		t.Fatalf("failed attempt left status %q, want pending (resumable)", res.Status)
	}
	if aCalls != 1 {
		t.Fatalf("step a ran %d times on the first attempt, want 1", aCalls)
	}

	upstreamDown = false
	res, err = e.Run(ctx, id)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if res.Status != "completed" {
		t.Fatalf("resume left status %q, want completed", res.Status)
	}
	if aCalls != 1 {
		t.Fatalf("step a ran %d times in total, want 1 — it was re-executed on resume", aCalls)
	}
	if bCalls != 1 {
		t.Fatalf("step b ran %d times, want 1", bCalls)
	}
	if res.Replayed != 1 || res.Executed != 1 {
		t.Fatalf("resume: %d replayed / %d executed, want 1 / 1", res.Replayed, res.Executed)
	}
}

// A workflow killed mid-flight leaves status 'running' with nobody executing
// it. RunPending has to pick that up, or a SIGKILL strands the run forever.
func TestRunPendingResumesAbandonedWorkflow(t *testing.T) {
	ctx := context.Background()
	store := rt.NewMemStore()

	// "Process 1": gets one step done, then the machine dies. Panicking out of
	// the workflow models a kill better than returning an error, because it
	// leaves the run marked 'running' exactly as SIGKILL would.
	e1 := rt.New(store)
	var sideEffects []string
	rt.Register(e1, "three_steps", func(c *rt.Context, i in) (out, error) {
		if _, err := rt.Step(c, "one", func() (int, error) {
			sideEffects = append(sideEffects, "one")
			return 1, nil
		}); err != nil {
			return out{}, err
		}
		panic("simulated process death")
	})

	id, err := e1.Start(ctx, "three_steps", in{N: 0})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	func() {
		defer func() { _ = recover() }()
		_, _ = e1.Run(ctx, id)
	}()

	if row, _ := store.LoadWorkflow(ctx, id); row.Status != "running" {
		t.Fatalf("after the kill, status is %q, want running (abandoned)", row.Status)
	}

	// "Process 2": same workflow type, complete implementation, fresh engine.
	e2 := rt.New(store)
	rt.Register(e2, "three_steps", func(c *rt.Context, i in) (out, error) {
		a, err := rt.Step(c, "one", func() (int, error) {
			sideEffects = append(sideEffects, "one")
			return 1, nil
		})
		if err != nil {
			return out{}, err
		}
		b, err := rt.Step(c, "two", func() (int, error) {
			sideEffects = append(sideEffects, "two")
			return a + 1, nil
		})
		if err != nil {
			return out{}, err
		}
		cc, err := rt.Step(c, "three", func() (int, error) {
			sideEffects = append(sideEffects, "three")
			return b + 1, nil
		})
		if err != nil {
			return out{}, err
		}
		return out{Sum: cc}, nil
	})

	done, err := e2.RunPending(ctx)
	if err != nil {
		t.Fatalf("RunPending: %v", err)
	}
	if len(done) != 1 || done[0] != id {
		t.Fatalf("RunPending finished %v, want [%s]", done, id)
	}

	// The whole point: three steps, three side effects, none of them twice.
	want := []string{"one", "two", "three"}
	if len(sideEffects) != len(want) {
		t.Fatalf("side effects = %v, want exactly %v", sideEffects, want)
	}
	for i := range want {
		if sideEffects[i] != want[i] {
			t.Fatalf("side effects = %v, want %v", sideEffects, want)
		}
	}
}

// rt.Now must be frozen at its first-run value, or a workflow that branches on
// time takes a different path on replay.
func TestJournaledClockIsStableAcrossReplay(t *testing.T) {
	ctx := context.Background()
	store := rt.NewMemStore()

	var seen []string
	register := func(e *rt.Engine, failAfter bool) {
		rt.Register(e, "clocked", func(c *rt.Context, i in) (out, error) {
			at, err := rt.Now(c)
			if err != nil {
				return out{}, err
			}
			seen = append(seen, at.Format("2006-01-02T15:04:05.000000000Z07:00"))
			if failAfter {
				return out{}, errors.New("stop")
			}
			return out{Sum: 1}, nil
		})
	}

	e1 := rt.New(store)
	register(e1, true)
	id, err := e1.Start(ctx, "clocked", in{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := e1.Run(ctx, id); err == nil {
		t.Fatal("expected the first attempt to fail")
	}

	e2 := rt.New(store)
	register(e2, false)
	if _, err := e2.Run(ctx, id); err != nil {
		t.Fatalf("resume: %v", err)
	}

	if len(seen) != 2 {
		t.Fatalf("workflow body ran %d times, want 2", len(seen))
	}
	if seen[0] != seen[1] {
		t.Fatalf("rt.Now returned %s then %s — the clock is not journaled", seen[0], seen[1])
	}
}

// Changing workflow code under an in-flight run must be caught rather than
// silently mis-executed. This is the failure mode workflow versioning exists to
// solve in stage 6.
func TestNondeterminismIsDetected(t *testing.T) {
	ctx := context.Background()
	store := rt.NewMemStore()

	e1 := rt.New(store)
	rt.Register(e1, "drifting", func(c *rt.Context, i in) (out, error) {
		if _, err := rt.Step(c, "a", func() (int, error) { return 1, nil }); err != nil {
			return out{}, err
		}
		return out{}, errors.New("stop here")
	})

	id, err := e1.Start(ctx, "drifting", in{N: 1})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := e1.Run(ctx, id); err == nil {
		t.Fatal("expected the first attempt to fail")
	}

	// Same workflow type, redeployed: step 1 now has a different name.
	e2 := rt.New(store)
	rt.Register(e2, "drifting", func(c *rt.Context, i in) (out, error) {
		if _, err := rt.Step(c, "a_renamed", func() (int, error) { return 1, nil }); err != nil {
			return out{}, err
		}
		return out{}, nil
	})

	res, err := e2.Run(ctx, id)
	if !errors.Is(err, rt.ErrNondeterministic) {
		t.Fatalf("got error %v, want ErrNondeterministic", err)
	}
	if res.Status != "failed" {
		t.Fatalf("nondeterministic run left status %q, want failed (terminal)", res.Status)
	}
}

func TestUnknownWorkflowIDIsReported(t *testing.T) {
	e := rt.New(rt.NewMemStore())
	if _, err := e.Run(context.Background(), "no-such-id"); !errors.Is(err, rt.ErrWorkflowNotFound) {
		t.Fatalf("got %v, want ErrWorkflowNotFound", err)
	}
}
