package rt_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/abmax777/ratchet/rt"
)

var fastPolicy = rt.RetryPolicy{MaxAttempts: 3, Initial: time.Millisecond, Multiplier: 2, Max: 10 * time.Millisecond}

// A permanent error parks the run as failed on the first attempt.
func TestPermanentErrorIsNotRetried(t *testing.T) {
	store := rt.NewMemStore()
	e := rt.New(store)
	rt.Register(e, "bad_request", func(c *rt.Context, i in) (out, error) {
		_, err := rt.Step(c, "call", func() (int, error) {
			return 0, rt.Permanent(errors.New("400 bad request"))
		})
		return out{}, err
	}, rt.WithRetryPolicy(fastPolicy))
	id, _ := e.Start(context.Background(), "bad_request", in{})

	res, err := e.Run(context.Background(), id)
	if res.Status != rt.StatusFailed || !rt.IsPermanent(err) {
		t.Fatalf("got %q, %v; want failed with a permanent error", res.Status, err)
	}
}

// Retryable errors are retried until the policy's budget runs out, then the
// run is parked as failed with the reason recorded.
func TestRetriesStopAtMaxAttempts(t *testing.T) {
	store := rt.NewMemStore()
	e := rt.New(store)
	var calls int
	rt.Register(e, "flaky", func(c *rt.Context, i in) (out, error) {
		_, err := rt.Step(c, "call", func() (int, error) { calls++; return 0, errors.New("503") })
		return out{}, err
	}, rt.WithRetryPolicy(fastPolicy))
	id, _ := e.Start(context.Background(), "flaky", in{})

	for i := 1; i <= 3; i++ {
		res, _ := e.Run(context.Background(), id)
		want := rt.StatusPending
		if i == 3 {
			want = rt.StatusFailed
		}
		if res.Status != want {
			t.Fatalf("attempt %d: status %q, want %q", i, res.Status, want)
		}
	}
	row := status(t, store, id)
	if calls != 3 || !strings.Contains(row.LastError, "gave up after 3 attempts") {
		t.Fatalf("calls %d, last error %q", calls, row.LastError)
	}
}

// Progress resets the count: a long workflow that keeps committing steps is
// not penalised for a failure many steps ago.
func TestProgressResetsAttemptCount(t *testing.T) {
	store := rt.NewMemStore()
	e := rt.New(store)
	failNext := map[string]bool{"a": true, "b": true, "c": true}
	step := func(c *rt.Context, name string) error {
		_, err := rt.Step(c, name, func() (int, error) {
			if failNext[name] {
				failNext[name] = false
				return 0, errors.New("transient")
			}
			return 1, nil
		})
		return err
	}
	rt.Register(e, "long", func(c *rt.Context, i in) (out, error) {
		for _, name := range []string{"a", "b", "c"} {
			if err := step(c, name); err != nil {
				return out{}, err
			}
		}
		return out{Sum: 3}, nil
	}, rt.WithRetryPolicy(rt.RetryPolicy{MaxAttempts: 2, Initial: time.Millisecond, Multiplier: 1}))
	id, _ := e.Start(context.Background(), "long", in{})

	// Three failures in total, but never two in a row without progress, so a
	// budget of two attempts is enough.
	var res rt.Result
	for i := 0; i < 4; i++ {
		res, _ = e.Run(context.Background(), id)
	}
	if res.Status != rt.StatusCompleted {
		t.Fatalf("status %q, want completed", res.Status)
	}
}

// RetryAfter is a floor on the backoff: the run is not claimable before it.
func TestRetryAfterIsRespected(t *testing.T) {
	store := rt.NewMemStore()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store.SetClock(func() time.Time { return now })
	e := rt.New(store)
	rt.Register(e, "rate_limited", func(c *rt.Context, i in) (out, error) {
		_, err := rt.Step(c, "call", func() (int, error) {
			return 0, rt.RetryAfter(errors.New("429"), 10*time.Minute)
		})
		return out{}, err
	}, rt.WithRetryPolicy(fastPolicy))
	id, _ := e.Start(context.Background(), "rate_limited", in{})
	if res, _ := e.Run(context.Background(), id); res.Status != rt.StatusPending {
		t.Fatalf("status %q, want pending", res.Status)
	}

	now = now.Add(9 * time.Minute)
	if ids, _ := store.Runnable(context.Background()); len(ids) != 0 {
		t.Fatal("claimable before Retry-After elapsed")
	}
	now = now.Add(2 * time.Minute)
	if ids, _ := store.Runnable(context.Background()); len(ids) != 1 {
		t.Fatal("not claimable after Retry-After elapsed")
	}
}

// A step's own policy overrides the workflow's.
func TestStepPolicyOverridesWorkflowPolicy(t *testing.T) {
	store := rt.NewMemStore()
	e := rt.New(store)
	rt.Register(e, "strict_step", func(c *rt.Context, i in) (out, error) {
		_, err := rt.Step(c, "once_only", func() (int, error) { return 0, errors.New("nope") },
			rt.WithRetry(rt.RetryPolicy{MaxAttempts: 1}))
		return out{}, err
	}, rt.WithRetryPolicy(rt.RetryPolicy{MaxAttempts: 100, Initial: time.Millisecond}))
	id, _ := e.Start(context.Background(), "strict_step", in{})
	if res, _ := e.Run(context.Background(), id); res.Status != rt.StatusFailed {
		t.Fatalf("status %q, want failed after the step's single attempt", res.Status)
	}
}

// A step that overruns its timeout is cut off and retried.
func TestStepTimeout(t *testing.T) {
	store := rt.NewMemStore()
	e := rt.New(store)
	rt.Register(e, "hangs", func(c *rt.Context, i in) (out, error) {
		_, err := rt.Step(c, "hang", func() (int, error) {
			<-c.Ctx().Done()
			return 0, c.Ctx().Err()
		}, rt.WithTimeout(20*time.Millisecond))
		return out{}, err
	}, rt.WithRetryPolicy(fastPolicy))
	id, _ := e.Start(context.Background(), "hangs", in{})

	start := time.Now()
	res, err := e.Run(context.Background(), id)
	if time.Since(start) > time.Second {
		t.Fatal("the step timeout did not fire")
	}
	if res.Status != rt.StatusPending || !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("got %q, %v; want pending after a timeout", res.Status, err)
	}
}

// The workflow deadline spans attempts: once it passes, no more retries.
func TestWorkflowDeadline(t *testing.T) {
	store := rt.NewMemStore()
	e := rt.New(store)
	rt.Register(e, "slowpoke", func(c *rt.Context, i in) (out, error) {
		_, err := rt.Step(c, "call", func() (int, error) { return 0, errors.New("transient") })
		return out{}, err
	}, rt.WithDeadline(30*time.Millisecond), rt.WithRetryPolicy(rt.RetryPolicy{MaxAttempts: 0, Initial: time.Millisecond}))
	id, _ := e.Start(context.Background(), "slowpoke", in{})

	if res, _ := e.Run(context.Background(), id); res.Status != rt.StatusPending {
		t.Fatalf("first attempt: %q, want pending", res.Status)
	}
	time.Sleep(40 * time.Millisecond)
	res, err := e.Run(context.Background(), id)
	if res.Status != rt.StatusFailed || !strings.Contains(err.Error(), "deadline") {
		t.Fatalf("after the deadline: %q, %v; want failed", res.Status, err)
	}
}

// RetryFailed resumes from the failed step: earlier steps are replayed, not
// re-executed.
func TestRetryFailedResumesFromTheFailedStep(t *testing.T) {
	store := rt.NewMemStore()
	e := rt.New(store)
	var firstCalls int
	broken := true
	rt.Register(e, "fixable", func(c *rt.Context, i in) (out, error) {
		if _, err := rt.Step(c, "first", func() (int, error) { firstCalls++; return 1, nil }); err != nil {
			return out{}, err
		}
		_, err := rt.Step(c, "second", func() (int, error) {
			if broken {
				return 0, rt.Permanent(errors.New("config missing"))
			}
			return 2, nil
		})
		return out{Sum: 2}, err
	})
	id, _ := e.Start(context.Background(), "fixable", in{})
	if res, _ := e.Run(context.Background(), id); res.Status != rt.StatusFailed {
		t.Fatalf("status %q, want failed", res.Status)
	}

	broken = false // someone fixes the config
	if err := e.RetryFailed(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	res, err := e.Run(context.Background(), id)
	if err != nil || res.Status != rt.StatusCompleted || res.Replayed != 1 || firstCalls != 1 {
		t.Fatalf("after retry: %+v, %v, first ran %d times", res, err, firstCalls)
	}
}

// The idempotency key is the same on every attempt of a step, so an external
// API can dedupe on it.
func TestIdempotencyKeyIsStableAcrossAttempts(t *testing.T) {
	store := rt.NewMemStore()
	e := rt.New(store)
	var keys []string
	rt.Register(e, "charge", func(c *rt.Context, i in) (out, error) {
		_, err := rt.Step(c, "charge_card", func() (int, error) {
			keys = append(keys, c.IdempotencyKey())
			if len(keys) == 1 {
				return 0, errors.New("timeout after the charge went through")
			}
			return 1, nil
		})
		return out{}, err
	}, rt.WithRetryPolicy(fastPolicy))
	id, _ := e.Start(context.Background(), "charge", in{})
	_, _ = e.Run(context.Background(), id)
	_, _ = e.Run(context.Background(), id)

	if len(keys) != 2 || keys[0] != keys[1] || keys[0] != id+":1" {
		t.Fatalf("keys = %v, want the same key %q twice", keys, id+":1")
	}
}

func TestBackoffFullJitterStaysUnderTheCeiling(t *testing.T) {
	p := rt.RetryPolicy{Initial: 100 * time.Millisecond, Multiplier: 2, Max: time.Second}
	for attempt, ceiling := range map[int]time.Duration{1: 100 * time.Millisecond, 3: 400 * time.Millisecond, 10: time.Second} {
		for i := 0; i < 200; i++ {
			if d := p.Backoff(attempt); d < 0 || d > ceiling {
				t.Fatalf("attempt %d: %s outside [0, %s]", attempt, d, ceiling)
			}
		}
	}
	p.NoJitter = true
	if d := p.Backoff(3); d != 400*time.Millisecond {
		t.Fatalf("no jitter: %s, want 400ms", d)
	}
}
