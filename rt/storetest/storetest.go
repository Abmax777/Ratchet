// Package storetest is a conformance suite for rt.Store implementations.
//
// MemStore and pgstore run the same tests, which is how we know the in-memory
// store the runtime's unit tests use behaves like the real one: same leases,
// same fencing, same refusal to journal a step twice.
package storetest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/abmax777/ratchet/rt"
)

// Run runs every conformance test against stores made by newStore. Each test
// gets a fresh store (or a freshly emptied database).
func Run(t *testing.T, newStore func(t *testing.T) rt.Store) {
	t.Run("UnknownIDIsNotFound", func(t *testing.T) { unknownID(t, newStore(t)) })
	t.Run("StepCommitsAtMostOnce", func(t *testing.T) { commitOnce(t, newStore(t)) })
	t.Run("StaleLeaseIsFenced", func(t *testing.T) { fencing(t, newStore(t)) })
	t.Run("ConcurrentClaimsNeverCollide", func(t *testing.T) { concurrentClaims(t, newStore(t)) })
	t.Run("ReleaseDelaysTheNextClaim", func(t *testing.T) { releaseDelay(t, newStore(t)) })
	t.Run("FailIsTerminalUntilRetry", func(t *testing.T) { failAndRetry(t, newStore(t)) })
	t.Run("RunWithoutLeaseIsAbandoned", func(t *testing.T) { abandonedDirectRun(t, newStore(t)) })
}

const unknownUUID = "00000000-0000-4000-8000-000000000000"

func create(t *testing.T, s rt.Store, id string) {
	t.Helper()
	if err := s.CreateWorkflow(context.Background(), id, "t", []byte(`{"n":1}`)); err != nil {
		t.Fatalf("create %s: %v", id, err)
	}
}

func claim(t *testing.T, s rt.Store, owner string, ttl time.Duration) rt.Lease {
	t.Helper()
	l, ok, err := s.Claim(context.Background(), owner, ttl)
	if err != nil || !ok {
		t.Fatalf("claim by %s: ok=%v err=%v", owner, ok, err)
	}
	return l
}

func unknownID(t *testing.T, s rt.Store) {
	ctx := context.Background()
	for _, id := range []string{unknownUUID, "not-a-uuid"} {
		if _, err := s.LoadWorkflow(ctx, id); !errors.Is(err, rt.ErrWorkflowNotFound) {
			t.Fatalf("LoadWorkflow(%q): got %v, want ErrWorkflowNotFound", id, err)
		}
		if _, err := s.Acquire(ctx, id); !errors.Is(err, rt.ErrWorkflowNotFound) {
			t.Fatalf("Acquire(%q): got %v, want ErrWorkflowNotFound", id, err)
		}
	}
}

func commitOnce(t *testing.T, s rt.Store) {
	ctx := context.Background()
	id := "11111111-1111-4111-8111-111111111111"
	create(t, s, id)
	l, err := s.Acquire(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CommitStep(ctx, l, 1, "a", []byte(`1`)); err != nil {
		t.Fatalf("first commit: %v", err)
	}
	if err := s.CommitStep(ctx, l, 1, "a", []byte(`2`)); !errors.Is(err, rt.ErrStepAlreadyCommitted) {
		t.Fatalf("second commit: got %v, want ErrStepAlreadyCommitted", err)
	}
	j, err := s.LoadJournal(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if string(j[1].Result) != "1" {
		t.Fatalf("journal[1] = %s, want the first result, untouched", j[1].Result)
	}
}

// A worker claims a run, stalls past its lease, and a second worker claims
// it. When the first wakes up, every write it attempts must be refused.
func fencing(t *testing.T, s rt.Store) {
	ctx := context.Background()
	id := "22222222-2222-4222-8222-222222222222"
	create(t, s, id)

	zombie := claim(t, s, "zombie", 50*time.Millisecond)
	if _, ok, _ := s.Claim(ctx, "other", time.Minute); ok {
		t.Fatal("a held lease was claimed by someone else")
	}
	time.Sleep(120 * time.Millisecond) // the zombie's lease expires

	owner := claim(t, s, "owner", time.Minute)
	if owner.WorkflowID != id || owner.Epoch <= zombie.Epoch {
		t.Fatalf("reclaim: got %+v after %+v, want same run with a higher epoch", owner, zombie)
	}

	for name, err := range map[string]error{
		"Heartbeat":  s.Heartbeat(ctx, zombie, time.Minute),
		"CommitStep": s.CommitStep(ctx, zombie, 1, "a", []byte(`"zombie"`)),
		"Complete":   s.Complete(ctx, zombie, []byte(`{}`)),
		"Fail":       s.Fail(ctx, zombie, "zombie"),
		"Release":    s.Release(ctx, zombie, "zombie", 0, 1),
	} {
		if !errors.Is(err, rt.ErrLeaseLost) {
			t.Errorf("zombie %s: got %v, want ErrLeaseLost", name, err)
		}
	}

	if err := s.CommitStep(ctx, owner, 1, "a", []byte(`"owner"`)); err != nil {
		t.Fatalf("owner commit: %v", err)
	}
	if err := s.Complete(ctx, owner, []byte(`{"ok":true}`)); err != nil {
		t.Fatalf("owner complete: %v", err)
	}
	row, _ := s.LoadWorkflow(ctx, id)
	if row.Status != rt.StatusCompleted {
		t.Fatalf("status %q, want completed", row.Status)
	}
}

func concurrentClaims(t *testing.T, s rt.Store) {
	ctx := context.Background()
	const runs, workers = 40, 8
	for i := 0; i < runs; i++ {
		create(t, s, rt.NewID())
	}
	var (
		mu   sync.Mutex
		seen = map[string]int{}
		wg   sync.WaitGroup
	)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				l, ok, err := s.Claim(ctx, "w", time.Minute)
				if err != nil {
					t.Errorf("claim: %v", err)
					return
				}
				if !ok {
					return
				}
				mu.Lock()
				seen[l.WorkflowID]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(seen) != runs {
		t.Fatalf("claimed %d distinct runs, want %d", len(seen), runs)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("run %s claimed %d times", id, n)
		}
	}
}

func releaseDelay(t *testing.T, s rt.Store) {
	ctx := context.Background()
	id := "33333333-3333-4333-8333-333333333333"
	create(t, s, id)
	l := claim(t, s, "w", time.Minute)
	if err := s.Release(ctx, l, "boom", time.Hour, 3); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.Claim(ctx, "w", time.Minute); ok {
		t.Fatal("a run released with an hour's delay was claimable at once")
	}
	if ids, _ := s.Runnable(ctx); len(ids) != 0 {
		t.Fatalf("Runnable = %v, want none", ids)
	}
	row, _ := s.LoadWorkflow(ctx, id)
	if row.Status != rt.StatusPending || row.Attempts != 3 || row.LastError != "boom" {
		t.Fatalf("after release: %+v", row)
	}
	// An explicit Acquire ignores the delay.
	if _, err := s.Acquire(ctx, id); err != nil {
		t.Fatalf("acquire during backoff: %v", err)
	}
}

func failAndRetry(t *testing.T, s rt.Store) {
	ctx := context.Background()
	id := "44444444-4444-4444-8444-444444444444"
	create(t, s, id)
	l := claim(t, s, "w", time.Minute)
	if err := s.CommitStep(ctx, l, 1, "a", []byte(`1`)); err != nil {
		t.Fatal(err)
	}
	if err := s.Fail(ctx, l, "permanent"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Acquire(ctx, id); !errors.Is(err, rt.ErrLeaseLost) {
		t.Fatalf("Acquire on a failed run: got %v, want ErrLeaseLost", err)
	}
	if ids, _ := s.Runnable(ctx); len(ids) != 0 {
		t.Fatalf("a failed run is runnable: %v", ids)
	}
	if err := s.Retry(ctx, id); err != nil {
		t.Fatal(err)
	}
	l2 := claim(t, s, "w", time.Minute)
	if l2.WorkflowID != id {
		t.Fatalf("claimed %s after retry, want %s", l2.WorkflowID, id)
	}
	if j, _ := s.LoadJournal(ctx, id); len(j) != 1 {
		t.Fatalf("retry lost the journal: %v", j)
	}
}

// A direct Run that dies with its process leaves 'running' and no lease. Such
// a run belongs to nobody and must be claimable at once.
func abandonedDirectRun(t *testing.T, s rt.Store) {
	ctx := context.Background()
	id := "55555555-5555-4555-8555-555555555555"
	create(t, s, id)
	if _, err := s.Acquire(ctx, id); err != nil {
		t.Fatal(err)
	}
	l := claim(t, s, "w", time.Minute)
	if l.WorkflowID != id {
		t.Fatalf("claimed %s, want the abandoned %s", l.WorkflowID, id)
	}
}
