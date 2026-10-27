// Package rt is the Ratchet runtime: durable execution for ordinary Go
// functions.
//
// A workflow is a normal Go function. The only rule is that anything with a
// side effect — an API call, a database write, reading the clock — goes inside
// rt.Step. Step commits its result to the store before execution continues.
//
// When a process dies, the workflow function is executed again from the top on
// the next run. Every Step that already has a journal entry returns the
// recorded value instead of calling its function, so execution fast-forwards to
// wherever it stopped and carries on.
//
// The cost of this trick is a constraint: the workflow function must be
// deterministic *outside* its steps. Same input, same sequence of Step calls,
// in the same order, with the same names. Calling time.Now, rand, or ranging
// over a map in workflow code will desync the journal. Use rt.Now and rt.Rand,
// which are journaled.
package rt

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	mrand "math/rand/v2"
	"strconv"
	"time"
)

// ErrNondeterministic is returned when a replay does not match the journal: the
// workflow reached step N with a different name than the one recorded on an
// earlier run. This is never retryable — either the workflow code changed under
// an in-flight run, or it branches on something that is not in its input. The
// run is parked as failed for a human.
var ErrNondeterministic = errors.New("nondeterministic workflow")

// commitTimeout bounds a journal write. Writes run on a context detached from
// the run's cancellation, so a step that finished just as shutdown began still
// gets recorded, but they must not hang forever either.
const commitTimeout = 10 * time.Second

// Context is passed to a workflow function. It carries the run's identity, the
// lease it runs under, and its journal.
//
// It is deliberately not safe for concurrent use: a workflow function is
// single-threaded by design, because the sequence of Step calls has to be
// reproducible on replay.
type Context struct {
	ctx        context.Context
	store      Store
	workflowID string
	lease      Lease

	// journal holds every committed step, keyed by position (1-based). Loaded
	// once at the start of an attempt, then extended as steps commit, so a
	// replayed step costs a map lookup rather than a query.
	journal map[int]JournalEntry
	// seq is the position of the most recent Step call on this attempt.
	seq int
	// curSeq is the position of the step whose function is running right now,
	// or 0 between steps. IdempotencyKey reads it.
	curSeq int
	// failedPolicy is set when a step with its own retry policy fails, so the
	// engine retries the run on that step's terms rather than the workflow's.
	failedPolicy *RetryPolicy

	Executed int // steps that actually ran on this attempt
	Replayed int // steps served from the journal
}

// Ctx returns the underlying context, for database calls and HTTP requests made
// inside a step. It is cancelled when the run's lease is lost, when the worker
// shuts down, or when a step timeout fires.
func (c *Context) Ctx() context.Context { return c.ctx }

// WorkflowID identifies the run.
func (c *Context) WorkflowID() string { return c.workflowID }

// IdempotencyKey identifies the step currently executing: the workflow id plus
// the step's position. It is stable across retries and across workers, so an
// external API that dedupes on it turns this runtime's at-least-once execution
// into exactly-once effects. Only meaningful inside a step's function; returns
// "" between steps.
func (c *Context) IdempotencyKey() string {
	if c.curSeq == 0 {
		return ""
	}
	return c.workflowID + ":" + strconv.Itoa(c.curSeq)
}

func (c *Context) writeContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(c.ctx), commitTimeout)
}

// StepOption configures one Step call.
type StepOption func(*stepConfig)

type stepConfig struct {
	timeout time.Duration
	policy  *RetryPolicy
}

// WithTimeout bounds one execution of the step's function. The step's
// c.Ctx() carries the deadline, so the function must pass it to whatever it
// calls; Go cannot stop a function that ignores its context. A timed-out step
// is a retryable failure.
func WithTimeout(d time.Duration) StepOption {
	return func(s *stepConfig) { s.timeout = d }
}

// WithRetry overrides the workflow's retry policy when this step fails. An LLM
// call that gets rate limited wants a different policy from a database write.
func WithRetry(p RetryPolicy) StepOption {
	return func(s *stepConfig) { s.policy = &p }
}

// Step runs fn at most once per workflow, ever, as far as the journal is
// concerned.
//
// CONTRACT
//
//   - The first time a workflow reaches this call site, Step calls fn and
//     records the result durably before returning.
//   - On every later run of the same workflow — after a crash, a redeploy, a
//     resume — Step returns that recorded result and does NOT call fn.
//   - Steps are identified by their position in the run, not by their name.
//     The name is there so drift can be detected: if the step at this position
//     was recorded under a different name, the workflow code has changed under
//     an in-flight run and Step fails with ErrNondeterministic.
//   - If fn returns an error, nothing is recorded and the step runs again on
//     the next attempt.
//
// The honest version of "at most once": fn's side effect can happen and the
// process can die before the commit lands. Then fn runs again. Execution is
// at-least-once; IdempotencyKey is how a step makes its effect exactly-once.
func Step[T any](c *Context, name string, fn func() (T, error), opts ...StepOption) (T, error) {
	var zero T
	var cfg stepConfig
	for _, o := range opts {
		o(&cfg)
	}

	c.seq++
	seq := c.seq

	// Replay: the journal already has this position.
	if entry, ok := c.journal[seq]; ok {
		if entry.Name != name {
			return zero, fmt.Errorf("step %d: journal recorded %q but the workflow now calls %q: %w",
				seq, entry.Name, name, ErrNondeterministic)
		}
		var v T
		if err := json.Unmarshal(entry.Result, &v); err != nil {
			// The recorded result no longer fits the type the code expects:
			// the code changed. Retrying cannot fix that.
			return zero, Permanent(fmt.Errorf("step %d (%s): decoding journaled result: %w", seq, name, err))
		}
		c.Replayed++
		return v, nil
	}

	// Execute.
	c.curSeq = seq
	parent := c.ctx
	var cancel context.CancelFunc
	if cfg.timeout > 0 {
		c.ctx, cancel = context.WithTimeout(parent, cfg.timeout)
	}
	v, err := fn()
	if cancel != nil {
		timedOut := errors.Is(c.ctx.Err(), context.DeadlineExceeded) && parent.Err() == nil
		cancel()
		c.ctx = parent
		if err != nil && timedOut {
			err = fmt.Errorf("timed out after %s: %w", cfg.timeout, err)
		}
	}
	c.curSeq = 0

	if err != nil {
		if cfg.policy != nil {
			p := *cfg.policy
			c.failedPolicy = &p
		}
		return zero, fmt.Errorf("step %d (%s): %w", seq, name, err)
	}

	encoded, err := json.Marshal(v)
	if err != nil {
		return zero, Permanent(fmt.Errorf("step %d (%s): encoding result: %w", seq, name, err))
	}

	wctx, wcancel := c.writeContext()
	err = c.store.CommitStep(wctx, c.lease, seq, name, encoded)
	wcancel()
	if err != nil {
		return zero, fmt.Errorf("step %d (%s): committing: %w", seq, name, err)
	}

	c.journal[seq] = JournalEntry{Name: name, Result: encoded}
	c.Executed++
	return v, nil
}

// Now is a journaled clock: the first run records the wall-clock time, every
// later run of the same workflow gets that same instant back. A workflow that
// branches on the time takes the same branch on replay.
func Now(c *Context) (time.Time, error) {
	return Step(c, "rt.now", func() (time.Time, error) {
		return time.Now().UTC(), nil
	})
}

// Rand is a journaled random number in [0, n). Same reasoning as Now.
func Rand(c *Context, n int64) (int64, error) {
	return Step(c, "rt.rand", func() (int64, error) {
		if n <= 0 {
			return 0, Permanent(fmt.Errorf("rt.Rand: n must be positive, got %d", n))
		}
		return mrand.Int64N(n), nil
	})
}

// NewID returns a fresh run id (a v4 UUID).
func NewID() string { return newID() }

// newID returns a RFC 4122 version 4 UUID. Hand-rolled to keep the runtime free
// of third-party dependencies; swap in github.com/google/uuid if you would
// rather not own sixteen bytes of bit-twiddling.
func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		binary.BigEndian.PutUint64(b[0:8], uint64(time.Now().UnixNano()))
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10

	var out [36]byte
	hex.Encode(out[0:8], b[0:4])
	out[8] = '-'
	hex.Encode(out[9:13], b[4:6])
	out[13] = '-'
	hex.Encode(out[14:18], b[6:8])
	out[18] = '-'
	hex.Encode(out[19:23], b[8:10])
	out[23] = '-'
	hex.Encode(out[24:36], b[10:16])
	return string(out[:])
}
