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
	"errors"
	"time"
)

// ErrNondeterministic is returned when a replay does not match the journal: the
// workflow reached step N with a different name than the one recorded on an
// earlier run. This is never retryable — either the workflow code changed under
// an in-flight run, or it branches on something that is not in its input. The
// run is parked as failed for a human.
var ErrNondeterministic = errors.New("nondeterministic workflow")

// Context is passed to a workflow function. It carries the run's identity and
// its journal.
//
// It is deliberately not safe for concurrent use: a workflow function is
// single-threaded by design, because the sequence of Step calls has to be
// reproducible on replay.
//
// TASK 3 owns the unexported fields here — add whatever state Step needs to
// know which step it is up to and what the journal already contains. The
// engine constructs this value, so whatever you add, construct it in
// engine.go too.
type Context struct {
	ctx        context.Context
	store      Store
	workflowID string

	// TODO(task 3): what else does Step need to do its job?

	Executed int // steps that actually ran on this attempt
	Replayed int // steps served from the journal
}

// Ctx returns the underlying context, for database calls and HTTP requests made
// inside a step.
func (c *Context) Ctx() context.Context { return c.ctx }

// WorkflowID identifies the run. It makes a good idempotency key for calls to
// external systems: a retried step sees the same workflow id and the same
// sequence number.
func (c *Context) WorkflowID() string { return c.workflowID }

// Step runs fn at most once per workflow, ever.
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
//     an in-flight run and Step must fail with ErrNondeterministic. That error
//     must be recognisable by errors.Is at the top of the engine.
//   - If fn returns an error, nothing is recorded and the step runs again on
//     the next attempt.
//   - Step maintains Executed and Replayed on the Context, so a caller can see
//     how much of an attempt was real work and how much was fast-forward.
//
// Errors returned from Step should say which step failed, by position and
// name — you will be reading these in anger later.
//
// See TUTORIAL part 3, and LEARNING.md task 3.
func Step[T any](c *Context, name string, fn func() (T, error)) (T, error) {
	panic("TODO(task 3): implement Step — see LEARNING.md")
}

// Now is a journaled clock.
//
// CONTRACT: the first run of a workflow records the wall-clock time here; every
// later run of that same workflow returns the SAME instant rather than the
// current one. A workflow that branches on the time therefore takes the same
// branch on replay as it did on the first run.
//
// This is four lines once Step exists, and writing it is how you understand
// what Step actually gives you. Rand is the same shape. So would be a journaled
// UUID, or a journaled read of a feature flag, or a journaled response from any
// service that answers differently each time you ask.
//
// See TUTORIAL part 4, and LEARNING.md task 4.
func Now(c *Context) (time.Time, error) {
	panic("TODO(task 4): implement Now — see LEARNING.md")
}

// Rand is a journaled random number in [0, n). Same reasoning as Now.
func Rand(c *Context, n int64) (int64, error) {
	panic("TODO(task 4): implement Rand — see LEARNING.md")
}

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
