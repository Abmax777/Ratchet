package rt

import (
	"errors"
	"time"
)

// Failure taxonomy (stage 3).
//
// When an attempt fails, the engine has to decide one thing: is trying again
// worth it? An error answers that question by its type, found with errors.As,
// so it survives any amount of %w wrapping on the way up:
//
//   - Permanent(err)         never retry. A 400, a validation error, a bug.
//   - RetryAfter(err, d)     retry, but not before d. A 429 with Retry-After.
//   - ErrNondeterministic    never retry: the code changed under the run.
//   - anything else          retry with the policy's backoff. Timeouts, 5xx,
//     network errors. Unknown errors default to retryable, because the cost of
//     a wasted retry is lower than the cost of abandoning a run that would
//     have succeeded.

// PermanentError marks a failure that no retry can fix.
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return "permanent: " + e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// Permanent wraps err so the engine parks the run as failed instead of
// retrying it.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &PermanentError{Err: err}
}

// RetryAfterError is a retryable failure that also says how long to wait,
// typically from a rate limiter's Retry-After header.
type RetryAfterError struct {
	Err   error
	After time.Duration
}

func (e *RetryAfterError) Error() string {
	return "retry after " + e.After.String() + ": " + e.Err.Error()
}
func (e *RetryAfterError) Unwrap() error { return e.Err }

// RetryAfter wraps err as retryable no sooner than d.
func RetryAfter(err error, d time.Duration) error {
	if err == nil {
		return nil
	}
	return &RetryAfterError{Err: err, After: d}
}

// IsPermanent reports whether retrying err is pointless.
func IsPermanent(err error) bool {
	if errors.Is(err, ErrNondeterministic) {
		return true
	}
	var p *PermanentError
	return errors.As(err, &p)
}

// retryAfterHint returns the minimum delay an error asks for, if any.
func retryAfterHint(err error) (time.Duration, bool) {
	var ra *RetryAfterError
	if errors.As(err, &ra) {
		return ra.After, true
	}
	return 0, false
}
