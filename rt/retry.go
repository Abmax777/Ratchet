package rt

import (
	"math"
	"math/rand/v2"
	"time"
)

// RetryPolicy decides how many times a run is retried and how long it waits in
// between (stage 3).
//
// A waiting run does not hold a worker. The engine releases it back to the
// table with next_attempt_at set in the future, and any worker can claim it
// once that time passes. That is what lets thousands of runs back off at once
// without thousands of sleeping goroutines.
type RetryPolicy struct {
	// MaxAttempts is the number of consecutive attempts without progress before
	// the run is parked as failed. 1 means never retry. 0 means retry forever.
	MaxAttempts int
	// Initial is the backoff ceiling for the first retry.
	Initial time.Duration
	// Multiplier grows the ceiling each attempt. 2 doubles it.
	Multiplier float64
	// Max caps the ceiling.
	Max time.Duration
	// NoJitter waits the full ceiling instead of a random point below it.
	// Only useful as a baseline to measure jitter against.
	NoJitter bool
}

// DefaultRetryPolicy is used when neither the workflow nor the failing step
// sets one.
var DefaultRetryPolicy = RetryPolicy{
	MaxAttempts: 10,
	Initial:     time.Second,
	Multiplier:  2,
	Max:         5 * time.Minute,
}

// Backoff returns the delay before attempt number `attempt` (1-based count of
// failures so far).
//
// Full jitter: pick uniformly in [0, ceiling], where the ceiling grows
// exponentially. Exponential backoff alone spreads retries out over time but
// keeps clients in lockstep: a thousand runs that failed together retry
// together, and hammer the recovering service in synchronized waves. The random
// point decorrelates them. (AWS architecture blog, "Exponential backoff and
// jitter", has the simulation.)
func (p RetryPolicy) Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	mult := p.Multiplier
	if mult < 1 {
		mult = 1
	}
	ceiling := float64(p.Initial) * math.Pow(mult, float64(attempt-1))
	if p.Max > 0 && ceiling > float64(p.Max) {
		ceiling = float64(p.Max)
	}
	if ceiling <= 0 {
		return 0
	}
	if p.NoJitter {
		return time.Duration(ceiling)
	}
	return time.Duration(rand.Int64N(int64(ceiling) + 1))
}

// exhausted reports whether `failures` consecutive failures use up the budget.
func (p RetryPolicy) exhausted(failures int) bool {
	return p.MaxAttempts > 0 && failures >= p.MaxAttempts
}
