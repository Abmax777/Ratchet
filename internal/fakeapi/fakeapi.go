// Package fakeapi is an external API that dedupes on idempotency keys, the
// way Stripe does.
//
// It is the other half of the exactly-once story. The runtime executes a step
// at least once: a worker can die after the API call and before the journal
// commit, and the step runs again on another worker. If every call carries the
// step's idempotency key (workflow id + step position), the API recognises the
// repeat and returns the original result instead of charging twice. The
// harness then checks the effect happened exactly once per step, and reports
// how many duplicate calls the keys absorbed.
package fakeapi

import (
	"context"
	"errors"
	"sync"
	"time"
)

type Ledger struct {
	// Latency is how long a call takes to return AFTER the effect is applied:
	// the window in which a worker can die having changed the world but
	// before it learns the result, let alone journals it.
	Latency time.Duration

	mu    sync.Mutex
	raw   int
	byKey map[string]int
}

func New() *Ledger { return &Ledger{byKey: make(map[string]int)} }

// Charge applies an effect once per key. A repeated key is acknowledged and
// changes nothing.
func (l *Ledger) Charge(ctx context.Context, key string) (string, error) {
	if key == "" {
		return "", errors.New("missing idempotency key")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	l.mu.Lock()
	l.raw++
	l.byKey[key]++
	l.mu.Unlock()
	if l.Latency > 0 {
		time.Sleep(l.Latency)
	}
	return "ch_" + key, nil
}

// Effects returns the set of keys whose effect happened.
func (l *Ledger) Effects() map[string]bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make(map[string]bool, len(l.byKey))
	for k := range l.byKey {
		out[k] = true
	}
	return out
}

// Calls returns raw calls and distinct effects.
func (l *Ledger) Calls() (raw, unique int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.raw, len(l.byKey)
}
