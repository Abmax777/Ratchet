// Package fakellm is an LLM provider you control: rate limited, flaky, and
// occasionally hung, on purpose. The chaos harness uses it to measure how the
// runtime's retry policies behave against the failure modes a real provider
// has, without paying for tokens.
package fakellm

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// Config sets how badly the provider behaves.
type Config struct {
	// RatePerSec and Burst define a token bucket. A call that finds it empty
	// gets a 429, the way a real provider's rate limiter answers.
	RatePerSec float64
	Burst      float64
	// ErrorRate is the share of calls that fail with a 503.
	ErrorRate float64
	// HangRate is the share of calls that hang until their context is done.
	HangRate float64
	// BadRequestRate is the share of calls rejected with a 400: permanent.
	BadRequestRate float64
	// Latency is how long a successful call takes.
	Latency time.Duration
}

// Error is an HTTP-shaped failure from the provider.
type Error struct{ Status int }

func (e *Error) Error() string { return fmt.Sprintf("llm provider returned %d", e.Status) }

// Stats counts what the provider saw.
type Stats struct {
	Calls, OK, RateLimited, ServerErrors, Hangs, BadRequests int64
}

type Provider struct {
	cfg Config

	mu     sync.Mutex
	tokens float64
	last   time.Time

	calls, ok, limited, errs, hangs, bad atomic.Int64
}

func New(cfg Config) *Provider {
	return &Provider{cfg: cfg, tokens: cfg.Burst, last: time.Now()}
}

func (p *Provider) Stats() Stats {
	return Stats{
		Calls: p.calls.Load(), OK: p.ok.Load(), RateLimited: p.limited.Load(),
		ServerErrors: p.errs.Load(), Hangs: p.hangs.Load(), BadRequests: p.bad.Load(),
	}
}

func (p *Provider) take() bool {
	if p.cfg.RatePerSec <= 0 {
		return true
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	p.tokens += now.Sub(p.last).Seconds() * p.cfg.RatePerSec
	if p.tokens > p.cfg.Burst {
		p.tokens = p.cfg.Burst
	}
	p.last = now
	if p.tokens < 1 {
		return false
	}
	p.tokens--
	return true
}

// Classify pretends to label a piece of text.
func (p *Provider) Classify(ctx context.Context, text string) (string, error) {
	p.calls.Add(1)
	if !p.take() {
		p.limited.Add(1)
		return "", &Error{Status: 429}
	}
	r := rand.Float64()
	switch {
	case r < p.cfg.BadRequestRate:
		p.bad.Add(1)
		return "", &Error{Status: 400}
	case r < p.cfg.BadRequestRate+p.cfg.HangRate:
		p.hangs.Add(1)
		<-ctx.Done()
		return "", ctx.Err()
	case r < p.cfg.BadRequestRate+p.cfg.HangRate+p.cfg.ErrorRate:
		p.errs.Add(1)
		return "", &Error{Status: 503}
	}
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-time.After(p.cfg.Latency):
	}
	p.ok.Add(1)
	if len(text)%2 == 0 {
		return "acknowledged", nil
	}
	return "rejected", nil
}
