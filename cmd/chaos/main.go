// Command chaos is the stage 3 proof: thousands of runs against a flaky,
// rate-limited LLM, with workers killed and stalled at random, once per retry
// policy. It checks invariants that must hold however unlucky the timing, and
// prints a table comparing the policies.
//
//	go run ./cmd/chaos                       # Postgres (RATCHET_DSN), all policies
//	go run ./cmd/chaos -store mem -runs 2000 # in memory, no database needed
//	go run ./cmd/chaos -policy full_jitter -csv results.csv
//
// Invariants, checked after every policy:
//
//  1. Every run ends: completed, or failed for a stated reason. None stuck.
//  2. Every completed run journaled exactly its three steps.
//  3. Every completed run's two external effects happened exactly once, per the
//     idempotency-keyed ledger, and no failed run left an effect behind.
//
// The Postgres mode TRUNCATES the runtime's tables between policies.
package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/abmax777/ratchet/internal/fakeapi"
	"github.com/abmax777/ratchet/internal/fakellm"
	"github.com/abmax777/ratchet/rt"
	"github.com/abmax777/ratchet/store/pgstore"
)

type job struct {
	Text string `json:"text"`
}

type outcome struct {
	Label    string `json:"label"`
	ChargeID string `json:"charge_id"`
}

var policies = []struct {
	name string
	p    rt.RetryPolicy
}{
	{"no_retry", rt.RetryPolicy{MaxAttempts: 1}},
	{"fixed_100ms", rt.RetryPolicy{MaxAttempts: 8, Initial: 100 * time.Millisecond, Multiplier: 1, NoJitter: true}},
	{"exponential", rt.RetryPolicy{MaxAttempts: 8, Initial: 50 * time.Millisecond, Multiplier: 2, Max: 3 * time.Second, NoJitter: true}},
	{"full_jitter", rt.RetryPolicy{MaxAttempts: 8, Initial: 50 * time.Millisecond, Multiplier: 2, Max: 3 * time.Second}},
}

type config struct {
	store                 string
	dsn                   string
	runs                  int
	workers, concurrency  int
	lease                 time.Duration
	killEvery, stallEvery time.Duration
	timeout               time.Duration
	apiLatency            time.Duration
	llm                   fakellm.Config
}

type report struct {
	policy                  string
	runs, completed, failed int
	p50, p99, wall          time.Duration
	llmCalls, rateLimited   int64
	kills, stalls, fenced   int64
	effectCalls, effects    int
	violations              []string
}

func main() {
	var cfg config
	only := flag.String("policy", "all", "policy to run: all, or one of no_retry, fixed_100ms, exponential, full_jitter")
	csvPath := flag.String("csv", "", "also write the results table to this CSV file")
	flag.StringVar(&cfg.store, "store", "pg", "pg or mem")
	flag.StringVar(&cfg.dsn, "dsn", envOr("RATCHET_DSN", "postgres://postgres@localhost:5432/ratchet?sslmode=disable"), "postgres connection string")
	flag.IntVar(&cfg.runs, "runs", 1000, "workflows per policy")
	flag.IntVar(&cfg.workers, "workers", 4, "simulated worker processes")
	flag.IntVar(&cfg.concurrency, "concurrency", 8, "runs per worker at once")
	flag.DurationVar(&cfg.lease, "lease", time.Second, "lease TTL")
	flag.DurationVar(&cfg.killEvery, "kill-every", time.Second, "kill a random worker this often (0 = never)")
	flag.DurationVar(&cfg.stallEvery, "stall-every", 1500*time.Millisecond, "stall a random worker's heartbeats this often (0 = never)")
	flag.DurationVar(&cfg.timeout, "timeout", 3*time.Minute, "give up waiting for runs to finish after this long")
	flag.DurationVar(&cfg.apiLatency, "api-latency", 30*time.Millisecond, "payments API latency after applying an effect")
	flag.Float64Var(&cfg.llm.RatePerSec, "llm-rate", 300, "LLM rate limit, calls per second")
	flag.Float64Var(&cfg.llm.Burst, "llm-burst", 30, "LLM rate limit burst")
	flag.Float64Var(&cfg.llm.ErrorRate, "llm-error-rate", 0.10, "share of LLM calls that 503")
	flag.Float64Var(&cfg.llm.HangRate, "llm-hang-rate", 0.02, "share of LLM calls that hang until the step timeout")
	flag.Float64Var(&cfg.llm.BadRequestRate, "llm-400-rate", 0.005, "share of LLM calls rejected as 400 (permanent)")
	flag.DurationVar(&cfg.llm.Latency, "llm-latency", 15*time.Millisecond, "LLM latency on success")
	flag.Parse()

	var db *sql.DB
	if cfg.store == "pg" {
		var err error
		if db, err = sql.Open("pgx", cfg.dsn); err != nil {
			log.Fatal(err)
		}
		defer db.Close()
		if err := db.Ping(); err != nil {
			log.Fatalf("connecting to %s: %v (or use -store mem)", cfg.dsn, err)
		}
	}

	fmt.Printf("chaos: %d runs per policy, %d workers x %d, lease %s, kill every %s, stall every %s, store %s\n",
		cfg.runs, cfg.workers, cfg.concurrency, cfg.lease, cfg.killEvery, cfg.stallEvery, cfg.store)
	fmt.Printf("llm: %.0f calls/s (burst %.0f), %.0f%% 503, %.0f%% hang, %.1f%% 400\n\n",
		cfg.llm.RatePerSec, cfg.llm.Burst, cfg.llm.ErrorRate*100, cfg.llm.HangRate*100, cfg.llm.BadRequestRate*100)

	var reports []report
	failedInvariants := false
	for _, pol := range policies {
		if *only != "all" && *only != pol.name {
			continue
		}
		var store rt.Store
		if db != nil {
			if _, err := db.Exec(`TRUNCATE steps, workflows, side_effects RESTART IDENTITY CASCADE`); err != nil {
				log.Fatalf("truncating: %v", err)
			}
			store = pgstore.New(db)
		} else {
			store = rt.NewMemStore()
		}
		r := runPolicy(cfg, store, pol.name, pol.p)
		reports = append(reports, r)
		status := "ok"
		if len(r.violations) > 0 {
			status = fmt.Sprintf("%d INVARIANT VIOLATIONS", len(r.violations))
			failedInvariants = true
		}
		fmt.Printf("  %-12s done in %s: %s\n", pol.name, r.wall.Round(10*time.Millisecond), status)
		for i, v := range r.violations {
			if i == 10 {
				fmt.Printf("    ... and %d more\n", len(r.violations)-10)
				break
			}
			fmt.Println("    " + v)
		}
	}

	printTable(reports)
	if *csvPath != "" {
		if err := writeCSV(*csvPath, reports); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("\nwrote %s\n", *csvPath)
	}
	if failedInvariants {
		os.Exit(1)
	}
}

func runPolicy(cfg config, store rt.Store, name string, policy rt.RetryPolicy) report {
	r := report{policy: name, runs: cfg.runs}
	llm := fakellm.New(cfg.llm)
	ledger := fakeapi.New()
	ledger.Latency = cfg.apiLatency

	// The workflow. Classify is the flaky LLM call, with a timeout so a hang
	// becomes a retryable failure. Charge and notify are external effects
	// keyed by c.IdempotencyKey(), so a step re-executed after a worker death
	// is recognised by the ledger rather than applied twice.
	flow := func(c *rt.Context, j job) (outcome, error) {
		label, err := rt.Step(c, "classify", func() (string, error) {
			out, err := llm.Classify(c.Ctx(), j.Text)
			var e *fakellm.Error
			if errors.As(err, &e) && e.Status == 400 {
				return "", rt.Permanent(err)
			}
			return out, err // 429, 503, timeouts: retryable
		}, rt.WithTimeout(200*time.Millisecond))
		if err != nil {
			return outcome{}, err
		}
		charge, err := rt.Step(c, "charge", func() (string, error) {
			return ledger.Charge(c.Ctx(), c.IdempotencyKey())
		})
		if err != nil {
			return outcome{}, err
		}
		if _, err := rt.Step(c, "notify", func() (string, error) {
			return ledger.Charge(c.Ctx(), c.IdempotencyKey())
		}); err != nil {
			return outcome{}, err
		}
		return outcome{Label: label, ChargeID: charge}, nil
	}

	ctx := context.Background()
	starter := rt.New(store)
	ids := make([]string, 0, cfg.runs)
	for i := 0; i < cfg.runs; i++ {
		id, err := starter.Start(ctx, "chaos_flow", job{Text: strconv.Itoa(i)})
		if err != nil {
			log.Fatalf("start: %v", err)
		}
		ids = append(ids, id)
	}

	// Terminal results are only reported by the worker that wrote them (the
	// write is fenced), so counting them here is exact.
	var finished sync.Map
	var done atomic.Int64
	onResult := func(id string, res rt.Result, err error) {
		if res.Status == rt.StatusCompleted || res.Status == rt.StatusFailed {
			if _, loaded := finished.LoadOrStore(id, res.Status); !loaded {
				done.Add(1)
			}
		}
	}

	var fenced, kills, stalls atomic.Int64
	type worker struct {
		store *killable
		stop  context.CancelFunc
	}
	var (
		mu      sync.Mutex
		fleet   []*worker
		wg      sync.WaitGroup
		started = time.Now()
		seq     int
	)
	spawn := func() {
		seq++
		k := &killable{inner: store, fencedCount: &fenced}
		e := rt.New(k)
		rt.Register(e, "chaos_flow", flow, rt.WithRetryPolicy(policy))
		wctx, stop := context.WithCancel(ctx)
		w := &worker{store: k, stop: stop}
		fleet = append(fleet, w)
		id := fmt.Sprintf("w%d", seq)
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = e.Work(wctx, rt.WorkerOptions{
				ID: id, Concurrency: cfg.concurrency, LeaseTTL: cfg.lease,
				PollInterval: 20 * time.Millisecond, ShutdownGrace: time.Millisecond,
				OnResult: onResult,
			})
		}()
	}
	mu.Lock()
	for i := 0; i < cfg.workers; i++ {
		spawn()
	}
	mu.Unlock()

	// Chaos: kill a random live worker and start a replacement; stall another.
	chaosCtx, stopChaos := context.WithCancel(ctx)
	var chaosWG sync.WaitGroup
	every := func(d time.Duration, f func()) {
		if d <= 0 {
			return
		}
		chaosWG.Add(1)
		go func() {
			defer chaosWG.Done()
			t := time.NewTicker(d)
			defer t.Stop()
			for {
				select {
				case <-chaosCtx.Done():
					return
				case <-t.C:
					mu.Lock()
					f()
					mu.Unlock()
				}
			}
		}()
	}
	live := func() []*worker {
		var out []*worker
		for _, w := range fleet {
			if !w.store.dead.Load() {
				out = append(out, w)
			}
		}
		return out
	}
	every(cfg.killEvery, func() {
		ws := live()
		victim := ws[rand.IntN(len(ws))]
		victim.store.kill()
		victim.stop()
		kills.Add(1)
		spawn()
	})
	every(cfg.stallEvery, func() {
		ws := live()
		ws[rand.IntN(len(ws))].store.stall(3 * cfg.lease)
		stalls.Add(1)
	})

	deadline := time.Now().Add(cfg.timeout)
	for done.Load() < int64(cfg.runs) && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	r.wall = time.Since(started)
	stopChaos()
	chaosWG.Wait()
	mu.Lock()
	for _, w := range fleet {
		w.stop()
	}
	mu.Unlock()
	wg.Wait()

	// Verify, straight from the store.
	effects := ledger.Effects()
	var latencies []time.Duration
	for _, id := range ids {
		row, err := store.LoadWorkflow(ctx, id)
		if err != nil {
			r.violations = append(r.violations, fmt.Sprintf("%s: %v", id, err))
			continue
		}
		journal, _ := store.LoadJournal(ctx, id)
		switch row.Status {
		case rt.StatusCompleted:
			r.completed++
			latencies = append(latencies, row.UpdatedAt.Sub(row.CreatedAt))
			if len(journal) != 3 {
				r.violations = append(r.violations, fmt.Sprintf("%s completed with %d journal entries, want 3", id, len(journal)))
			}
			for _, seq := range []int{2, 3} {
				if !effects[id+":"+strconv.Itoa(seq)] {
					r.violations = append(r.violations, fmt.Sprintf("%s completed but effect %d never happened", id, seq))
				}
			}
		case rt.StatusFailed:
			r.failed++
			if row.LastError == "" {
				r.violations = append(r.violations, fmt.Sprintf("%s failed without a recorded reason", id))
			}
			for _, seq := range []int{2, 3} {
				if effects[id+":"+strconv.Itoa(seq)] && len(journal) < seq {
					r.violations = append(r.violations, fmt.Sprintf("%s failed, but effect %d happened without being journaled", id, seq))
				}
			}
		default:
			r.violations = append(r.violations, fmt.Sprintf("%s still %s after %s", id, row.Status, cfg.timeout))
		}
	}

	slices.Sort(latencies)
	r.p50, r.p99 = percentile(latencies, 0.50), percentile(latencies, 0.99)
	st := llm.Stats()
	r.llmCalls, r.rateLimited = st.Calls, st.RateLimited
	r.kills, r.stalls, r.fenced = kills.Load(), stalls.Load(), fenced.Load()
	r.effectCalls, r.effects = ledger.Calls()
	return r
}

func percentile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(q * float64(len(sorted)-1))
	return sorted[i]
}

func printTable(rs []report) {
	fmt.Println()
	fmt.Println("| policy | completed | failed | p50 | p99 | LLM calls | 429s | kills | stalls | fenced writes | duplicate effect calls absorbed |")
	fmt.Println("|---|---|---|---|---|---|---|---|---|---|---|")
	for _, r := range rs {
		fmt.Printf("| %s | %.1f%% | %d | %s | %s | %d | %d | %d | %d | %d | %d |\n",
			r.policy, 100*float64(r.completed)/float64(r.runs), r.failed,
			r.p50.Round(time.Millisecond), r.p99.Round(time.Millisecond),
			r.llmCalls, r.rateLimited, r.kills, r.stalls, r.fenced, r.effectCalls-r.effects)
	}
}

func writeCSV(path string, rs []report) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	_ = w.Write([]string{"policy", "runs", "completed", "failed", "p50_ms", "p99_ms", "llm_calls", "rate_limited", "kills", "stalls", "fenced_writes", "duplicate_effects_absorbed"})
	for _, r := range rs {
		_ = w.Write([]string{
			r.policy, strconv.Itoa(r.runs), strconv.Itoa(r.completed), strconv.Itoa(r.failed),
			strconv.FormatInt(r.p50.Milliseconds(), 10), strconv.FormatInt(r.p99.Milliseconds(), 10),
			strconv.FormatInt(r.llmCalls, 10), strconv.FormatInt(r.rateLimited, 10),
			strconv.FormatInt(r.kills, 10), strconv.FormatInt(r.stalls, 10), strconv.FormatInt(r.fenced, 10),
			strconv.Itoa(r.effectCalls - r.effects),
		})
	}
	w.Flush()
	return w.Error()
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
