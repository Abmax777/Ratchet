# Ratchet

A durable execution engine in Go. Workflows are ordinary functions; every
completed step is a click of a ratchet, and a crash falls back to the last
click rather than to the start.

```
$ make crash-test

run 1 — dies after step 2 commits
workflow 9f3c1e2a-… started
  executing fetch_email  (side effect)
  executing classify     (side effect)
  *** simulated crash after step 2 ***
ok    2 steps journaled, process dead

run 2 — resumes from the journal
  executing write_row    (side effect)
resumed and completed 9f3c1e2a-…   (executed 1, replayed 2)

 step_name   | side_effects
-------------+--------------
 fetch_email |            1
 classify    |            1
 write_row   |            1

ok    no step's side effect happened twice
ok    3 side effects, one per step
ok    workflow completed
```

The process was killed after the second step. On restart the workflow function
ran again *from the top*, but `fetch_email` and `classify` returned their
recorded results instead of executing, and only `write_row` actually ran.

## The idea

```go
func ProcessEmail(c *rt.Context, in Input) (Output, error) {
    msg, err := rt.Step(c, "fetch_email", func() (email, error) {
        return gmail.Fetch(in.MessageID)          // real API call
    })
    if err != nil { return Output{}, err }

    cls, err := rt.Step(c, "classify", func() (classification, error) {
        return llm.Classify(msg)                  // slow, costs money, never the same twice
    })
    if err != nil { return Output{}, err }

    at, err := rt.Now(c)                          // journaled clock
    if err != nil { return Output{}, err }

    _, err = rt.Step(c, "write_row", func() (struct{}, error) {
        return struct{}{}, tracker.Write(cls, at)
    })
    return Output{Label: cls.Label}, err
}
```

Nothing in that function knows it can be killed halfway through.

`Step` commits its result to Postgres before execution continues. On a later
run, a step that already has a journal entry returns the recorded value without
calling its function. The LLM call is the clearest case for why the result must
be *recorded* rather than recomputed: you cannot reproduce it, and asking again
costs money.

### The constraint this buys

Replay only works if the workflow function is deterministic **outside** its
steps. `time.Now()`, `rand`, map iteration order and mutable globals all drift
between runs and desync the journal. Hence `rt.Now` and `rt.Rand`, which are
journaled like any other step.

Drift is detected rather than tolerated: if step *n* has a different name than
the journal records, the run fails with `ErrNondeterministic` and parks for a
human. Retrying could not help.

## Running it

Needs Go 1.25+ (pgx 5.11 requires it) and a Postgres.

```bash
go mod tidy                # fetches pgx (the only third-party dependency)
make up                    # postgres in docker
make migrate               # apply both migrations
make test                  # runtime tests, in memory, no database needed
make test-pg               # the same store conformance suite against Postgres

make demo                  # stage 1: run one workflow start to finish
make crash-test            # stage 1: kill it mid-flight and prove it resumes cleanly
make worker-kill-test      # stage 2: three workers, SIGKILL one mid-run
make chaos                 # stage 3: thousands of runs, random kills and stalls, per retry policy
make chaos-mem             # the same, in memory
```

## Layout

```
rt/                 the runtime: standard library only
  rt.go             Context, Step (+ timeout and retry options), journaled Now/Rand, IdempotencyKey
  engine.go         Register, Start, Run, RunPending, RetryFailed, and the attempt state machine
  worker.go         Work: claim loop, heartbeats, graceful shutdown
  retry.go          RetryPolicy, exponential backoff with full jitter
  errors.go         failure taxonomy: Permanent, RetryAfter
  store.go          the Store interface, leases and fencing
  memstore.go       in-memory Store
  storetest/        conformance suite every Store must pass
store/pgstore/      Postgres implementation of Store
migrations/         001 schema, 002 leases and retries
cmd/demo/           the email workflow: single run, resume, enqueue, worker mode
cmd/chaos/          stage 3 chaos harness
internal/fakellm/   rate-limited, flaky, hanging LLM provider
internal/fakeapi/   payments API that dedupes on idempotency keys
scripts/            crash_test.sh (stage 1), worker_kill_test.sh (stage 2)
```

## Design notes

**The journal is a table, and its primary key is the safety property.** `steps`
is keyed on `(workflow_id, seq)`, so a step cannot be committed twice, even
under a race. `CommitStep` has no `ON CONFLICT` clause on purpose: a conflict
means two runners executed the same step, which should be loud.

**Claiming is one statement.** `UPDATE ... FROM (SELECT ... FOR UPDATE SKIP
LOCKED LIMIT 1)` finds the oldest runnable run, takes its lease and bumps its
epoch at once. Concurrent workers skip each other's locked rows instead of
queueing behind them, and there is no gap between "found it" and "own it".

**Leases expire; epochs fence.** A worker heartbeats every TTL/3. A dead
worker's lease simply runs out and another worker takes the run. A worker that
stalls past its lease and wakes up is the dangerous case, so every write
carries the epoch it claimed with and matches zero rows if the epoch has moved
on (`rt.ErrLeaseLost`). Step commits use `INSERT ... SELECT ... FOR SHARE` so a
claim cannot slip in between the epoch check and the insert. All lease times
come from the database clock, never a worker's.

**Waiting runs do not hold workers.** A failed attempt is released with
`next_attempt_at` in the future. Thousands of runs can back off at once with no
sleeping goroutines.

**Retries are decided by error type.** `rt.Permanent` never retries,
`rt.RetryAfter` sets a floor on the wait, `ErrNondeterministic` parks the run,
anything else retries with exponential backoff and full jitter. The attempt
count resets whenever an attempt commits a step, so a long workflow that keeps
moving is not penalised for old hiccups. Exhausted runs park as `failed` and
`RetryFailed` resumes them from the failed step.

## What this is not

- **Execution is at-least-once.** The side effect inside a step happens before
  the journal records it, and a worker can die in between. Effects are made
  exactly-once by the API on the other side, keyed on `c.IdempotencyKey()`
  (workflow id plus step position). The chaos harness counts how many duplicate
  calls those keys absorb.
- **Direct `Run` and workers should not be mixed on the same run.** `Run` takes
  a run without an expiring lease (stage 1 behaviour), so a worker treats it as
  abandoned. Fencing keeps that safe, but it wastes work.
- **Polling, not LISTEN/NOTIFY.** Idle workers poll with jitter.
- **One Postgres, no partitioning, no cross-region.** Out of scope on purpose.

## Where this goes

1. Journal and replay loop: done.
2. Workers, leases and the queue: done.
3. Retry policy, failure taxonomy, timeouts, chaos harness: done.
4. Durable timers and signals: `rt.Sleep(ctx, 72*time.Hour)` without holding a
   worker, and workflows that wait for a human.
5. Multi-tenancy: per-tenant concurrency caps, fair scheduling, backpressure.
6. Control plane: HTTP API and a run inspector, plus workflow versioning.
7. A real agent on top.
