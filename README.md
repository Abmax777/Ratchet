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

Needs Go 1.22+ and a Postgres.

```bash
go mod tidy                # fetches pgx (the only third-party dependency)
make up                    # postgres in docker
make migrate               # apply migrations/001_init.sql
make demo                  # run one workflow start to finish
make crash-test            # kill it mid-flight and prove it resumes cleanly
make test                  # runtime tests, no database needed
```

`make test` runs against the in-memory store, so it needs nothing but Go.

## Layout

```
rt/                 the runtime — standard library only
  rt.go             Context, Step, journaled Now/Rand
  engine.go         Register, Start, Run, RunPending
  store.go          the Store interface
  memstore.go       in-memory Store, for tests
  replay_test.go
store/pgstore/      Postgres implementation of Store
migrations/         schema
cmd/demo/           a three-step workflow with a deterministic kill switch
scripts/            crash_test.sh — the proof
```

## Design notes

**The journal is a table, and its primary key is the safety property.** `steps`
is keyed on `(workflow_id, seq)`, so a step cannot be committed twice, even
under a race. `CommitStep` has no `ON CONFLICT` clause on purpose: with one
runner a conflict is impossible, and once there are many, a conflict means two
of them executed the same workflow — which should be loud.

**The store is an interface.** Not ceremony: it lets workflow logic be unit
tested with no database at all, and it is where lease-aware claim methods slot
in next.

**Errors are resumable by default.** A failing step leaves the journal intact
and sends the run back to `pending`, so the next attempt continues from the last
committed step. There is no retry policy yet — that is deliberate scope, not an
oversight.

## What this is not

- **It is at-least-once, not exactly-once.** The side effect inside a step
  happens before the journal records it. A crash in that gap re-runs the step on
  resume. Narrowing that window is all any durable runtime can do; closing it is
  not possible against an external system, which is why steps that touch one
  want an idempotency key — `WorkflowID()` plus the step's sequence number is a
  good one.
- **One runner at a time.** `RunPending` picks up `running` rows as well as
  `pending` ones, because a SIGKILLed process leaves its workflow marked
  running with nobody executing it. Telling "running somewhere" apart from
  "abandoned" needs leases, which is the next stage.
- **One Postgres, no partitioning, no cross-region.** Out of scope on purpose.

## Where this goes

1. **Journal and replay loop** — *this stage.*
2. Workers, leases and the queue — `FOR UPDATE SKIP LOCKED`, heartbeats,
   fencing tokens against a stalled worker that thinks it still owns a run.
3. Retry policy, backoff with jitter, a failure taxonomy, and a chaos harness
   that kills workers while asserting every side effect happened once.
4. Durable timers and signals — `rt.Sleep(ctx, 72*time.Hour)` without holding a
   worker, and workflows that wait for a human.
5. Multi-tenancy: per-tenant concurrency caps, fair scheduling, backpressure.
6. Control plane — HTTP API and a run inspector, plus workflow versioning.
7. A real agent on top.
