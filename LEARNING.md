# Ratchet — build it yourself

Nine tasks. The scaffolding, the tests and the crash-test script are here; the
mechanism is not.

**Read the tutorial first.** This file is the checklist and the hints; the
tutorial is where the concepts are explained from the ground up. Each task
points at the part that covers it.

The tests are the second spec. Read `rt/replay_test.go` before you start — it
is the clearest statement of what this thing is supposed to do.

## The tasks

| # | file | what | tutorial |
|---|---|---|---|
| 1 | `migrations/001_init.sql` | design the two tables | part 6 |
| 2 | `rt/memstore.go` | the Store, in maps | part 2 |
| 3 | `rt/rt.go` | `Step` — the heart | part 3 |
| 4 | `rt/rt.go` | `Now` / `Rand` — journaled non-determinism | part 4 |
| 5 | `rt/engine.go` | `Register` — type erasure | part 2 |
| 6 | `rt/engine.go` | `Run` — the state machine | part 5 |
| 7 | `rt/engine.go` | `RunPending` — abandoned runs | part 5 |
| 8 | `store/pgstore/pgstore.go` | the same Store, in SQL | part 6 |
| 9 | `cmd/demo/main.go` | write a workflow | part 7 |

## Milestones

Work in pushes. Each one ends somewhere you can stop.

| push | tasks | result |
|---|---|---|
| 1 | 2, 3, 5, 6 | `make test` — 4 of 6 green |
| 2 | 4 | 5 of 6 |
| 3 | 7 | **all 6 green — the runtime works** |
| 4 | 1, 8 | the same runtime, against a real database |
| 5 | 9 | `make crash-test` green — the whole thing |

Note push 1: **first green is a plateau, not a step.** Nothing in the suite
passes until `MemStore`, `Step`, `Register` and `Run` all exist, because every
test needs all four. Do them in one sitting if you can.

To get feedback before then, run `go vet ./rt/...` after every task — it catches
signature and type mistakes without running anything. And after task 2, the
tutorial has a twenty-line scratch program that exercises `MemStore` on its own.

Until task 8, `go build ./...` fails: `pgstore` does not satisfy `rt.Store` yet,
and the compiler lists exactly which methods are missing. That is expected —
`make test` only builds `./rt/...`.

## How to use the hints

Three levels each. Read level 1, go away and try. Come back for 2 only when you
are stuck on *approach* rather than syntax. Level 3 names the specific thing
that bites — use it to get unstuck, not to skip the thinking.

---

## Task 1 — the schema (`migrations/001_init.sql`)

**Done when:** `make migrate` succeeds and `\d workflows` / `\d steps` in psql
show what you intended.

<details><summary>Hint 1</summary>

`workflows` needs: id, type, status, input, output, last_error, created_at,
updated_at. `steps` needs: workflow_id, seq, name, result, created_at.

The interesting decisions are which columns are `NOT NULL`, what the primary key
of `steps` is, and how you stop an invalid status from ever being stored.
</details>

<details><summary>Hint 2</summary>

`PRIMARY KEY (workflow_id, seq)` on `steps`. That composite key is the safety
property: the same step cannot be committed twice, ever, by anyone. Your Go code
inherits that guarantee for free.

`status text NOT NULL CHECK (status IN ('pending','running','completed','failed'))`
turns a Go typo into a loud database error instead of a silently wrong row.

`output` and `last_error` are nullable — a run that has not finished has
neither.
</details>

<details><summary>Hint 3</summary>

Add `REFERENCES workflows(id) ON DELETE CASCADE` to `steps.workflow_id`, so
deleting a run takes its journal with it.

A partial index — `CREATE INDEX ... ON workflows (created_at) WHERE status IN
('pending','running')` — makes `Runnable` cheap once the table has a million
finished rows in it. Not needed for correctness; worth doing because it is the
kind of thing you should notice.
</details>

---

## Task 2 — `MemStore` (`rt/memstore.go`)

Seven methods, no SQL, no generics. `rt.Store` implemented with maps.

**Done when:** it compiles and the scratch program in tutorial part 2 runs.

<details><summary>Hint 1</summary>

The interface deals in `WorkflowRow` (type, status, input) and `JournalEntry`
(name, result). A workflow is a row *plus* its journal, so one struct holding
both beats two parallel maps.

`LoadJournal` hands its map to the engine, which hands it to `Step`. Think about
whether you want callers holding a reference to your internal state.
</details>

<details><summary>Hint 2</summary>

`map[string]*workflow`, where `workflow` holds a `WorkflowRow`, the output
bytes, the last error, and a `map[int]JournalEntry`. One mutex around the whole
thing.

Two traps:

- **`Runnable` returns oldest-first**, and Go randomises map iteration
  deliberately. Keep a monotonic counter, stamp each workflow on create, sort
  by it.
- **`CommitStep` rejects a second commit** for the same `(id, seq)` with
  `ErrStepAlreadyCommitted` rather than overwriting. You are hand-enforcing what
  the primary key in task 1 enforces, so both stores behave identically and the
  tests do not care which they run against.
</details>

<details><summary>Hint 3</summary>

`LoadJournal` should return a *copy*. Hand out the internal map and `Step`
mutates your store directly — which works, right up until it hides a bug in
task 3.

`SetStatus` is called with `output == nil` on most transitions. Only overwrite a
stored output when the argument is non-nil, or the `running` write that precedes
execution wipes a result from an earlier attempt.
</details>

---

## Task 3 — `Step` (`rt/rt.go`)

The heart of the project, and about thirty lines. Tutorial part 3 is the one to
read closely.

**Done when:** four of the tests pass, once tasks 5 and 6 exist to call it.

<details><summary>Hint 1</summary>

Steps are identified by *position*, not name. So `Context` needs a counter, and
the first thing `Step` does is claim its number.

Then one question decides everything: is there already a journal entry at this
position?
</details>

<details><summary>Hint 2</summary>

```
seq = ++counter
if entry, found := journal[seq]:
        if entry.Name != name  -> ErrNondeterministic, wrapped with %w
        json.Unmarshal(entry.Result) into T; Replayed++; return    // fn NOT called
else:
        v, err := fn()
        if err != nil -> return it; journal nothing
        json.Marshal(v); store.CommitStep(...); cache it in journal; Executed++
        return v
```

Include both names in the nondeterminism error. You will read that message at
2am after a deploy.
</details>

<details><summary>Hint 3</summary>

**Cache the entry you just committed into the in-memory journal map too.**
Nothing in stage 1 forces you to and every test still passes — until a workflow
calls `Step` after a step that already ran in the same attempt, and your counter
and journal disagree. Keep the map and the store saying the same thing.

**Look hard at the order of the last two operations.** `fn()` runs, *then* the
result is committed. A crash in that gap means the side effect happened and
nobody recorded it, so it runs again on resume. That gap is exactly why this is
*at-least-once* and not exactly-once, and no rearrangement closes it —
committing first would record steps that never ran. Put a comment where you
notice it. Best thing you will be able to say about this project in an
interview.
</details>

---

## Task 4 — `Now` and `Rand` (`rt/rt.go`)

Four lines each, once `Step` exists.

**Done when:** `TestJournaledClockIsStableAcrossReplay` passes.

<details><summary>Hint 1</summary>

Everything non-deterministic becomes deterministic by going through a step. You
already built the machine; these just use it.
</details>

<details><summary>Hint 2</summary>

`return Step(c, "$now", func() (time.Time, error) { return time.Now().UTC(), nil })`

The `$` prefix is a convention marking a runtime-owned step name, so it cannot
collide with a user's. `.UTC()` matters: a `time.Time` carrying a location
round-trips through JSON differently than you expect.
</details>

<details><summary>Hint 3</summary>

These take a step position like any other step, which means adding an `rt.Now`
call to an existing workflow shifts every step after it by one — and the
nondeterminism check will catch that on any in-flight run. That is correct
behaviour, and it is the problem workflow versioning solves in stage 6. Meet it
now, on purpose: exercise 3 at the bottom of this file.
</details>

---

## Task 5 — `Register` (`rt/engine.go`)

**Done when:** `go vet ./rt/...` is clean and the tests get past registration.

<details><summary>Hint 1</summary>

The registry is `map[string]runner` — one concrete function type — but callers
register functions with all sorts of input and output types. Something must
bridge those. That something is a closure.
</details>

<details><summary>Hint 2</summary>

Inside `Register`, build a `runner` that closes over `I` and `O`: it declares a
`var in I`, unmarshals the raw input into it, calls `fn`, then marshals the
result back to `json.RawMessage`. The type parameters are fixed at registration
time, so the map can hold a uniform func while each entry knows its own types.

Guard the unmarshal: `len(raw) > 0` first, since a workflow with no input stores
`null`.
</details>

<details><summary>Hint 3</summary>

Try writing it as a method — `func (e *Engine) Register[I, O any](...)` — once,
and read the compiler error. Go does not allow methods to declare type
parameters. That rule is why half the generic APIs you will meet in Go are
package-level functions taking the receiver as the first argument.
</details>

---

## Task 6 — `Engine.Run` (`rt/engine.go`)

The state machine around the workflow function. Tutorial part 5.

**Done when:** `make test` shows 5 of 6 passing.

<details><summary>Hint 1</summary>

Write the state transitions on paper first. A run enters in some status and
leaves in another, and there are exactly three ways the workflow function can
end. Two of those three are not failures of your code.
</details>

<details><summary>Hint 2</summary>

Load the row. Terminal already? Return its status, run nothing. Look up the
runner by type. Load the journal, mark the row `running`, build the `Context`,
call the runner. Then:

| outcome | persisted status | why |
|---|---|---|
| `nil` | `completed`, store the output | done |
| `errors.Is(err, ErrNondeterministic)` | `failed` | terminal — code changed under the run |
| anything else | `pending` | resumable — journal intact, next attempt fast-forwards |

Fill `Result` from the `Context` counters on **every** path, including failures.
The counters are how you see replay working.
</details>

<details><summary>Hint 3</summary>

Load the journal *before* flipping the status to `running`, or a store error
leaves a row marked running that nothing is running.

`ErrWorkflowNotFound` must survive out of `Run` intact enough for `errors.Is` —
wrap with `%w`, never `%v`.

Sending a failed run back to `pending` rather than `failed` feels wrong the
first time. It is deliberate: the journal is intact, so the next attempt resumes
rather than restarts. Stage 3 adds a policy deciding *when* that attempt
happens; stage 1 only makes it possible.
</details>

---

## Task 7 — `RunPending` (`rt/engine.go`)

Ten lines, one idea.

**Done when:** all six tests pass.

<details><summary>Hint 1</summary>

Ask the store what is runnable, run each, collect the ids that *finished*.

The interesting question is not in this function: what status does a workflow
have when the process running it is SIGKILLed?
</details>

<details><summary>Hint 2</summary>

`Run` set the row to `running` before calling the workflow function. Then the
process died. Nothing set it back — so it sits at `running` with nobody running
it, and if `Runnable` only looks for `pending`, that workflow is stranded
forever.

So `Runnable` returns both. Which is only safe because stage 1 has exactly one
runner: with two, both would grab the same abandoned workflow and race. Telling
"running somewhere, healthy" from "abandoned" is what leases and heartbeats are
for, and that is stage 2's entire reason to exist. Leave a comment saying so.
</details>

<details><summary>Hint 3</summary>

A workflow that came back `pending` did not finish, so it does not belong in the
returned slice. `TestRunPendingResumesAbandonedWorkflow` checks exactly that.
</details>

---

## Task 8 — `pgstore` (`store/pgstore/pgstore.go`)

The same six methods, against Postgres. Tutorial part 6.

**Done when:** `make up && make migrate && make crash-test` goes green.

<details><summary>Hint 1</summary>

Your schema already enforces most of what `MemStore` enforced by hand. This is
mostly translation — plus deciding what to do when the database says no.

Three places have opinions: how `LoadWorkflow` reports a missing row, how
`SetStatus` avoids clobbering a stored output, what `CommitStep` does with a
duplicate.
</details>

<details><summary>Hint 2</summary>

- `LoadWorkflow`: turn `sql.ErrNoRows` into `rt.ErrWorkflowNotFound`, wrapped
  with `%w`. Never let a `database/sql` sentinel escape this package.
- `SetStatus`: `output = COALESCE($3, output)` keeps a stored result when this
  write carries none. Easy to get backwards — check it in psql.
- `CommitStep`: plain `INSERT`, no `ON CONFLICT`. On error use the provided
  `isUniqueViolation` to return `rt.ErrStepAlreadyCommitted`.
- `Runnable`: `WHERE status IN ('pending','running') ORDER BY created_at`.
</details>

<details><summary>Hint 3</summary>

A nil `[]byte` passed as a query argument does not reliably become SQL `NULL`.
If `COALESCE` is not doing what you expect, that is usually why — convert to an
`any` that is literally `nil`.

`jsonb` columns scan cleanly into `[]byte`. Do not reach for a JSON type.

When `crash-test` fails, look at the table before the code:
`SELECT seq, name, result FROM steps ORDER BY seq` shows exactly how far run 1
got, and the bug is usually visible there.
</details>

---

## Task 9 — write a workflow (`cmd/demo/main.go`)

The user-facing side: what it feels like to *use* what you built.

**Done when:** `make demo` completes and `make crash-test` is green.

<details><summary>Hint 1</summary>

Three `rt.Step` calls, the names given in the file's comment, with
`maybeCrash(n)` between them. The step names matter — `scripts/crash_test.sh`
reads them back out of the journal.
</details>

<details><summary>Hint 2</summary>

Each step's function calls `record(c, "<name>")`, sleeps, and returns a value.
The value must survive `json.Marshal` and `json.Unmarshal` — that is what gets
journaled and replayed.

Use `rt.Now(c)` for the timestamp, not `time.Now()`. You will prove to yourself
why in exercise 2 below.
</details>

<details><summary>Hint 3</summary>

`write_row` has nothing useful to return. `struct{}{}` works and journals as
`{}`. Steps that exist purely for their side effect are normal; the journal
entry still records *that it happened*, which is the point.
</details>

---

## When it is all green

Worth more than the next feature:

1. Run `crash-test` with `-crash-after 1` and `-crash-after 3`. Convince
   yourself the resume is correct in each case, not just the one I picked.
2. Break determinism on purpose: put a bare `time.Now()` in the workflow body
   outside a step, print it, crash mid-run, resume. Watch the two runs disagree,
   and understand why `rt.Now` exists.
3. Add a step *between* two existing ones and resume an in-flight workflow. The
   nondeterminism check should catch it. This is what workflow versioning solves
   in stage 6 — meet the problem now so the solution means something.
4. Write down, in your own words, why this is at-least-once and not
   exactly-once. If you can say that clearly, you can defend the whole project.
