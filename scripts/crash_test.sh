#!/usr/bin/env bash
# The proof for stage 1.
#
# Runs a three-step workflow, kills the process after step 2 commits, then runs
# it again. Asserts that every step's side effect happened exactly once — which
# is the whole claim durable execution makes.
#
#   ./scripts/crash_test.sh
set -uo pipefail

DSN="${RATCHET_DSN:-postgres://postgres@localhost:5432/ratchet?sslmode=disable}"
PSQL=(psql "$DSN" -qtAX)

bold() { printf '\033[1m%s\033[0m\n' "$1"; }
fail() { printf '\033[31mFAIL\033[0m  %s\n' "$1"; exit 1; }
pass() { printf '\033[32mok\033[0m    %s\n' "$1"; }

bold "resetting tables"
"${PSQL[@]}" -c "TRUNCATE steps, workflows, side_effects RESTART IDENTITY CASCADE;" >/dev/null || \
  fail "could not reach the database at $DSN (is postgres up, and migrated?)"

echo
bold "run 1 — dies after step 2 commits"
RATCHET_DSN="$DSN" ${GO:-go} run ./cmd/demo -crash-after 2
status=$?
[ $status -eq 1 ] || fail "expected the demo to exit 1 on the simulated crash, got $status"

journaled=$("${PSQL[@]}" -c "SELECT count(*) FROM steps;")
[ "$journaled" = "2" ] || fail "expected 2 journaled steps after the crash, found $journaled"
pass "2 steps journaled, process dead"

echo
bold "run 2 — resumes from the journal"
RATCHET_DSN="$DSN" ${GO:-go} run ./cmd/demo -resume || fail "resume failed"

echo
bold "the evidence"
"${PSQL[@]}" -P border=2 -P format=aligned \
  -c "SELECT step_name, count(*) AS side_effects FROM side_effects GROUP BY step_name ORDER BY min(id);"

dupes=$("${PSQL[@]}" -c "SELECT count(*) FROM (SELECT step_name FROM side_effects GROUP BY step_name HAVING count(*) > 1) d;")
[ "$dupes" = "0" ] || fail "$dupes step(s) had their side effect more than once"
pass "no step's side effect happened twice"

total=$("${PSQL[@]}" -c "SELECT count(*) FROM side_effects;")
[ "$total" = "3" ] || fail "expected 3 side effects in total, found $total"
pass "3 side effects, one per step"

state=$("${PSQL[@]}" -c "SELECT status FROM workflows;")
[ "$state" = "completed" ] || fail "workflow ended in state '$state', want completed"
pass "workflow completed"

echo
bold "journal"
"${PSQL[@]}" -P border=2 -P format=aligned -c "SELECT seq, name, result FROM steps ORDER BY seq;"
