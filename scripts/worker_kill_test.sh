#!/usr/bin/env bash
# The proof for stage 2.
#
# Queues 30 workflows, starts three worker processes, and SIGKILLs one of them
# while it is mid-run. The dead worker never releases its claims: its leases
# simply stop being renewed. Once they expire the surviving workers claim those
# runs, replay whatever the dead worker had already journaled, and finish them.
#
#   ./scripts/worker_kill_test.sh
#
# What it asserts:
#   - every run completes, including the ones the dead worker was holding
#   - every completed run has exactly one journal row per step
#   - some runs were claimed more than once (epoch > 1): the takeover happened
#
# What it reports but cannot assert: duplicate side effects. A worker killed
# after a step's side effect but before its journal commit leaves an effect
# that will happen again on takeover. That is the at-least-once window, at most
# one per run the dead worker held. Idempotency keys (stage 3) close it.
set -uo pipefail

DSN="${RATCHET_DSN:-postgres://postgres@localhost:5432/ratchet?sslmode=disable}"
PSQL=(psql "$DSN" -qtAX)
GO="${GO:-go}"
RUNS=30
LEASE=3s

bold() { printf '\033[1m%s\033[0m\n' "$1"; }
fail() { printf '\033[31mFAIL\033[0m  %s\n' "$1"; kill "${PIDS[@]}" 2>/dev/null; exit 1; }
pass() { printf '\033[32mok\033[0m    %s\n' "$1"; }
PIDS=()

bold "building"
mkdir -p bin && "$GO" build -o bin/demo ./cmd/demo || fail "build failed"

bold "resetting tables, queueing $RUNS runs"
"${PSQL[@]}" -c "TRUNCATE steps, workflows, side_effects RESTART IDENTITY CASCADE;" >/dev/null || \
  fail "could not reach the database at $DSN (is postgres up, and migrated?)"
RATCHET_DSN="$DSN" ./bin/demo -enqueue "$RUNS" >/dev/null || fail "enqueue failed"

echo
bold "starting workers w1, w2, w3 (lease $LEASE)"
for w in w1 w2 w3; do
  RATCHET_DSN="$DSN" ./bin/demo -work -worker-id "$w" -concurrency 3 -lease "$LEASE" > "bin/$w.log" 2>&1 &
  PIDS+=($!)
done
VICTIM=${PIDS[1]}

sleep 2.5
held=$("${PSQL[@]}" -c "SELECT count(*) FROM workflows WHERE locked_by = 'w2' AND status = 'running';")
bold "SIGKILL w2 (pid $VICTIM) while it holds $held run(s)"
kill -9 "$VICTIM"

bold "waiting for the survivors to finish everything"
for i in $(seq 1 120); do
  left=$("${PSQL[@]}" -c "SELECT count(*) FROM workflows WHERE status <> 'completed';")
  [ "$left" = "0" ] && break
  sleep 1
done

bold "SIGTERM w1 and w3 (graceful shutdown)"
kill -TERM "${PIDS[0]}" "${PIDS[2]}"
wait "${PIDS[0]}" "${PIDS[2]}" 2>/dev/null
grep -h "stopped cleanly" bin/w1.log bin/w3.log

echo
bold "the evidence"
"${PSQL[@]}" -P border=2 -P format=aligned -c "
  SELECT status, count(*) AS runs, count(*) FILTER (WHERE epoch > 1) AS claimed_more_than_once
  FROM workflows GROUP BY status;"

left=$("${PSQL[@]}" -c "SELECT count(*) FROM workflows WHERE status <> 'completed';")
[ "$left" = "0" ] || fail "$left run(s) never completed"
pass "all $RUNS runs completed, including the ones w2 died holding"

bad=$("${PSQL[@]}" -c "
  SELECT count(*) FROM (
    SELECT w.id FROM workflows w LEFT JOIN steps s ON s.workflow_id = w.id
    GROUP BY w.id HAVING count(s.seq) <> 4) x;")
[ "$bad" = "0" ] || fail "$bad run(s) do not have exactly 4 journal rows"
pass "every run has exactly one journal row per step (4)"

taken=$("${PSQL[@]}" -c "SELECT count(*) FROM workflows WHERE epoch > 1;")
[ "$taken" -gt 0 ] || fail "no run was ever reclaimed: the kill landed between runs, try again"
pass "$taken run(s) were taken over after w2 died"

dupes=$("${PSQL[@]}" -c "
  SELECT coalesce(sum(n - 1), 0) FROM (
    SELECT count(*) AS n FROM side_effects GROUP BY workflow_id, step_name HAVING count(*) > 1) d;")
echo "info  duplicate side effects: $dupes (the at-least-once window; at most $held possible)"
[ "$dupes" -le "$held" ] || fail "more duplicate side effects than runs w2 held"
pass "duplicates stay within the at-least-once bound"
