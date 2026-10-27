-- Stage 2 and 3: leases, fencing, retry scheduling.
--
-- locked_by / locked_until   who holds the lease, and until when. A running
--                            row with an expired (or NULL) lease is abandoned
--                            and can be claimed by another worker.
-- epoch                      the fencing token. Every claim bumps it, and every
--                            write a runner makes is conditioned on the epoch it
--                            claimed with. A worker that stalls past its lease
--                            and wakes up later finds its writes refused.
-- attempts                   consecutive failed attempts without progress; the
--                            retry policy reads it, a committed step resets it.
-- next_attempt_at            when a pending run may be claimed again. A run
--                            waiting out its backoff sits here, not in a sleeping
--                            goroutine holding a worker.

ALTER TABLE workflows
    ADD COLUMN IF NOT EXISTS locked_by       text,
    ADD COLUMN IF NOT EXISTS locked_until    timestamptz,
    ADD COLUMN IF NOT EXISTS epoch           bigint      NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS attempts        integer     NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS next_attempt_at timestamptz NOT NULL DEFAULT now();

-- The claim query scans unfinished runs oldest first. A partial index keeps it
-- small: finished runs, which are most rows over time, are not in it at all.
DROP INDEX IF EXISTS workflows_runnable_idx;
CREATE INDEX IF NOT EXISTS workflows_claim_idx
    ON workflows (created_at)
    WHERE status IN ('pending', 'running');
