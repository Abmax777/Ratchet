-- TASK 1 — design the schema.
--
-- Two tables carry this entire runtime. Write them yourself; the tutorial's
-- "Postgres from zero" and "Design decisions" parts cover everything you need.
--
-- workflows — one row per run. It must be able to answer:
--     what kind of workflow is this, and what was its input?
--     what state is it in right now?
--     if it finished, what did it produce?
--     if it failed, what went wrong?
--     which runs are unfinished, oldest first?
--
--   Constrain the status column to the four states the engine actually uses.
--   A typo in Go should be rejected by the database, not silently stored.
--
-- steps — the journal. One row per COMPLETED step. It must answer:
--     for this run, which steps are already done, in order?
--     what was each one called, and what did it return?
--
--   Choose the primary key deliberately. One choice here makes it physically
--   impossible to commit the same step twice, even if two processes race —
--   which is the safety property the whole design rests on. Get this right and
--   your Go code needs no locking to be correct.
--
-- Both tables store their payloads as jsonb. Ids are uuid. Timestamps are
-- timestamptz, never timestamp.
--
-- Check your work:  make migrate  &&  psql "$DSN" -c '\d workflows' -c '\d steps'

-- TODO(task 1): CREATE TABLE workflows ...

CREATE TABLE IF NOT EXISTS workflows (
    id uuid PRIMARY KEY,
    type text NOT NULL,
    status text NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    input jsonb NOT NULL DEFAULT 'null'::jsonb,
    output jsonb,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- TODO(task 1): CREATE TABLE steps ...

CREATE TABLE IF NOT EXISTS steps (
    workflow_id uuid NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    seq integer NOT NULL,
    PRIMARY KEY (workflow_id, seq),
    name text NOT NULL,
    result jsonb,
    returned_at timestamptz NOT NULL DEFAULT now()
);


-- Not part of the runtime, and not your job — this is the observation table the
-- crash test uses to prove a step's side effect happened exactly once across a
-- kill and a resume. Real workflows write to their own tables; this one exists
-- to be counted.
CREATE TABLE IF NOT EXISTS side_effects (
    id          bigserial   PRIMARY KEY,
    workflow_id uuid        NOT NULL,
    step_name   text        NOT NULL,
    happened_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS workflows_runnable_idx
    ON workflows (created_at)
    WHERE status IN ('pending', 'running');
