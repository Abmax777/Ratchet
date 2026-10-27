// Package pgstore is the Postgres implementation of rt.Store.
//
// It talks through database/sql, so it compiles without a driver; the binary
// that uses it blank-imports one (cmd/demo uses pgx).
//
// Most of the guarantees live in SQL rather than Go:
//
//   - PRIMARY KEY (workflow_id, seq) on steps: a step can be journaled once.
//   - Claim uses FOR UPDATE SKIP LOCKED: concurrent workers each get a
//     different run, and nobody waits on anybody else's row lock.
//   - Every write that changes a run's progress is conditioned on
//     epoch = <the epoch the writer claimed with>. A stale worker's write
//     matches zero rows and comes back as rt.ErrLeaseLost.
//
// Timestamps that matter for leases and retries (locked_until,
// next_attempt_at) are computed with the database's now(), never the worker's
// clock, so workers with skewed clocks still agree on who owns what.
package pgstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/abmax777/ratchet/rt"
)

type Store struct{ db *sql.DB }

func New(db *sql.DB) *Store { return &Store{db: db} }

// Compile-time proof that Store satisfies the interface.
var _ rt.Store = (*Store)(nil)

// runnable is the single definition of "a worker may take this run now". The
// leading status filter lets Postgres use the partial claim index.
const runnable = `
	status IN ('pending', 'running')
	AND (
		(status = 'pending' AND next_attempt_at <= now())
		OR (status = 'running' AND (locked_until IS NULL OR locked_until < now()))
	)`

func (s *Store) CreateWorkflow(ctx context.Context, id, workflowType string, input []byte) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO workflows (id, type, status, input) VALUES ($1, $2, 'pending', $3::jsonb)`,
		id, workflowType, string(input))
	if err != nil {
		return fmt.Errorf("creating workflow %s: %w", id, err)
	}
	return nil
}

func (s *Store) LoadWorkflow(ctx context.Context, id string) (rt.WorkflowRow, error) {
	var (
		row           rt.WorkflowRow
		input, output string
	)
	err := s.db.QueryRowContext(ctx, `
		SELECT type, status, input::text, COALESCE(output::text, ''), COALESCE(last_error, ''),
		       epoch, attempts, created_at, updated_at
		FROM workflows WHERE id = $1`, id,
	).Scan(&row.Type, &row.Status, &input, &output, &row.LastError,
		&row.Epoch, &row.Attempts, &row.CreatedAt, &row.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) || isInvalidID(err) {
		return rt.WorkflowRow{}, fmt.Errorf("loading %s: %w", id, rt.ErrWorkflowNotFound)
	}
	if err != nil {
		return rt.WorkflowRow{}, fmt.Errorf("loading %s: %w", id, err)
	}
	row.Input = []byte(input)
	if output != "" {
		row.Output = []byte(output)
	}
	return row, nil
}

func (s *Store) LoadJournal(ctx context.Context, id string) (map[int]rt.JournalEntry, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT seq, name, COALESCE(result::text, 'null') FROM steps WHERE workflow_id = $1 ORDER BY seq`, id)
	if err != nil {
		return nil, fmt.Errorf("loading journal for %s: %w", id, err)
	}
	defer rows.Close()

	journal := make(map[int]rt.JournalEntry)
	for rows.Next() {
		var (
			seq          int
			name, result string
		)
		if err := rows.Scan(&seq, &name, &result); err != nil {
			return nil, fmt.Errorf("reading journal for %s: %w", id, err)
		}
		journal[seq] = rt.JournalEntry{Name: name, Result: []byte(result)}
	}
	return journal, rows.Err()
}

func (s *Store) Acquire(ctx context.Context, id string) (rt.Lease, error) {
	var epoch int64
	err := s.db.QueryRowContext(ctx, `
		UPDATE workflows
		SET status = 'running', epoch = epoch + 1, locked_by = NULL, locked_until = NULL, updated_at = now()
		WHERE id = $1 AND status IN ('pending', 'running')
		RETURNING epoch`, id).Scan(&epoch)
	if errors.Is(err, sql.ErrNoRows) || isInvalidID(err) {
		// Either it does not exist, or it is already terminal.
		if _, lerr := s.LoadWorkflow(ctx, id); lerr != nil {
			return rt.Lease{}, lerr
		}
		return rt.Lease{}, rt.ErrLeaseLost
	}
	if err != nil {
		return rt.Lease{}, fmt.Errorf("acquiring %s: %w", id, err)
	}
	return rt.Lease{WorkflowID: id, Epoch: epoch}, nil
}

// Claim is the heart of stage 2.
//
// The inner SELECT finds the oldest runnable run and locks it FOR UPDATE SKIP
// LOCKED: if another worker is mid-claim on that row, this one skips it and
// takes the next, instead of queueing behind the lock. The outer UPDATE then
// takes the lease and bumps the epoch in the same statement, so there is no
// window between "found it" and "own it".
func (s *Store) Claim(ctx context.Context, owner string, ttl time.Duration) (rt.Lease, bool, error) {
	var (
		id    string
		epoch int64
	)
	err := s.db.QueryRowContext(ctx, `
		UPDATE workflows w
		SET status = 'running',
		    locked_by = $1,
		    locked_until = now() + ($2::bigint * interval '1 millisecond'),
		    epoch = w.epoch + 1,
		    updated_at = now()
		FROM (
			SELECT id FROM workflows
			WHERE `+runnable+`
			ORDER BY created_at
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		) c
		WHERE w.id = c.id
		RETURNING w.id::text, w.epoch`, owner, ttl.Milliseconds(),
	).Scan(&id, &epoch)
	if errors.Is(err, sql.ErrNoRows) {
		return rt.Lease{}, false, nil
	}
	if err != nil {
		return rt.Lease{}, false, fmt.Errorf("claiming: %w", err)
	}
	return rt.Lease{WorkflowID: id, Epoch: epoch, Owner: owner}, true, nil
}

func (s *Store) Heartbeat(ctx context.Context, l rt.Lease, ttl time.Duration) error {
	return s.fenced(ctx, l, "heartbeat", `
		UPDATE workflows
		SET locked_until = now() + ($3::bigint * interval '1 millisecond')
		WHERE id = $1 AND epoch = $2 AND status = 'running'`,
		l.WorkflowID, l.Epoch, ttl.Milliseconds())
}

// CommitStep journals one step, fenced.
//
// INSERT ... SELECT ... FOR SHARE: the step row is only inserted if the run
// still has our epoch, and FOR SHARE holds that run row until we commit, so a
// concurrent Claim (which needs FOR UPDATE on it) cannot bump the epoch between
// our check and our insert. If a claim got there first, Postgres re-checks our
// WHERE against the newest row version, sees the new epoch, and inserts
// nothing.
//
// A unique violation is deliberately NOT absorbed with ON CONFLICT DO NOTHING.
// It means two runners executed the same step, and that must be loud.
func (s *Store) CommitStep(ctx context.Context, l rt.Lease, seq int, name string, result []byte) error {
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO steps (workflow_id, seq, name, result)
		SELECT id, $3, $4, $5::jsonb
		FROM workflows
		WHERE id = $1 AND epoch = $2 AND status = 'running'
		FOR SHARE`,
		l.WorkflowID, l.Epoch, seq, name, string(result))
	if isUniqueViolation(err) {
		return fmt.Errorf("committing step %d of %s: %w", seq, l.WorkflowID, rt.ErrStepAlreadyCommitted)
	}
	if err != nil {
		return fmt.Errorf("committing step %d of %s: %w", seq, l.WorkflowID, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("committing step %d of %s: %w", seq, l.WorkflowID, rt.ErrLeaseLost)
	}
	return nil
}

func (s *Store) Complete(ctx context.Context, l rt.Lease, output []byte) error {
	return s.fenced(ctx, l, "completing", `
		UPDATE workflows
		SET status = 'completed', output = $3::jsonb, locked_by = NULL, locked_until = NULL, updated_at = now()
		WHERE id = $1 AND epoch = $2 AND status = 'running'`,
		l.WorkflowID, l.Epoch, string(output))
}

func (s *Store) Fail(ctx context.Context, l rt.Lease, lastErr string) error {
	return s.fenced(ctx, l, "failing", `
		UPDATE workflows
		SET status = 'failed', last_error = $3, locked_by = NULL, locked_until = NULL, updated_at = now()
		WHERE id = $1 AND epoch = $2 AND status = 'running'`,
		l.WorkflowID, l.Epoch, lastErr)
}

func (s *Store) Release(ctx context.Context, l rt.Lease, lastErr string, delay time.Duration, attempts int) error {
	return s.fenced(ctx, l, "releasing", `
		UPDATE workflows
		SET status = 'pending',
		    last_error = $3,
		    next_attempt_at = now() + ($4::bigint * interval '1 millisecond'),
		    attempts = $5,
		    locked_by = NULL, locked_until = NULL, updated_at = now()
		WHERE id = $1 AND epoch = $2 AND status = 'running'`,
		l.WorkflowID, l.Epoch, lastErr, delay.Milliseconds(), attempts)
}

func (s *Store) Retry(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE workflows
		SET status = 'pending', attempts = 0, next_attempt_at = now(), updated_at = now()
		WHERE id = $1 AND status = 'failed'`, id)
	if isInvalidID(err) {
		return fmt.Errorf("retrying %s: %w", id, rt.ErrWorkflowNotFound)
	}
	if err != nil {
		return fmt.Errorf("retrying %s: %w", id, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		if _, lerr := s.LoadWorkflow(ctx, id); lerr != nil {
			return lerr
		}
	}
	return nil
}

func (s *Store) Runnable(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id::text FROM workflows WHERE `+runnable+` ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("listing runnable workflows: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// fenced runs an UPDATE conditioned on the caller's epoch and turns "zero rows
// matched" into rt.ErrLeaseLost.
func (s *Store) fenced(ctx context.Context, l rt.Lease, what, query string, args ...any) error {
	res, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("%s %s: %w", what, l.WorkflowID, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("%s %s: %w", what, l.WorkflowID, rt.ErrLeaseLost)
	}
	return nil
}

// sqlState extracts a Postgres SQLSTATE code through an interface, so this
// package works with pgx and lib/pq alike without importing either.
func sqlState(err error) string {
	var coded interface{ SQLState() string }
	if errors.As(err, &coded) {
		return coded.SQLState()
	}
	return ""
}

// isUniqueViolation reports whether err is SQLSTATE 23505.
func isUniqueViolation(err error) bool { return sqlState(err) == "23505" }

// isInvalidID reports whether err is SQLSTATE 22P02 (invalid text
// representation): an id that is not a uuid at all, which cannot exist.
func isInvalidID(err error) bool { return sqlState(err) == "22P02" }
