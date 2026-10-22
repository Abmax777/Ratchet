// Package pgstore is the Postgres implementation of rt.Store.
//
// It talks through database/sql, so it compiles without a driver; the binary
// that uses it blank-imports one (cmd/demo uses pgx).
//
// The whole durability story is already declared for you in
// migrations/001_init.sql. Read that file before writing a line here — the
// primary key on steps (workflow_id, seq) and the CHECK on workflows.status are
// doing work your Go code then does not have to.
//
// See TUTORIAL part 6, and LEARNING.md task 8.
package pgstore

import (
	"database/sql"
	"errors"

	"github.com/abmax777/ratchet/rt"
)

type Store struct{ db *sql.DB }

func New(db *sql.DB) *Store { return &Store{db: db} }

// Compile-time proof that Store satisfies the interface. Leave this here: it is
// how you find out you got a signature wrong at build time rather than at 2am.
var _ rt.Store = (*Store)(nil)

// TODO(task 8): implement the six Store methods against Postgres.
//
// CONTRACT — identical to MemStore's, which is the point: the same tests should
// pass against either. Three places where the SQL has opinions worth forming:
//
//   - LoadWorkflow must translate sql.ErrNoRows into rt.ErrWorkflowNotFound,
//     wrapped so errors.Is still works. Callers should never see a
//     database/sql sentinel leak out of this package.
//
//   - SetStatus is called several times per run, and only sometimes carries an
//     output. Make sure a later status write cannot blank an output an earlier
//     one stored. (COALESCE is your friend. Test it — it is easy to get
//     backwards.)
//
//   - CommitStep must map a unique-violation into rt.ErrStepAlreadyCommitted.
//     Resist the urge to write ON CONFLICT DO NOTHING: with one runner a
//     conflict is impossible, and once stage 2 adds workers it means two of
//     them executed the same workflow at once. That should be loud, not
//     silently absorbed.
//
// Work out for yourself what Runnable's WHERE clause has to include, and why
// 'pending' alone is not enough. (Hint: what status does a SIGKILLed run leave
// behind?)

// isUniqueViolation reports whether err is Postgres SQLSTATE 23505.
//
// Left implemented — it is a driver-portability trick rather than a lesson.
// Matching through an interface rather than a concrete driver type keeps this
// package working with pgx and lib/pq alike.
func isUniqueViolation(err error) bool {
	var coded interface{ SQLState() string }
	if errors.As(err, &coded) {
		return coded.SQLState() == "23505"
	}
	return false
}

var _ = isUniqueViolation
