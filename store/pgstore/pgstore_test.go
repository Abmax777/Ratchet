package pgstore_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/abmax777/ratchet/rt"
	"github.com/abmax777/ratchet/rt/storetest"
	"github.com/abmax777/ratchet/store/pgstore"
)

// Runs the same conformance suite as MemStore against a real Postgres.
//
//	RATCHET_TEST_DSN=postgres://postgres@localhost:5432/ratchet?sslmode=disable go test ./store/...
//
// Skipped when RATCHET_TEST_DSN is unset. It TRUNCATES the tables: point it at
// a scratch database.
func TestPgStoreConformance(t *testing.T) {
	dsn := os.Getenv("RATCHET_TEST_DSN")
	if dsn == "" {
		t.Skip("RATCHET_TEST_DSN not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("connecting: %v", err)
	}

	storetest.Run(t, func(t *testing.T) rt.Store {
		if _, err := db.Exec(`TRUNCATE steps, workflows, side_effects RESTART IDENTITY CASCADE`); err != nil {
			t.Fatalf("truncating: %v", err)
		}
		return pgstore.New(db)
	})
}
