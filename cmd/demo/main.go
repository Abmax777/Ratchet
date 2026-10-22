// Command demo runs a three-step workflow shaped like a real email-processing
// pipeline: fetch, classify with a (fake) LLM call, write a row.
//
// Every step appends to the side_effects table. That table is the evidence: if
// durable replay works, killing the process mid-workflow and running it again
// leaves exactly one row per step, not two.
//
//	go run ./cmd/demo -crash-after 2     # dies after committing step 2
//	go run ./cmd/demo -resume            # picks up where it stopped
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"


	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/abmax777/ratchet/rt"
	"github.com/abmax777/ratchet/store/pgstore"
)

type Input struct {
	MessageID string `json:"message_id"`
}

type Output struct {
	Label     string `json:"label"`
	Company   string `json:"company"`
	WrittenAt string `json:"written_at"`
}

type email struct {
	From    string `json:"from"`
	Subject string `json:"subject"`
}

type classification struct {
	Label   string `json:"label"`
	Company string `json:"company"`
}

// appDB is the *application's* database handle. The workflow's side effects go
// here, not through the runtime — which is exactly how a real workflow behaves,
// and why "exactly once" has to be reasoned about rather than assumed.
var appDB *sql.DB

// crashAfter is the number of steps to let complete before the process dies.
// Zero means never. This is how the crash test gets a deterministic kill point
// without racing a sleep.
var crashAfter int

func main() {
	var (
		dsn     = flag.String("dsn", envOr("RATCHET_DSN", "postgres://postgres@localhost:5432/ratchet?sslmode=disable"), "postgres connection string")
		resume  = flag.Bool("resume", false, "run every unfinished workflow instead of starting a new one")
		message = flag.String("message", "18f2a", "gmail message id to process")
	)
	flag.IntVar(&crashAfter, "crash-after", 0, "simulate a crash after N steps have committed (0 = never)")
	flag.Parse()

	db, err := sql.Open("pgx", *dsn)
	if err != nil {
		log.Fatalf("opening database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("connecting to database: %v", err)
	}

	appDB = db
	engine := rt.New(pgstore.New(db))
	rt.Register(engine, "process_email", processEmail)

	if *resume {
		ids, err := engine.RunPending(ctx)
		if err != nil {
			log.Fatalf("resume: %v", err)
		}
		if len(ids) == 0 {
			fmt.Println("nothing to resume")
			return
		}
		for _, id := range ids {
			fmt.Printf("resumed and completed %s\n", id)
		}
		return
	}

	id, err := engine.Start(ctx, "process_email", Input{MessageID: *message})
	if err != nil {
		log.Fatalf("start: %v", err)
	}
	fmt.Printf("workflow %s started\n", id)

	res, err := engine.Run(ctx, id)
	if err != nil {
		log.Fatalf("run: %v", err)
	}
	fmt.Printf("workflow %s %s  (executed %d, replayed %d)\n", id, res.Status, res.Executed, res.Replayed)
}

// TASK 9 — write the workflow.
//
// This is the user-facing side of the runtime: what it feels like to USE the
// thing you built. Three steps, in this order, each wrapped in rt.Step:
//
//	fetch_email  -> call record(c, "fetch_email"), sleep ~300ms, return an email
//	classify     -> call record(c, "classify"), sleep ~600ms, return classify(msg)
//	write_row    -> call record(c, "write_row"), sleep ~200ms, return struct{}{}
//
// Between them, call maybeCrash(1), maybeCrash(2) and maybeCrash(3) — that is
// what lets the crash test kill the process at a chosen point.
//
// Somewhere before write_row, get the timestamp for the Output. Use rt.Now, not
// time.Now, and make sure you understand the difference before you type it: one
// of them makes this function replay identically and the other quietly breaks
// it. The names in the rt.Step calls must match the ones above, because
// scripts/crash_test.sh reads them back out of the journal.
//
// The sleeps are not decoration. They stand in for a real API call and a real
// LLM call, and they make the crash test's timing legible when you watch it run.
//
// You will need to add imports as you go — "time" for the sleeps, for a start.
//
// See TUTORIAL part 7, and LEARNING.md task 9.
func processEmail(c *rt.Context, in Input) (Output, error) {
	panic("TODO(task 9): write the workflow — see LEARNING.md")
}

// record appends to the observation table and logs, so the crash test can count
// side effects and a human can watch what happened.
func record(c *rt.Context, step string) {
	fmt.Printf("  executing %-12s (side effect)\n", step)
	// Deliberately outside any transaction the runtime controls: this is what a
	// real external side effect looks like from the runtime's point of view.
	if _, err := appDB.ExecContext(c.Ctx(),
		`INSERT INTO side_effects (workflow_id, step_name) VALUES ($1, $2)`,
		c.WorkflowID(), step); err != nil {
		log.Printf("recording side effect: %v", err)
	}
}

func maybeCrash(afterStep int) {
	if crashAfter > 0 && afterStep == crashAfter {
		fmt.Printf("  *** simulated crash after step %d ***\n", afterStep)
		os.Exit(1)
	}
}

// classify stands in for the LLM call. In the real pipeline this is the part
// that is slow, costs money, and returns something different every time you ask
// — which is precisely why its output must be journaled rather than recomputed.
func classify(m email) classification {
	if strings.Contains(strings.ToLower(m.Subject), "unfortunately") {
		return classification{Label: "rejected", Company: "Example Corp"}
	}
	return classification{Label: "acknowledged", Company: "Example Corp"}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
