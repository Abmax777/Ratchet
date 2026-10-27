// Command demo runs a three-step workflow shaped like a real email-processing
// pipeline: fetch, classify with a (fake) LLM call, write a row.
//
// Every step appends to the side_effects table. That table is the evidence: if
// durable replay works, killing the process mid-workflow and running it again
// leaves exactly one row per step, not two.
//
// Stage 1, one process:
//
//	go run ./cmd/demo -crash-after 2     # dies after committing step 2
//	go run ./cmd/demo -resume            # picks up where it stopped
//
// Stage 2, a fleet of workers:
//
//	go run ./cmd/demo -enqueue 30                      # queue 30 runs
//	go run ./cmd/demo -work -worker-id w1 -lease 3s    # start a worker (repeat in other terminals)
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

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
		dsn         = flag.String("dsn", envOr("RATCHET_DSN", "postgres://postgres@localhost:5432/ratchet?sslmode=disable"), "postgres connection string")
		resume      = flag.Bool("resume", false, "run every unfinished workflow instead of starting a new one")
		message     = flag.String("message", "18f2a", "gmail message id to process")
		enqueue     = flag.Int("enqueue", 0, "start N workflows without running them, for workers to pick up")
		work        = flag.Bool("work", false, "run as a worker: claim and execute queued workflows until SIGINT/SIGTERM")
		workerID    = flag.String("worker-id", "", "worker name recorded in workflows.locked_by")
		concurrency = flag.Int("concurrency", 3, "runs a worker executes at once")
		lease       = flag.Duration("lease", 5*time.Second, "lease TTL for a worker's claims")
	)
	flag.IntVar(&crashAfter, "crash-after", 0, "simulate a crash after N steps have committed (0 = never)")
	flag.Parse()

	db, err := sql.Open("pgx", *dsn)
	if err != nil {
		log.Fatalf("opening database: %v", err)
	}
	defer db.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("connecting to database: %v", err)
	}

	appDB = db
	engine := rt.New(pgstore.New(db))
	rt.Register(engine, "process_email", processEmail)

	switch {
	case *enqueue > 0:
		for i := 0; i < *enqueue; i++ {
			id, err := engine.Start(ctx, "process_email", Input{MessageID: fmt.Sprintf("msg-%03d", i)})
			if err != nil {
				log.Fatalf("enqueue: %v", err)
			}
			fmt.Println(id)
		}
		return

	case *work:
		opts := rt.WorkerOptions{
			ID:          *workerID,
			Concurrency: *concurrency,
			LeaseTTL:    *lease,
			OnResult: func(id string, res rt.Result, err error) {
				if err != nil {
					fmt.Printf("[%s] %s %s: %v\n", *workerID, id[:8], res.Status, err)
					return
				}
				fmt.Printf("[%s] %s %s (executed %d, replayed %d)\n", *workerID, id[:8], res.Status, res.Executed, res.Replayed)
			},
			OnError: func(err error) { log.Print(err) },
		}
		fmt.Printf("[%s] working, lease %s, concurrency %d\n", *workerID, *lease, *concurrency)
		if err := engine.Work(ctx, opts); err != nil {
			log.Fatalf("worker: %v", err)
		}
		fmt.Printf("[%s] stopped cleanly\n", *workerID)
		return

	case *resume:
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

// processEmail is the workflow (task 9): three steps, each wrapped in rt.Step.
//
// The function is re-executed from the top on every resume. Each rt.Step that
// already has a journal entry returns the recorded value without running its
// function, so only unfinished work happens again.
//
// The timestamp comes from rt.Now, not time.Now. time.Now would return a new
// value on every replay, so the output would change depending on when the
// resume happened; rt.Now journals the first value and returns it forever.
// It sits after maybeCrash(2) so that a crash after step 2 leaves exactly two
// journal rows, which is what the crash test checks.
func processEmail(c *rt.Context, in Input) (Output, error) {
	msg, err := rt.Step(c, "fetch_email", func() (email, error) {
		record(c, "fetch_email")
		time.Sleep(300 * time.Millisecond) // stands in for the Gmail API
		return email{From: "talent@example.com", Subject: "Your application for message " + in.MessageID}, nil
	})
	if err != nil {
		return Output{}, err
	}
	maybeCrash(1)

	cls, err := rt.Step(c, "classify", func() (classification, error) {
		record(c, "classify")
		time.Sleep(600 * time.Millisecond) // stands in for the LLM call
		return classify(msg), nil
	})
	if err != nil {
		return Output{}, err
	}
	maybeCrash(2)

	at, err := rt.Now(c)
	if err != nil {
		return Output{}, err
	}

	if _, err := rt.Step(c, "write_row", func() (struct{}, error) {
		record(c, "write_row")
		time.Sleep(200 * time.Millisecond) // stands in for the tracker write
		return struct{}{}, nil
	}); err != nil {
		return Output{}, err
	}
	maybeCrash(3)

	return Output{Label: cls.Label, Company: cls.Company, WrittenAt: at.Format(time.RFC3339)}, nil
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
