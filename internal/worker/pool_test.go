package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/ayamschikov/task-queue/internal/model"
	"github.com/ayamschikov/task-queue/internal/repository"
	"github.com/ayamschikov/task-queue/internal/worker"
)

var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	code, err := runIntegrationTests(m)
	if err != nil {
		log.Fatal(err)
	}
	os.Exit(code)
}

func runIntegrationTests(m *testing.M) (int, error) {
	ctx := context.Background()
	container, err := postgres.Run(ctx, "postgres:17-alpine",
		postgres.WithDatabase("taskqueue"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategyAndDeadline(60*time.Second,
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
		),
	)
	if err != nil {
		return 0, fmt.Errorf("start postgres: %w", err)
	}
	defer func() { _ = container.Terminate(ctx) }()

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return 0, fmt.Errorf("connection string: %w", err)
	}
	testPool, err = pgxpool.New(ctx, connStr)
	if err != nil {
		return 0, fmt.Errorf("pool: %w", err)
	}
	defer testPool.Close()

	if err := applyMigrations(ctx, testPool); err != nil {
		return 0, fmt.Errorf("migrate: %w", err)
	}
	return m.Run(), nil
}

func applyMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	matches, err := filepath.Glob(filepath.Join("..", "..", "migrations", "*.sql"))
	if err != nil {
		return err
	}
	sort.Strings(matches)
	for _, m := range matches {
		sqlBytes, err := os.ReadFile(m)
		if err != nil {
			return fmt.Errorf("read %s: %w", m, err)
		}
		upSQL := strings.Split(string(sqlBytes), "-- +goose Down")[0]
		if _, err := pool.Exec(ctx, upSQL); err != nil {
			return fmt.Errorf("apply %s: %w", m, err)
		}
	}
	return nil
}

func resetDB(t *testing.T) {
	t.Helper()
	if _, err := testPool.Exec(context.Background(), "TRUNCATE tasks RESTART IDENTITY"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
}

func newRepo() *repository.TaskRepository {
	return repository.NewTaskRepository(testPool)
}

// quickConfig keeps test runtime tight: aggressive poll, near-zero backoff
// so retries fire within the deadline of waitFor().
func quickConfig(size int) worker.Config {
	return worker.Config{
		Size:        size,
		MinPoll:     5 * time.Millisecond,
		MaxPoll:     50 * time.Millisecond,
		BaseBackoff: 10 * time.Millisecond,
		MaxBackoff:  50 * time.Millisecond,
		Logger:      slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	}
}

// waitFor polls the predicate until it returns true or the deadline fires.
// Used instead of arbitrary sleeps so the tests stay fast and deterministic
// when the worker is faster than the timeout, and don't lie when it isn't.
func waitFor(t *testing.T, deadline time.Duration, msg string, ok func() bool) {
	t.Helper()
	start := time.Now()
	for time.Since(start) < deadline {
		if ok() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s after %v", msg, deadline)
}

func TestRun_processesTaskToDone(t *testing.T) {
	resetDB(t)
	repo := newRepo()
	ctx := context.Background()

	var called atomic.Int32
	pool := worker.New(repo, quickConfig(2))
	pool.Register("noop", func(_ context.Context, _ json.RawMessage) error {
		called.Add(1)
		return nil
	})

	tsk, err := repo.Enqueue(ctx, repository.EnqueueParams{Type: "noop", Payload: json.RawMessage(`{}`), MaxRetries: 3})
	if err != nil {
		t.Fatal(err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { pool.Run(runCtx); close(done) }()

	waitFor(t, 2*time.Second, "task to reach status=done", func() bool {
		got, err := repo.FindByID(ctx, tsk.ID)
		return err == nil && got.Status == model.TaskStatusDone
	})

	cancel()
	<-done

	if called.Load() != 1 {
		t.Errorf("handler invocations: got %d want 1", called.Load())
	}
}

func TestRun_failingHandlerSchedulesRetry(t *testing.T) {
	resetDB(t)
	repo := newRepo()
	ctx := context.Background()

	pool := worker.New(repo, quickConfig(1))
	pool.Register("flaky", func(_ context.Context, _ json.RawMessage) error {
		return errors.New("boom")
	})

	tsk, err := repo.Enqueue(ctx, repository.EnqueueParams{Type: "flaky", Payload: json.RawMessage(`{}`), MaxRetries: 5})
	if err != nil {
		t.Fatal(err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { pool.Run(runCtx); close(done) }()

	waitFor(t, 2*time.Second, "attempts to increment", func() bool {
		got, err := repo.FindByID(ctx, tsk.ID)
		return err == nil && got.Attempts >= 1
	})

	cancel()
	<-done

	got, err := repo.FindByID(ctx, tsk.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Attempts < 1 {
		t.Errorf("attempts: got %d want >=1", got.Attempts)
	}
	if got.LastError == nil || *got.LastError != "boom" {
		t.Errorf("last_error: got %v want 'boom'", got.LastError)
	}
}

func TestRun_unknownTypeIsTerminal(t *testing.T) {
	resetDB(t)
	repo := newRepo()
	ctx := context.Background()

	pool := worker.New(repo, quickConfig(1))
	// no handler registered — task type 'mystery' is unknown

	tsk, err := repo.Enqueue(ctx, repository.EnqueueParams{Type: "mystery", Payload: json.RawMessage(`{}`), MaxRetries: 10})
	if err != nil {
		t.Fatal(err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { pool.Run(runCtx); close(done) }()

	waitFor(t, 2*time.Second, "task to reach status=failed (terminal)", func() bool {
		got, err := repo.FindByID(ctx, tsk.ID)
		return err == nil && got.Status == model.TaskStatusFailed
	})

	cancel()
	<-done

	got, _ := repo.FindByID(ctx, tsk.ID)
	if got.Attempts != 1 {
		t.Errorf("attempts: got %d want 1 (terminal on first attempt)", got.Attempts)
	}
	if got.LastError == nil || !strings.Contains(*got.LastError, "no handler") {
		t.Errorf("last_error: got %v want mention of 'no handler'", got.LastError)
	}
}

func TestRun_concurrentWorkersAllProcessed(t *testing.T) {
	resetDB(t)
	repo := newRepo()
	ctx := context.Background()

	const total = 100
	for i := 0; i < total; i++ {
		if _, err := repo.Enqueue(ctx, repository.EnqueueParams{Type: "noop", Payload: json.RawMessage(`{}`), MaxRetries: 3}); err != nil {
			t.Fatal(err)
		}
	}

	var processed atomic.Int32
	pool := worker.New(repo, quickConfig(8))
	pool.Register("noop", func(_ context.Context, _ json.RawMessage) error {
		processed.Add(1)
		return nil
	})

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { pool.Run(runCtx); close(done) }()

	waitFor(t, 5*time.Second, "all 100 tasks processed", func() bool {
		return processed.Load() == total
	})

	cancel()
	<-done

	// No duplicate processing — each task counted exactly once.
	if processed.Load() != total {
		t.Errorf("processed: got %d want %d", processed.Load(), total)
	}

	// Every task is in 'done' state.
	var doneCount, otherCount int
	rows, err := testPool.Query(ctx, `SELECT status, COUNT(*) FROM tasks GROUP BY status`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			t.Fatal(err)
		}
		if status == string(model.TaskStatusDone) {
			doneCount = count
		} else {
			otherCount += count
		}
	}
	if doneCount != total {
		t.Errorf("done count: got %d want %d", doneCount, total)
	}
	if otherCount != 0 {
		t.Errorf("non-done count: got %d want 0", otherCount)
	}
}

func TestRun_gracefulShutdown(t *testing.T) {
	resetDB(t)
	repo := newRepo()

	pool := worker.New(repo, quickConfig(2))
	pool.Register("noop", func(_ context.Context, _ json.RawMessage) error {
		return nil
	})

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		pool.Run(runCtx)
		close(done)
	}()

	// Let the workers spin up and start polling.
	time.Sleep(50 * time.Millisecond)

	cancel()

	// Run must return promptly after cancellation.
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return within 2s after cancel")
	}
}

func TestRun_inFlightHandlerCompletesBeforeShutdown(t *testing.T) {
	resetDB(t)
	repo := newRepo()
	ctx := context.Background()

	started := make(chan struct{})
	finished := make(chan struct{})
	pool := worker.New(repo, quickConfig(1))
	pool.Register("slow", func(handlerCtx context.Context, _ json.RawMessage) error {
		close(started)
		// Run for a bit even after shutdown — handler that ignores ctx,
		// emulating a real in-flight task we want to drain.
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(100 * time.Millisecond)
		}()
		wg.Wait()
		close(finished)
		return nil
	})

	tsk, err := repo.Enqueue(ctx, repository.EnqueueParams{Type: "slow", Payload: json.RawMessage(`{}`), MaxRetries: 3})
	if err != nil {
		t.Fatal(err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { pool.Run(runCtx); close(done) }()

	<-started        // handler is now executing
	cancel()         // shutdown begins
	<-finished       // handler ran to completion despite shutdown
	<-done           // Run returned

	got, err := repo.FindByID(ctx, tsk.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != model.TaskStatusDone {
		t.Errorf("status: got %q want done (handler finished, MarkDone should land)", got.Status)
	}
}
