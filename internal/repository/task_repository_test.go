package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/ayamschikov/task-queue/internal/model"
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
		// Take everything before the Down marker so we don't undo what we just applied.
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

func TestEnqueue(t *testing.T) {
	resetDB(t)
	repo := NewTaskRepository(testPool)
	ctx := context.Background()

	task, err := repo.Enqueue(ctx, EnqueueParams{
		Type:       "send_email",
		Payload:    json.RawMessage(`{"to":"a@b.com"}`),
		Priority:   5,
		MaxRetries: 3,
	})
	if err != nil {
		t.Fatal(err)
	}

	if task.ID == 0 {
		t.Error("expected ID > 0")
	}
	if task.Status != model.TaskStatusPending {
		t.Errorf("status: got %q want pending", task.Status)
	}
	if task.Type != "send_email" {
		t.Errorf("type: got %q", task.Type)
	}
	if task.Priority != 5 {
		t.Errorf("priority: got %d", task.Priority)
	}
	if task.Attempts != 0 {
		t.Errorf("attempts: got %d want 0", task.Attempts)
	}
	if task.LastError != nil {
		t.Errorf("last_error: got %v want nil", task.LastError)
	}
}

func TestPickPending_flipsToRunning(t *testing.T) {
	resetDB(t)
	repo := NewTaskRepository(testPool)
	ctx := context.Background()

	if _, err := repo.Enqueue(ctx, EnqueueParams{Type: "x", Payload: json.RawMessage(`{}`), MaxRetries: 3}); err != nil {
		t.Fatal(err)
	}

	tasks, err := repo.PickPending(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 {
		t.Fatalf("len: got %d want 1", len(tasks))
	}
	if tasks[0].Status != model.TaskStatusRunning {
		t.Errorf("status: got %q want running", tasks[0].Status)
	}
	// picked_at must be set; tolerate clock skew between host and container.
	if tasks[0].PickedAt == nil {
		t.Error("picked_at: got nil want set")
	} else if elapsed := time.Since(*tasks[0].PickedAt); elapsed < -5*time.Second || elapsed > 30*time.Second {
		t.Errorf("picked_at: got %v, expected within recent window (elapsed=%v)", tasks[0].PickedAt, elapsed)
	}
}

func TestPickPending_skipsAlreadyRunning(t *testing.T) {
	resetDB(t)
	repo := NewTaskRepository(testPool)
	ctx := context.Background()

	if _, err := repo.Enqueue(ctx, EnqueueParams{Type: "x", Payload: json.RawMessage(`{}`), MaxRetries: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.PickPending(ctx, 10); err != nil {
		t.Fatal(err)
	}

	tasks, err := repo.PickPending(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 0 {
		t.Errorf("len: got %d want 0 (already running)", len(tasks))
	}
}

func TestPickPending_skipsFutureScheduled(t *testing.T) {
	resetDB(t)
	repo := NewTaskRepository(testPool)
	ctx := context.Background()

	_, err := testPool.Exec(ctx,
		`INSERT INTO tasks (type, payload, scheduled_at) VALUES ($1, $2, NOW() + INTERVAL '1 hour')`,
		"later", []byte(`{}`),
	)
	if err != nil {
		t.Fatal(err)
	}

	tasks, err := repo.PickPending(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 0 {
		t.Errorf("len: got %d want 0 (scheduled in future)", len(tasks))
	}
}

func TestPickPending_orderByPriorityDesc(t *testing.T) {
	resetDB(t)
	repo := NewTaskRepository(testPool)
	ctx := context.Background()

	if _, err := repo.Enqueue(ctx, EnqueueParams{Type: "low", Payload: json.RawMessage(`{}`), Priority: 1, MaxRetries: 3}); err != nil {
		t.Fatal(err)
	}
	high, err := repo.Enqueue(ctx, EnqueueParams{Type: "high", Payload: json.RawMessage(`{}`), Priority: 10, MaxRetries: 3})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Enqueue(ctx, EnqueueParams{Type: "mid", Payload: json.RawMessage(`{}`), Priority: 5, MaxRetries: 3}); err != nil {
		t.Fatal(err)
	}

	tasks, err := repo.PickPending(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 3 {
		t.Fatalf("len: got %d want 3", len(tasks))
	}
	if tasks[0].ID != high.ID {
		t.Errorf("first task: got id=%d want %d (highest priority)", tasks[0].ID, high.ID)
	}
}

// TestPickPending_concurrentSkipLocked is the load-bearing test for the queue.
// 10 workers each try to claim 5 tasks out of 50 — every task must be picked
// by exactly one worker and the total must add up. If FOR UPDATE SKIP LOCKED
// is missing or the CTE is wrong, this test will surface duplicates or losses.
func TestPickPending_concurrentSkipLocked(t *testing.T) {
	resetDB(t)
	repo := NewTaskRepository(testPool)
	ctx := context.Background()

	const tasks = 50
	for range tasks {
		if _, err := repo.Enqueue(ctx, EnqueueParams{Type: "x", Payload: json.RawMessage(`{}`), MaxRetries: 3}); err != nil {
			t.Fatal(err)
		}
	}

	const workers = 10
	results := make([][]int, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			picked, err := repo.PickPending(ctx, 5)
			if err != nil {
				t.Errorf("worker %d: %v", idx, err)
				return
			}
			ids := make([]int, len(picked))
			for j, p := range picked {
				ids[j] = p.ID
			}
			results[idx] = ids
		}(i)
	}
	wg.Wait()

	seen := make(map[int]int)
	total := 0
	for w, ids := range results {
		total += len(ids)
		for _, id := range ids {
			if prev, dup := seen[id]; dup {
				t.Errorf("duplicate pick: id=%d seen by worker %d and %d", id, prev, w)
			}
			seen[id] = w
		}
	}
	if total != tasks {
		t.Errorf("total picks: got %d want %d", total, tasks)
	}
}

func TestMarkDone(t *testing.T) {
	resetDB(t)
	repo := NewTaskRepository(testPool)
	ctx := context.Background()

	tsk, err := repo.Enqueue(ctx, EnqueueParams{Type: "x", Payload: json.RawMessage(`{}`), MaxRetries: 3})
	if err != nil {
		t.Fatal(err)
	}

	if err := repo.MarkDone(ctx, tsk.ID); err != nil {
		t.Fatal(err)
	}

	got, err := repo.FindByID(ctx, tsk.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != model.TaskStatusDone {
		t.Errorf("status: got %q want done", got.Status)
	}
}

func TestMarkDone_notFound(t *testing.T) {
	resetDB(t)
	repo := NewTaskRepository(testPool)

	err := repo.MarkDone(context.Background(), 999_999)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("err: got %v want ErrNotFound", err)
	}
}

func TestMarkFailed_retry(t *testing.T) {
	resetDB(t)
	repo := NewTaskRepository(testPool)
	ctx := context.Background()

	tsk, err := repo.Enqueue(ctx, EnqueueParams{Type: "x", Payload: json.RawMessage(`{}`), MaxRetries: 3})
	if err != nil {
		t.Fatal(err)
	}

	const backoff = 5 * time.Minute
	if err := repo.MarkFailed(ctx, tsk.ID, "boom", backoff); err != nil {
		t.Fatal(err)
	}

	got, err := repo.FindByID(ctx, tsk.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != model.TaskStatusPending {
		t.Errorf("status: got %q want pending (retry)", got.Status)
	}
	if got.Attempts != 1 {
		t.Errorf("attempts: got %d want 1", got.Attempts)
	}
	if got.LastError == nil || *got.LastError != "boom" {
		t.Errorf("last_error: got %v want 'boom'", got.LastError)
	}
	// Allow a few seconds of clock slack — backoff was applied client-side.
	if got.ScheduledAt.Before(time.Now().Add(backoff - 30*time.Second)) {
		t.Errorf("scheduled_at: got %v, expected ~%v in the future", got.ScheduledAt, backoff)
	}
}

func TestMarkFailed_terminalAfterMaxRetries(t *testing.T) {
	resetDB(t)
	repo := NewTaskRepository(testPool)
	ctx := context.Background()

	tsk, err := repo.Enqueue(ctx, EnqueueParams{Type: "x", Payload: json.RawMessage(`{}`), MaxRetries: 1})
	if err != nil {
		t.Fatal(err)
	}

	if err := repo.MarkFailed(ctx, tsk.ID, "boom", time.Minute); err != nil {
		t.Fatal(err)
	}

	got, err := repo.FindByID(ctx, tsk.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != model.TaskStatusFailed {
		t.Errorf("status: got %q want failed (DLQ)", got.Status)
	}
	if got.Attempts != 1 {
		t.Errorf("attempts: got %d want 1", got.Attempts)
	}
}

func TestMarkFailed_notFound(t *testing.T) {
	resetDB(t)
	repo := NewTaskRepository(testPool)

	err := repo.MarkFailed(context.Background(), 999_999, "boom", time.Minute)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("err: got %v want ErrNotFound", err)
	}
}

func TestMarkFailedTerminal(t *testing.T) {
	resetDB(t)
	repo := NewTaskRepository(testPool)
	ctx := context.Background()

	// MaxRetries deliberately high — terminal must override retry semantics.
	tsk, err := repo.Enqueue(ctx, EnqueueParams{Type: "x", Payload: json.RawMessage(`{}`), MaxRetries: 10})
	if err != nil {
		t.Fatal(err)
	}

	if err := repo.MarkFailedTerminal(ctx, tsk.ID, "no handler"); err != nil {
		t.Fatal(err)
	}

	got, err := repo.FindByID(ctx, tsk.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != model.TaskStatusFailed {
		t.Errorf("status: got %q want failed", got.Status)
	}
	if got.LastError == nil || *got.LastError != "no handler" {
		t.Errorf("last_error: got %v want 'no handler'", got.LastError)
	}
	if got.Attempts != 1 {
		t.Errorf("attempts: got %d want 1", got.Attempts)
	}
}

func TestMarkFailedTerminal_notFound(t *testing.T) {
	resetDB(t)
	repo := NewTaskRepository(testPool)

	err := repo.MarkFailedTerminal(context.Background(), 999_999, "no handler")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("err: got %v want ErrNotFound", err)
	}
}

func TestRecoverStuck_returnsToPending(t *testing.T) {
	resetDB(t)
	repo := NewTaskRepository(testPool)
	ctx := context.Background()

	// Insert directly with picked_at in the past so the row looks stuck.
	_, err := testPool.Exec(ctx, `
		INSERT INTO tasks (type, payload, status, picked_at, max_retries)
		VALUES ($1, $2, 'running', NOW() - INTERVAL '10 minutes', 5)
	`, "x", []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}

	n, err := repo.RecoverStuck(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("recovered: got %d want 1", n)
	}

	got, err := selectFirst(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != model.TaskStatusPending {
		t.Errorf("status: got %q want pending", got.Status)
	}
	if got.Attempts != 1 {
		t.Errorf("attempts: got %d want 1", got.Attempts)
	}
	if got.PickedAt != nil {
		t.Errorf("picked_at: got %v want nil (cleared on recovery)", got.PickedAt)
	}
	if got.LastError == nil || *got.LastError != "stuck task recovery" {
		t.Errorf("last_error: got %v want 'stuck task recovery'", got.LastError)
	}
}

func TestRecoverStuck_terminalAfterMaxRetries(t *testing.T) {
	resetDB(t)
	repo := NewTaskRepository(testPool)
	ctx := context.Background()

	// attempts=2, max_retries=3 → next recovery hits the cap and goes terminal.
	_, err := testPool.Exec(ctx, `
		INSERT INTO tasks (type, payload, status, picked_at, attempts, max_retries)
		VALUES ($1, $2, 'running', NOW() - INTERVAL '10 minutes', 2, 3)
	`, "x", []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := repo.RecoverStuck(ctx, time.Minute); err != nil {
		t.Fatal(err)
	}

	got, err := selectFirst(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != model.TaskStatusFailed {
		t.Errorf("status: got %q want failed (DLQ)", got.Status)
	}
	if got.Attempts != 3 {
		t.Errorf("attempts: got %d want 3", got.Attempts)
	}
}

func TestRecoverStuck_skipsRecentlyPicked(t *testing.T) {
	resetDB(t)
	repo := NewTaskRepository(testPool)
	ctx := context.Background()

	// picked_at = NOW() — well within threshold.
	tsk, err := repo.Enqueue(ctx, EnqueueParams{Type: "x", Payload: json.RawMessage(`{}`), MaxRetries: 3})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.PickPending(ctx, 1); err != nil {
		t.Fatal(err)
	}

	n, err := repo.RecoverStuck(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("recovered: got %d want 0 (recent picks should not be recovered)", n)
	}

	got, err := repo.FindByID(ctx, tsk.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != model.TaskStatusRunning {
		t.Errorf("status: got %q want running (untouched)", got.Status)
	}
}

func TestRecoverStuck_skipsNonRunning(t *testing.T) {
	resetDB(t)
	repo := NewTaskRepository(testPool)
	ctx := context.Background()

	// pending row with old picked_at (e.g., previously processed and re-queued).
	// Should not be recovered because status != 'running'.
	_, err := testPool.Exec(ctx, `
		INSERT INTO tasks (type, payload, status, picked_at)
		VALUES ($1, $2, 'pending', NOW() - INTERVAL '10 minutes')
	`, "x", []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}

	n, err := repo.RecoverStuck(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("recovered: got %d want 0", n)
	}
}

// selectFirst returns the only row in tasks for a single-row test setup.
func selectFirst(ctx context.Context) (*model.Task, error) {
	row := testPool.QueryRow(ctx, `
		SELECT id, type, payload, status, priority, attempts, max_retries, last_error, created_at, updated_at, scheduled_at, picked_at
		FROM tasks ORDER BY id LIMIT 1`)
	return scanTask(row)
}

func TestFindByID_notFound(t *testing.T) {
	resetDB(t)
	repo := NewTaskRepository(testPool)

	_, err := repo.FindByID(context.Background(), 999_999)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("err: got %v want ErrNotFound", err)
	}
}
