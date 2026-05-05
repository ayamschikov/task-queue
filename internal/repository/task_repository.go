package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ayamschikov/task-queue/internal/model"
)

var ErrNotFound = errors.New("task not found")

type TaskRepository struct {
	pool *pgxpool.Pool
}

func NewTaskRepository(pool *pgxpool.Pool) *TaskRepository {
	return &TaskRepository{pool: pool}
}

type EnqueueParams struct {
	Type       string
	Payload    json.RawMessage
	Priority   int
	MaxRetries int
}

func (r *TaskRepository) Enqueue(ctx context.Context, p EnqueueParams) (*model.Task, error) {
	row := r.pool.QueryRow(ctx, `
		INSERT INTO tasks (type, payload, priority, max_retries)
		VALUES ($1, $2, $3, $4)
		RETURNING id, type, payload, status, priority, attempts, max_retries, last_error, created_at, updated_at, scheduled_at`,
		p.Type, p.Payload, p.Priority, p.MaxRetries,
	)
	t, err := scanTask(row)
	if err != nil {
		return nil, fmt.Errorf("enqueue: %w", err)
	}
	return t, nil
}

// PickPending atomically claims up to `limit` tasks ready to run and flips them
// to 'running'. The CTE with FOR UPDATE SKIP LOCKED is the heart of the queue:
// concurrent workers running this query will each get their own disjoint set
// without contention, and crashed/slow tx are stepped over instead of blocking.
func (r *TaskRepository) PickPending(ctx context.Context, limit int) ([]*model.Task, error) {
	rows, err := r.pool.Query(ctx, `
		WITH picked AS (
			SELECT id FROM tasks
			WHERE status = 'pending' AND scheduled_at <= NOW()
			ORDER BY priority DESC, scheduled_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE tasks t
		SET status = 'running', updated_at = NOW()
		FROM picked
		WHERE t.id = picked.id
		RETURNING t.id, t.type, t.payload, t.status, t.priority, t.attempts, t.max_retries, t.last_error, t.created_at, t.updated_at, t.scheduled_at`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("pick pending: %w", err)
	}
	defer rows.Close()

	var tasks []*model.Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, fmt.Errorf("pick pending scan: %w", err)
		}
		tasks = append(tasks, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pick pending rows: %w", err)
	}
	return tasks, nil
}

func (r *TaskRepository) MarkDone(ctx context.Context, id int) error {
	cmd, err := r.pool.Exec(ctx, `
		UPDATE tasks
		SET status = 'done', updated_at = NOW()
		WHERE id = $1
	`, id)
	if err != nil {
		return fmt.Errorf("mark done: %w", err)
	}
	if cmd.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// MarkFailed increments attempts and either schedules a retry or moves the task
// to the terminal 'failed' state if max_retries is exceeded. The decision is
// made in SQL (single round-trip, no race with concurrent reads).
func (r *TaskRepository) MarkFailed(ctx context.Context, id int, errMsg string, backoff time.Duration) error {
	nextRun := time.Now().Add(backoff)
	cmd, err := r.pool.Exec(ctx, `
		UPDATE tasks
		SET attempts = attempts + 1,
		    last_error = $2,
		    status = CASE
		        WHEN attempts + 1 >= max_retries THEN 'failed'
		        ELSE 'pending'
		    END,
		    scheduled_at = CASE
		        WHEN attempts + 1 >= max_retries THEN scheduled_at
		        ELSE $3
		    END,
		    updated_at = NOW()
		WHERE id = $1
	`, id, errMsg, nextRun)
	if err != nil {
		return fmt.Errorf("mark failed: %w", err)
	}
	if cmd.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *TaskRepository) FindByID(ctx context.Context, id int) (*model.Task, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT id, type, payload, status, priority, attempts, max_retries, last_error, created_at, updated_at, scheduled_at
		FROM tasks WHERE id = $1`, id)
	t, err := scanTask(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("find by id: %w", err)
	}
	return t, nil
}

func scanTask(row pgx.Row) (*model.Task, error) {
	var t model.Task
	if err := row.Scan(
		&t.ID, &t.Type, &t.Payload, &t.Status, &t.Priority,
		&t.Attempts, &t.MaxRetries, &t.LastError,
		&t.CreatedAt, &t.UpdatedAt, &t.ScheduledAt,
	); err != nil {
		return nil, err
	}
	return &t, nil
}

