package worker_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayamschikov/task-queue/internal/model"
	"github.com/ayamschikov/task-queue/internal/worker"
)

type stubRecoverer struct {
	calls atomic.Int32
	n     int
	err   error
}

func (s *stubRecoverer) RecoverStuck(_ context.Context, _ time.Duration) (int, error) {
	s.calls.Add(1)
	return s.n, s.err
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestSweeper_recoversStuckTaskEndToEnd(t *testing.T) {
	resetDB(t)
	repo := newRepo()
	ctx := context.Background()

	// Insert a row that looks like a worker died mid-flight 10 minutes ago.
	if _, err := testPool.Exec(ctx, `
		INSERT INTO tasks (type, payload, status, picked_at, max_retries)
		VALUES ($1, $2, 'running', NOW() - INTERVAL '10 minutes', 5)
	`, "x", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}

	sweeper := worker.NewSweeper(repo, worker.SweepConfig{
		Interval:  20 * time.Millisecond,
		Threshold: 1 * time.Minute,
		Logger:    quietLogger(),
	})

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { sweeper.Run(runCtx); close(done) }()

	waitFor(t, 2*time.Second, "stuck task to be recovered to pending", func() bool {
		var status string
		err := testPool.QueryRow(ctx, `SELECT status FROM tasks LIMIT 1`).Scan(&status)
		return err == nil && status == string(model.TaskStatusPending)
	})

	cancel()
	<-done
}

func TestSweeper_exitsOnCtxCancel(t *testing.T) {
	stub := &stubRecoverer{}
	sweeper := worker.NewSweeper(stub, worker.SweepConfig{
		Interval:  10 * time.Millisecond,
		Threshold: time.Minute,
		Logger:    quietLogger(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { sweeper.Run(ctx); close(done) }()

	// Let it tick at least once.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("sweeper did not exit within 1s after cancel")
	}

	if stub.calls.Load() == 0 {
		t.Error("expected sweeper to have ticked at least once")
	}
}

func TestSweeper_continuesAfterRecoverError(t *testing.T) {
	stub := &stubRecoverer{err: errors.New("transient db hiccup")}
	sweeper := worker.NewSweeper(stub, worker.SweepConfig{
		Interval:  10 * time.Millisecond,
		Threshold: time.Minute,
		Logger:    quietLogger(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { sweeper.Run(ctx); close(done) }()

	// Several ticks despite errors.
	time.Sleep(60 * time.Millisecond)
	cancel()
	<-done

	if stub.calls.Load() < 2 {
		t.Errorf("expected multiple ticks despite errors, got %d", stub.calls.Load())
	}
}
