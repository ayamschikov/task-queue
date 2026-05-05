package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/ayamschikov/task-queue/internal/model"
)

type Handler func(ctx context.Context, payload json.RawMessage) error

type TaskQueue interface {
	PickPending(ctx context.Context, limit int) ([]*model.Task, error)
	MarkDone(ctx context.Context, id int) error
	MarkFailed(ctx context.Context, id int, errMsg string, backoff time.Duration) error
	MarkFailedTerminal(ctx context.Context, id int, errMsg string) error
}

type Config struct {
	Size        int
	MinPoll     time.Duration
	MaxPoll     time.Duration
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
	Logger      *slog.Logger
}

const (
	defaultSize        = 4
	defaultMinPoll     = 100 * time.Millisecond
	defaultMaxPoll     = 5 * time.Second
	defaultBaseBackoff = 10 * time.Second
	defaultMaxBackoff  = 5 * time.Minute
)

func (c *Config) applyDefaults() {
	if c.Size <= 0 {
		c.Size = defaultSize
	}
	if c.MinPoll <= 0 {
		c.MinPoll = defaultMinPoll
	}
	if c.MaxPoll <= 0 {
		c.MaxPoll = defaultMaxPoll
	}
	if c.BaseBackoff <= 0 {
		c.BaseBackoff = defaultBaseBackoff
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = defaultMaxBackoff
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

type Pool struct {
	repo     TaskQueue
	handlers map[string]Handler
	cfg      Config
}

func New(repo TaskQueue, cfg Config) *Pool {
	cfg.applyDefaults()
	return &Pool{
		repo:     repo,
		handlers: make(map[string]Handler),
		cfg:      cfg,
	}
}

func (p *Pool) Register(taskType string, h Handler) {
	p.handlers[taskType] = h
}

// Run launches Size workers and blocks until ctx is cancelled and every
// in-flight handler has returned. Cancellation propagates to handlers via
// the same ctx, so handlers must respect it for shutdown to be timely.
func (p *Pool) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < p.cfg.Size; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			p.loop(ctx, id)
		}(i)
	}
	wg.Wait()
}

func (p *Pool) loop(ctx context.Context, id int) {
	log := p.cfg.Logger.With("worker", id)
	pollDelay := p.cfg.MinPoll

	for {
		if ctx.Err() != nil {
			return
		}

		tasks, err := p.repo.PickPending(ctx, 1)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Error("pick pending", "err", err)
			sleep(ctx, pollDelay)
			pollDelay = nextPollDelay(pollDelay, p.cfg.MaxPoll)
			continue
		}

		if len(tasks) == 0 {
			sleep(ctx, pollDelay)
			pollDelay = nextPollDelay(pollDelay, p.cfg.MaxPoll)
			continue
		}

		// Found work — reset polling interval so the next idle period starts fresh.
		pollDelay = p.cfg.MinPoll
		for _, t := range tasks {
			p.process(ctx, t, log)
		}
	}
}

// writeResultTimeout caps how long we wait when writing a task result. Kept
// independent of the worker ctx so a graceful shutdown can't strand a task
// in 'running' after its handler already finished.
const writeResultTimeout = 5 * time.Second

func (p *Pool) process(ctx context.Context, t *model.Task, log *slog.Logger) {
	handler, ok := p.handlers[t.Type]
	if !ok {
		msg := fmt.Sprintf("no handler registered for type %q", t.Type)
		log.Error("unknown task type", "id", t.ID, "type", t.Type)
		writeCtx, cancel := context.WithTimeout(context.Background(), writeResultTimeout)
		defer cancel()
		if err := p.repo.MarkFailedTerminal(writeCtx, t.ID, msg); err != nil {
			log.Error("mark failed terminal", "id", t.ID, "err", err)
		}
		return
	}

	err := handler(ctx, t.Payload)

	writeCtx, cancel := context.WithTimeout(context.Background(), writeResultTimeout)
	defer cancel()

	if err != nil {
		// If shutdown raced with the handler we don't penalize the task — it
		// stays 'running' and a future stuck-task sweep will recover it.
		// (The sweep is not implemented yet; until then, a crash mid-flight
		// strands the row.)
		if errors.Is(err, context.Canceled) {
			log.Warn("task interrupted by shutdown", "id", t.ID)
			return
		}
		backoff := computeBackoff(t.Attempts, p.cfg.BaseBackoff, p.cfg.MaxBackoff)
		if markErr := p.repo.MarkFailed(writeCtx, t.ID, err.Error(), backoff); markErr != nil {
			log.Error("mark failed", "id", t.ID, "err", markErr)
		}
		return
	}
	if err := p.repo.MarkDone(writeCtx, t.ID); err != nil {
		log.Error("mark done", "id", t.ID, "err", err)
	}
}

// computeBackoff returns base * 2^attempts with up to +20% jitter, capped at max.
// attempts is the count of completed retries; the first failure has attempts=0,
// so the very first backoff equals base.
func computeBackoff(attempts int, base, max time.Duration) time.Duration {
	multiplier := math.Pow(2, float64(attempts))
	delay := time.Duration(float64(base) * multiplier)
	if delay > max || delay < 0 {
		delay = max
	}
	if delay > 0 {
		jitter := time.Duration(rand.Int64N(int64(delay) / 5))
		delay += jitter
	}
	return delay
}

func nextPollDelay(current, max time.Duration) time.Duration {
	next := current * 2
	if next > max {
		return max
	}
	return next
}

func sleep(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
