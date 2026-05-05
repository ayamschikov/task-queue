package worker

import (
	"context"
	"log/slog"
	"time"
)

type StuckRecoverer interface {
	RecoverStuck(ctx context.Context, threshold time.Duration) (int, error)
}

type SweepConfig struct {
	Interval  time.Duration // how often to sweep
	Threshold time.Duration // running for longer than this = stuck
	Logger    *slog.Logger
}

const (
	defaultSweepInterval  = 30 * time.Second
	defaultSweepThreshold = 5 * time.Minute
)

func (c *SweepConfig) applyDefaults() {
	if c.Interval <= 0 {
		c.Interval = defaultSweepInterval
	}
	if c.Threshold <= 0 {
		c.Threshold = defaultSweepThreshold
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

type Sweeper struct {
	repo StuckRecoverer
	cfg  SweepConfig
}

func NewSweeper(repo StuckRecoverer, cfg SweepConfig) *Sweeper {
	cfg.applyDefaults()
	return &Sweeper{repo: repo, cfg: cfg}
}

// Run keeps recovering stuck tasks until ctx is cancelled. Errors during a
// sweep are logged, not fatal — the next tick retries.
func (s *Sweeper) Run(ctx context.Context) {
	log := s.cfg.Logger.With("component", "sweeper")
	log.Info("sweeper starting", "interval", s.cfg.Interval, "threshold", s.cfg.Threshold)

	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info("sweeper stopped")
			return
		case <-ticker.C:
			s.sweepOnce(ctx, log)
		}
	}
}

func (s *Sweeper) sweepOnce(ctx context.Context, log *slog.Logger) {
	// Use a brief detached ctx so an in-progress sweep can finish even if the
	// parent ctx fires; bounded by the interval so we never overlap two sweeps.
	sweepCtx, cancel := context.WithTimeout(context.Background(), s.cfg.Interval)
	defer cancel()

	n, err := s.repo.RecoverStuck(sweepCtx, s.cfg.Threshold)
	if err != nil {
		log.Error("recover stuck", "err", err)
		return
	}
	if n > 0 {
		log.Warn("recovered stuck tasks", "count", n)
	}
}
