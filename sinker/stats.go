package sinker

import (
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dmetrics"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

type Stats struct {
	*shutter.Shutter

	dbFlushRate    *dmetrics.AvgRatePromCounter
	flusehdEntries *dmetrics.ValueFromMetric
	lastBlock      bstream.BlockRef
	logger         *zap.Logger
}

func NewStats(logger *zap.Logger) *Stats {
	return &Stats{
		Shutter: shutter.New(),

		dbFlushRate:    dmetrics.MustNewAvgRateFromPromCounter(FlushCount, 1*time.Second, 30*time.Second, "flush"),
		flusehdEntries: dmetrics.NewValueFromMetric(FlushedEntriesCount, "entries"),
		logger:         logger,
	}
}

func (s *Stats) RecordBlock(block bstream.BlockRef) {
	s.lastBlock = block
}

func (s *Stats) Start(each time.Duration) {
	s.logger.Info("starting stats service", zap.Duration("runs_each", each))

	if s.IsTerminating() || s.IsTerminated() {
		panic("already shutdown, refusing to start again")
	}

	go func() {
		ticker := time.NewTicker(each)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Logging fields order is important as it affects the final rendering, we carefully ordered
				// them so the development logs looks nicer.
				fields := []zap.Field{
					zap.Stringer("db_flush_rate", s.dbFlushRate),
					zap.Uint64("flushed_entries", s.flusehdEntries.ValueUint()),
				}

				if s.lastBlock == nil {
					fields = append(fields, zap.String("last_block", "None"))
				} else {
					fields = append(fields, zap.Stringer("last_block", s.lastBlock))
				}

				s.logger.Info("substreams postgres sink stats", fields...)
			case <-s.Terminating():
				return
			}
		}
	}()
}

func (s *Stats) Close() {
	s.Shutdown(nil)
}
