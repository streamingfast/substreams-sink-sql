package sinker

import (
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dmetrics"
	"github.com/streamingfast/shutter"
	sink "github.com/streamingfast/substreams-sink"
	"go.uber.org/zap"
)

type Stats struct {
	*shutter.Shutter

	dbFlushRate        *dmetrics.AvgRatePromCounter
	dbFlushAvgDuration *dmetrics.AvgDurationCounter
	flushedRows        *dmetrics.ValueFromMetric
	dbFlushedRowsRate  *dmetrics.AvgRatePromCounter
	lastBlock          bstream.BlockRef
	logger             *zap.Logger
}

func NewStats(logger *zap.Logger) *Stats {
	return &Stats{
		Shutter: shutter.New(),

		dbFlushRate:        dmetrics.MustNewAvgRateFromPromCounter(FlushCount, 1*time.Second, 30*time.Second, "flush"),
		dbFlushAvgDuration: dmetrics.NewAvgDurationCounter(30*time.Second, dmetrics.InferUnit, "per flush"),
		flushedRows:        dmetrics.NewValueFromMetric(FlushedRowsCount, "rows"),
		dbFlushedRowsRate:  dmetrics.MustNewAvgRateFromPromCounter(FlushedRowsCount, 1*time.Second, 30*time.Second, "flushed rows"),
		logger:             logger,

		lastBlock: unsetBlockRef{},
	}
}

func (s *Stats) RecordBlock(block bstream.BlockRef) {
	s.lastBlock = block
}

func (s *Stats) RecordFlushDuration(duration time.Duration) {
	s.dbFlushAvgDuration.AddDuration(duration)
}

func (s *Stats) Start(each time.Duration, cursor *sink.Cursor) {
	if !cursor.IsBlank() {
		s.lastBlock = cursor.Block()
	}

	if s.IsTerminating() || s.IsTerminated() {
		panic("already shutdown, refusing to start again")
	}

	go func() {
		ticker := time.NewTicker(each)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				s.LogNow()
			case <-s.Terminating():
				return
			}
		}
	}()
}

func (s *Stats) LogNow() {
	// Logging fields order is important as it affects the final rendering, we carefully ordered
	// them so the development logs looks nicer.
	s.logger.Info("postgres sink stats",
		zap.Stringer("db_flush_rate", s.dbFlushRate),
		zap.Stringer("db_flush_duration_rate", s.dbFlushAvgDuration),
		zap.Uint64("flushed_rows", s.flushedRows.ValueUint()),
		zap.Stringer("db_flushed_rows_rate", s.dbFlushedRowsRate),
		zap.Stringer("last_block", s.lastBlock),
	)
}

func (s *Stats) Close() {
	s.Shutdown(nil)
}

type unsetBlockRef struct{}

func (unsetBlockRef) ID() string     { return "" }
func (unsetBlockRef) Num() uint64    { return 0 }
func (unsetBlockRef) String() string { return "<Unset>" }
