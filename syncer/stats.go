package syncer

import (
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

type Stats struct {
	*shutter.Shutter

	dbFlushRate     *RateFromCounter
	dataMsgRate     *RateFromCounter
	progressMsgRate *RateFromCounter
	storeDeltaRate  *RateFromCounter
	lastBlock       bstream.BlockRef

	logger *zap.Logger
}

func NewStats(logger *zap.Logger) *Stats {
	return &Stats{
		Shutter: shutter.New(),

		dbFlushRate:     NewPerSecondAverageRateFromCounter(FlushCount, 30*time.Second, "flush"),
		dataMsgRate:     NewPerSecondAverageRateFromCounter(DataMessageCount, 30*time.Second, "msg"),
		progressMsgRate: NewPerSecondAverageRateFromCounter(ProgressMessageCount, 30*time.Second, "msg"),
		storeDeltaRate:  NewPerSecondAverageRateFromCounter(StoreDeltasCount, 30*time.Second, "delta"),

		logger: logger,
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
					zap.Stringer("data_msg_rate", s.dataMsgRate),
					zap.Stringer("progress_msg_rate", s.progressMsgRate),
					zap.Stringer("store_delta_rate", s.storeDeltaRate),
				}

				if s.lastBlock == nil {
					fields = append(fields, zap.String("last_block", "None"))
				} else {
					fields = append(fields, zap.Stringer("last_block", s.lastBlock))
				}

				s.logger.Info("substreams lidar stats", fields...)
			case <-s.Terminating():
				break
			}
		}
	}()
}

func (s *Stats) Close() {
	s.Shutdown(nil)
}
