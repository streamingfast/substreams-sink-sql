package syncer

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/lidar/store"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/shutter"
	"github.com/streamingfast/substreams-postgres-sink/db"
	"github.com/streamingfast/substreams/client"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	"go.uber.org/zap"
)

type Streamer struct {
	*shutter.Shutter

	startBlock     uint64
	stopBlock      uint64
	clientConfig   *client.SubstreamsClientConfig
	pkg            *pbsubstreams.Package
	outputModule   *pbsubstreams.Module
	stats          *Stats
	db             *db.Loader
	cache          *cache
	requestAttempt uint64

	logger *zap.Logger
	tracer logging.Tracer
}

const BLOCK_PROGESS = 1000

func New(
	pkg *pbsubstreams.Package,
	outputModule *pbsubstreams.Module,
	startBlock,
	stopBlock uint64,
	clientConfig *client.SubstreamsClientConfig,
	db *db.Loader,
	logger *zap.Logger,
	tracer logging.Tracer,
) (*Streamer, error) {
	s := &Streamer{
		Shutter:      shutter.New(),
		clientConfig: clientConfig,
		pkg:          pkg,
		outputModule: outputModule,
		stats:        NewStats(logger),
		startBlock:   startBlock,
		stopBlock:    stopBlock,
		db:           db,
		//cache:        newCache(db),
		logger: logger,
		tracer: tracer,
	}

	return s, nil
}

func (s *Streamer) Start(ctx context.Context) error {
	//cursor, err := s.store.GetCursor()
	//if err != nil && !errors.Is(err, store.NotFound) {
	//	return fmt.Errorf("unable to retrieve cursor: %w", err)
	//}
	//
	//if errors.Is(err, store.NotFound) {
	//	cursorStartBlock := s.startBlock
	//	if s.startBlock > 0 {
	//		cursorStartBlock = s.startBlock - 1
	//	}
	//
	//	cursor = &store.Cursor{
	//		Model: gorm.Model{
	//			ID: 1,
	//		},
	//		Cursor:   "",
	//		BlockNum: cursorStartBlock,
	//	}
	//
	//	cursor, err = s.store.CreateCursor(cursor)
	//	if err != nil {
	//		return fmt.Errorf("failed to create initial cursor: %w", err)
	//	}
	//}

	s.OnTerminating(func(_ error) { s.stats.Close() })
	s.stats.OnTerminated(func(err error) { s.Shutdown(err) })
	s.stats.Start(15 * time.Second)

	return s.run(ctx, nil)
}

func (s *Streamer) run(ctx context.Context, cursor *store.Cursor) error {
	ssClient, closeFunc, callOpts, err := client.NewSubstreamsClient(s.clientConfig)
	if err != nil {
		return fmt.Errorf("new substreams client: %w", err)
	}

	s.OnTerminating(func(_ error) { closeFunc() })

	// We will wait at max approximatively 5m before diying
	backOff := backoff.WithContext(backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 15), ctx)

	for {
		var lastErr error

		s.requestAttempt++
		req := &pbsubstreams.Request{
			// Since we are not using a cursor and the `startBlockNum` is inclusive, we always want
			// to start one block later. If no cursor existed, it's created with the user's start block
			// adjusted to be minus one to be account for that.
			StartBlockNum: int64(cursor.BlockNum + 1),
			// Cursor is not working for now in Substreams, so let's use StartBlockNum instead
			// StartCursor:   cursor.Cursor,
			ForkSteps:     []pbsubstreams.ForkStep{pbsubstreams.ForkStep_STEP_IRREVERSIBLE},
			Modules:       s.pkg.Modules,
			OutputModules: []string{s.outputModule.Name},
		}

		s.logger.Info("launching substreams request",
			zap.Int64("start_block", req.StartBlockNum),
			zap.Uint64("request_attempt", s.requestAttempt),
		)

		progressMessageCount := 0
		stream, err := ssClient.Blocks(ctx, req, callOpts...)
		if err != nil {
			lastErr = fmt.Errorf("call sf.substreams.v1.Stream/Blocks: %w", err)
			SubstreamsErrorCount.Inc()
			goto errorHandling
		}

		for {
			if s.tracer.Enabled() {
				s.logger.Debug("substreams waiting to receive message", zap.Reflect("cursor", cursor))
			}

			resp, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					s.logger.Info("substreams ended correctly, will attempt to reconnect in 15 seconds", zap.Reflect("last_cursor", cursor))
					return nil
				}

				SubstreamsErrorCount.Inc()
				lastErr = fmt.Errorf("receive stream next message: %w", err)
				goto errorHandling
			}

			switch r := resp.Message.(type) {
			case *pbsubstreams.Response_Progress:

				for _, module := range r.Progress.Modules {
					progressMessageCount++
					ProgressMessageCount.Inc(module.Name)
				}

				if s.tracer.Enabled() {
					s.logger.Debug("received response progress", zap.Reflect("progress", r))
				}

			case *pbsubstreams.Response_Data:
				atBlock := bstream.NewBlockRef(r.Data.Clock.Id, r.Data.Clock.Number)
				cursor.Cursor = r.Data.Cursor
				cursor.BlockNum = r.Data.Clock.Number

				s.stats.RecordBlock(atBlock)

				for _, output := range r.Data.Outputs {
					DataMessageCount.Inc(output.Name)

					if output.Name == "store_transfers" {
						//deltas := output.GetStoreDeltas()
						//StoreDeltasCount.AddInt(len(deltas.Deltas), output.Name)
						//
						//if err := s.processStoreTransfersDeltas(deltas, atBlock); err != nil {
						//	return fmt.Errorf("failed to process store transfers deltas at block %q: %w", r.Data.Clock.String(), err)
						//}
					}
					if output.Name == "store_tokens" {
						//deltas := output.GetStoreDeltas()
						//if deltas == nil {
						//	continue
						//}
						//StoreDeltasCount.AddInt(len(deltas.Deltas), output.Name)
						//if err := s.processStoreTokenDeltas(deltas, atBlock); err != nil {
						//	return fmt.Errorf("failed to process store token deltas at block %q: %w", r.Data.Clock.String(), err)
						//}

					}
					if output.Name == "store_ownership" {
						//deltas := output.GetStoreDeltas()
						//if deltas == nil {
						//	continue
						//}
						//StoreDeltasCount.AddInt(len(deltas.Deltas), output.Name)
						//if err := s.processStoreOwnershipDeltas(deltas, atBlock); err != nil {
						//	return fmt.Errorf("failed to process store ownership at block %q: %w", r.Data.Clock.String(), err)
						//}
					}
				}

				if cursor.BlockNum%BLOCK_PROGESS == 0 {
					//if s.tracer.Enabled() {
					s.logger.Info(fmt.Sprintf("processing block 1/%d... flushing", BLOCK_PROGESS),
						zap.Uint64("block_num", cursor.BlockNum),
						zap.String("cursor", cursor.Cursor),
					)
					//}

					t0 := time.Now()
					if err := s.cache.Flush(cursor); err != nil {
						return fmt.Errorf("failed to flush: %w", err)
					}
					elapsed := time.Since(t0)
					s.logger.Info("flushing cache to db", zap.Duration("elapsed", elapsed))
					//FlushDuration.AddInt(int(elapsed.Nanoseconds()))

					s.cache.Reset()
				}

			default:
				s.logger.Error("received unknown type of message")
			}

		}

	errorHandling:
		if lastErr != nil {
			SubstreamsErrorCount.Inc()
			s.logger.Error("substreams encountered an error", zap.Error(lastErr))

			sleepFor := backOff.NextBackOff()
			if sleepFor == backoff.Stop {
				s.logger.Info("backoff requested to stop retries")
				return lastErr
			}

			s.logger.Info("sleeping before re-connecting", zap.Duration("sleep", sleepFor))
			time.Sleep(sleepFor)
		}
	}
}
