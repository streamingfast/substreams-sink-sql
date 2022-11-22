package syncer

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"

	pbddatabase "github.com/streamingfast/substreams-sink-postgres/pb/database/v1"
	"google.golang.org/protobuf/proto"

	"github.com/streamingfast/substreams/manifest"

	"github.com/cenkalti/backoff/v4"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/shutter"
	"github.com/streamingfast/substreams-sink-postgres/db"
	"github.com/streamingfast/substreams/client"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	"go.uber.org/zap"
)

type Syncer struct {
	*shutter.Shutter

	startBlock       uint64
	stopBlock        uint64
	clientConfig     *client.SubstreamsClientConfig
	pkg              *pbsubstreams.Package
	outputModule     *pbsubstreams.Module
	outputModuelHash string
	stats            *Stats
	db               *db.Loader
	cache            *cache
	requestAttempt   uint64

	logger *zap.Logger
	tracer logging.Tracer
}

const BLOCK_PROGESS = 1000

func New(pkg *pbsubstreams.Package, outputModule *pbsubstreams.Module, hash manifest.ModuleHash, startBlock, stopBlock uint64, clientConfig *client.SubstreamsClientConfig, db *db.Loader, logger *zap.Logger, tracer logging.Tracer) (*Syncer, error) {
	s := &Syncer{
		Shutter:          shutter.New(),
		clientConfig:     clientConfig,
		pkg:              pkg,
		outputModule:     outputModule,
		outputModuelHash: hex.EncodeToString(hash),
		stats:            NewStats(logger),
		startBlock:       startBlock,
		stopBlock:        stopBlock,
		db:               db,
		logger:           logger,
		tracer:           tracer,
	}

	return s, nil
}

func (s *Syncer) Start(ctx context.Context) error {
	cursor, err := s.db.GetCursor(s.outputModuelHash)
	if err != nil && !errors.Is(err, db.ErrCursorNotFound) {
		return fmt.Errorf("unable to retrieve cursor: %w", err)
	}

	if errors.Is(err, db.ErrCursorNotFound) {
		cursorStartBlock := s.startBlock
		if s.startBlock > 0 {
			cursorStartBlock = s.startBlock - 1
		}

		cursor = &db.Cursor{
			Id:       s.outputModuelHash,
			Cursor:   "",
			BlockNum: cursorStartBlock,
		}

		if err = s.db.WriteCursor(cursor); err != nil {
			return fmt.Errorf("failed to create initial cursor: %w", err)
		}
	}

	s.OnTerminating(func(_ error) { s.stats.Close() })
	s.stats.OnTerminated(func(err error) { s.Shutdown(err) })
	s.stats.Start(2 * time.Second)

	return s.run(ctx, cursor)
}

func (s *Syncer) run(ctx context.Context, cursor *db.Cursor) error {
	s.logger.Info("run substreams syncer", zap.Reflect("cursor", cursor))
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
			//StartCursor:   cursor.Cursor,
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
				BlockCount.AddInt(1)

				for _, output := range r.Data.Outputs {
					DataMessageCount.Inc(output.Name)

					if output.Name != s.outputModule.Name {
						continue
					}
					dbChanges := &pbddatabase.DatabaseChanges{}
					err := proto.Unmarshal(output.GetMapOutput().GetValue(), dbChanges)
					if err != nil {
						return fmt.Errorf("unmarshalling database changes: %w", err)
					}
					err = s.applyDatabaseChanges(dbChanges)
					if err != nil {
						return fmt.Errorf("applying  database changes: %w", err)
					}
				}

				if cursor.BlockNum%BLOCK_PROGESS == 0 {
					s.logger.Info(fmt.Sprintf("processing block 1/%d... flushing", BLOCK_PROGESS),
						zap.Uint64("block_num", cursor.BlockNum),
						zap.String("cursor", cursor.Cursor),
						zap.Object("db_cache", s.db),
					)
					FlushedEntriesCount.AddUint64(s.db.EntriesCount)
					t0 := time.Now()
					if err := s.db.Flush(ctx, cursor); err != nil {
						return fmt.Errorf("failed to flush: %w", err)
					}
					elapsed := time.Since(t0)
					s.logger.Info("flushing cache to db", zap.Duration("elapsed", elapsed))
					FlushCount.Inc()
					FlushDuration.AddInt(int(elapsed.Nanoseconds()))
					s.db.Reset()
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

func (s *Syncer) applyDatabaseChanges(dbChanges *pbddatabase.DatabaseChanges) error {
	for _, change := range dbChanges.TableChanges {

		if !s.db.HasTable(change.Table) {
			continue
		}

		primaryKey := change.Pk
		changes := map[string]string{}
		for _, field := range change.Fields {
			changes[field.Name] = field.NewValue
		}

		switch change.Operation {
		case pbddatabase.TableChange_CREATE:
			err := s.db.Insert(change.Table, primaryKey, changes)
			if err != nil {
				return fmt.Errorf("database insert: %w", err)
			}
		case pbddatabase.TableChange_UPDATE:
			err := s.db.Update(change.Table, primaryKey, changes)
			if err != nil {
				return fmt.Errorf("database update: %w", err)
			}
		case pbddatabase.TableChange_DELETE:
			err := s.db.Delete(change.Table, primaryKey)
			if err != nil {
				return fmt.Errorf("database delete: %w", err)
			}
		default:
			//case database.TableChange_UNSET:
		}
	}
	return nil
}
