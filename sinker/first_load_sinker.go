package sinker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/streamingfast/dstore"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/shutter"
	sink "github.com/streamingfast/substreams-sink"
	pbdatabase "github.com/streamingfast/substreams-sink-database-changes/pb/sf/substreams/sink/database/v1"
	"github.com/streamingfast/substreams-sink-postgres/bundler"
	"github.com/streamingfast/substreams-sink-postgres/bundler/writer"
	"github.com/streamingfast/substreams-sink-postgres/db"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type FirstLoadSinker struct {
	*shutter.Shutter
	*sink.Sinker
	destFolder string

	fileBundlers map[string]*bundler.Bundler
	stopBlock    uint64

	// cursor
	loader *db.Loader
	logger *zap.Logger
	tracer logging.Tracer

	stats *Stats
}

func NewFirstLoad(
	sink *sink.Sinker,
	destFolder string,
	workingDir string,
	bundleSize uint64,
	bufferSize uint64,
	loader *db.Loader,
	logger *zap.Logger,
	tracer logging.Tracer,
) (*FirstLoadSinker, error) {
	blockRange := sink.BlockRange()
	if blockRange == nil || blockRange.EndBlock() == nil {
		return nil, fmt.Errorf("sink must have a stop block defined")
	}

	s := &FirstLoadSinker{
		Shutter: shutter.New(),
		Sinker:  sink,

		fileBundlers: make(map[string]*bundler.Bundler),
		stopBlock:    *blockRange.EndBlock(),

		loader: loader,
		logger: logger,
		tracer: tracer,

		stats: NewStats(logger),
	}

	csvOutputStore, err := dstore.NewStore(destFolder, "csv", "", false)
	if err != nil {
		return nil, err
	}
	tables := s.loader.GetAvailableTablesInSchemaList()

	for _, table := range tables {
		columns := s.loader.GetColumnsForTable(table)
		fb, err := getBundler(table, s.Sinker.BlockRange().StartBlock(), s.stopBlock, bundleSize, bufferSize, csvOutputStore, workingDir, logger, columns)
		if err != nil {
			return nil, err
		}
		s.fileBundlers[table] = fb
	}

	return s, nil
}

func (s *FirstLoadSinker) Run(ctx context.Context) {
	cursor, mistmatchDetected, err := s.loader.GetCursor(ctx, s.OutputModuleHash())
	if err != nil && !errors.Is(err, db.ErrCursorNotFound) {
		s.Shutdown(fmt.Errorf("unable to retrieve cursor: %w", err))
		return
	}

	// We write an empty cursor right away in the database because the flush logic
	// only performs an `update` operation so an initial cursor is required in the database
	// for the flush to work correctly.
	if errors.Is(err, db.ErrCursorNotFound) {
		if err := s.loader.InsertCursor(ctx, s.OutputModuleHash(), sink.NewBlankCursor()); err != nil {
			s.Shutdown(fmt.Errorf("unable to write initial empty cursor: %w", err))
			return
		}
	} else if mistmatchDetected {
		if err := s.loader.InsertCursor(ctx, s.OutputModuleHash(), cursor); err != nil {
			s.Shutdown(fmt.Errorf("unable to write new cursor after module mistmatch: %w", err))
			return
		}
	}
	s.Sinker.OnTerminating(s.Shutdown)
	s.OnTerminating(func(err error) {
		s.Sinker.Shutdown(err)
		s.stats.LogNow()
		s.logger.Info("csv sinker terminating", zap.Stringer("last_block_written", s.stats.lastBlock))
		s.stats.Close()
		if err == nil {
			s.handleStopBlockReached(ctx)
		}
		s.CloseAllFileBundlers(err)
	})

	s.stats.OnTerminated(func(err error) { s.Shutdown(err) })

	logEach := 15 * time.Second
	if s.logger.Core().Enabled(zap.DebugLevel) {
		logEach = 5 * time.Second
	}

	s.stats.Start(logEach, cursor)

	s.logger.Info("starting postgres sink",
		zap.Duration("stats_refresh_each", logEach),
		zap.Stringer("restarting_at", cursor.Block()),
		zap.String("database", s.loader.GetDatabase()),
		zap.String("schema", s.loader.GetSchema()),
	)

	uploadContext := context.Background()
	for _, fb := range s.fileBundlers {
		fb.Launch(uploadContext)
	}
	s.Sinker.Run(ctx, cursor, s)
}

func (s *FirstLoadSinker) HandleBlockScopedData(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *sink.Cursor) error {
	output := data.Output

	if output.Name != s.OutputModuleName() {
		return fmt.Errorf("received data from wrong output module, expected to received from %q but got module's output for %q", s.OutputModuleName(), output.Name)
	}

	dbChanges := &pbdatabase.DatabaseChanges{}
	mapOutput := output.GetMapOutput()
	if !mapOutput.MessageIs(dbChanges) && mapOutput.TypeUrl != "type.googleapis.com/sf.substreams.database.v1.DatabaseChanges" {
		return fmt.Errorf("mismatched message type: trying to unmarshal unknown type %q", mapOutput.MessageName())
	}

	// We do not use UnmarshalTo here because we need to parse an older proto type and
	// UnmarshalTo enforces the type check. So we check manually the `TypeUrl` above and we use
	// `Unmarshal` instead which only deals with the bytes value.
	if err := proto.Unmarshal(mapOutput.Value, dbChanges); err != nil {
		return fmt.Errorf("unmarshal database changes: %w", err)
	}

	if err := s.dumpDatabaseChangesIntoCSV(dbChanges); err != nil {
		return fmt.Errorf("apply database changes: %w", err)
	}

	s.rollAllBundlers(ctx, data.Clock.Number)

	if cursor.Block().Num()%s.batchBlockModulo(data, isLive) == 0 {
		flushStart := time.Now()
		if err := s.loader.Flush(ctx, s.OutputModuleHash(), cursor); err != nil {
			return fmt.Errorf("failed to flush at block %s: %w", cursor.Block(), err)
		}

		flushDuration := time.Since(flushStart)

		FlushCount.Inc()
		FlushedEntriesCount.SetUint64(s.loader.EntriesCount())
		FlushDuration.AddInt64(flushDuration.Nanoseconds())
		s.stats.RecordBlock(cursor.Block())
	}

	return nil
}

func (s *FirstLoadSinker) dumpDatabaseChangesIntoCSV(dbChanges *pbdatabase.DatabaseChanges) error {
	for _, change := range dbChanges.TableChanges {
		if !s.loader.HasTable(change.Table) {
			return fmt.Errorf(
				"your Substreams sent us a change for a table named %s we don't know about on %s (available tables: %s)",
				change.Table,
				s.loader.GetIdentifier(),
				s.loader.GetAvailableTablesInSchema(),
			)
		}

		var fields map[string]string
		switch u := change.PrimaryKey.(type) {
		case *pbdatabase.TableChange_Pk:
			var err error
			fields, err = s.loader.GetPrimaryKey(change.Table, u.Pk)
			if err != nil {
				return err
			}
		case *pbdatabase.TableChange_CompositePk:
			fields = u.CompositePk.Keys
		default:
			return fmt.Errorf("unknown primary key type: %T", change.PrimaryKey)
		}
		table := change.Table
		tableBundler, ok := s.fileBundlers[table]
		if !ok {
			return fmt.Errorf("cannot get bundler writer for table %s", table)
		}
		switch change.Operation {
		case pbdatabase.TableChange_CREATE:
			// add fields
			for _, field := range change.Fields {
				fields[field.Name] = field.NewValue
			}
			data, _ := bundler.CSVEncode(fields)
			if !tableBundler.HeaderWritten {
				tableBundler.Writer().Write(tableBundler.Header)
				tableBundler.HeaderWritten = true
			}
			tableBundler.Writer().Write(data)
		case pbdatabase.TableChange_UPDATE:
		case pbdatabase.TableChange_DELETE:
		default:
			return fmt.Errorf("Currently, we only support append only databases")
		}
	}

	return nil
}

func (s *FirstLoadSinker) handleStopBlockReached(ctx context.Context) error {
	store, err := dstore.NewSimpleStore(s.destFolder)
	if err != nil {
		return fmt.Errorf("failed to initialize store at path %s: %w", s.destFolder, err)
	}

	lastBlockAndHash := fmt.Sprintf("%d:%s\n", s.stats.lastBlock.Num(), s.stats.lastBlock.ID())
	
	if err := store.WriteObject(context.Background(), "last_block.txt", bytes.NewReader([]byte(lastBlockAndHash))); err != nil {
		s.logger.Warn("could not write last block")
	}

	return nil
}

func (s *FirstLoadSinker) rollAllBundlers(ctx context.Context, blockNum uint64) {
	var wg sync.WaitGroup
	for _, entityBundler := range s.fileBundlers {
		wg.Add(1)

		eb := entityBundler
		go func() {
			if err := eb.Roll(ctx, blockNum); err != nil {
				// no worries, Shutdown can and will be called multiple times
				if errors.Is(err, bundler.ErrStopBlockReached) {
					err = nil
				}
				s.Shutdown(err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func (s *FirstLoadSinker) CloseAllFileBundlers(err error) {
	var wg sync.WaitGroup
	for _, fb := range s.fileBundlers {
		wg.Add(1)
		f := fb
		go func() {
			f.Shutdown(err)
			<-f.Terminated()
			wg.Done()
		}()
	}
	wg.Wait()
}

func (s *FirstLoadSinker) HandleBlockUndoSignal(ctx context.Context, data *pbsubstreamsrpc.BlockUndoSignal, cursor *sink.Cursor) error {
	return fmt.Errorf("received undo signal but there is no handling of undo, this is because you used `--undo-buffer-size=0` which is invalid right now")
}

func (s *FirstLoadSinker) batchBlockModulo(blockData *pbsubstreamsrpc.BlockScopedData, isLive *bool) uint64 {
	if isLive == nil {
		panic(fmt.Errorf("liveness checker has been disabled on the Sinker instance, this is invalid in the context of 'substreams-sink-postgres'"))
	}

	if *isLive {
		return LIVE_BLOCK_FLUSH_EACH
	}

	if s.loader.FlushInterval() > 0 {
		return uint64(s.loader.FlushInterval())
	}

	return HISTORICAL_BLOCK_FLUSH_EACH
}

func getBundler(table string, startBlock, stopBlock, bundleSize, bufferSize uint64, baseOutputStore dstore.Store, workingDir string, logger *zap.Logger, columns []string) (*bundler.Bundler, error) {
	// Should be CSV
	boundaryWriter := writer.NewBufferedIO(
		bufferSize,
		filepath.Join(workingDir, table),
		writer.FileTypeCSV,
		logger.With(zap.String("table_name", table)),
	)
	subStore, err := baseOutputStore.SubStore(table)
	if err != nil {
		return nil, err
	}

	sort.Strings(columns)

	header := []byte(strings.Join(columns, ",") + "\n")
	fb, err := bundler.New(bundleSize, stopBlock, boundaryWriter, subStore, logger, header)
	if err != nil {
		return nil, err
	}
	fb.Start(startBlock)
	return fb, nil
}
