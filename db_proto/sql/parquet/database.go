package parquet

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams-sink-sql/bytes"
	"github.com/streamingfast/substreams-sink-sql/db_changes/db"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/schema"
	"go.uber.org/zap"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

type parquetInserter interface {
	insert(table string, values []any) error
	init(db *Database) error
}

type parquetFlusher interface {
	flush(db *Database) error
}

type Database struct {
	*sql.BaseDatabase
	basePath string
	logger   *zap.Logger
	dialect  *DialectParquet
	inserter parquetInserter
	flusher  parquetFlusher
}

func NewDatabase(schema *schema.Schema, dsn *db.DSN, moduleOutputType string, rootMessageDescriptor protoreflect.MessageDescriptor, useProtoOptions bool, bytesEncoding bytes.Encoding, logger *zap.Logger) (*Database, error) {
	logger = logger.Named("parquet")

	logger.Info("using parquet base path", zap.String("path", dsn.Database))

	dialect, err := NewDialectParquet(schema, dsn.Database, logger)
	if err != nil {
		return nil, fmt.Errorf("creating parquet dialect: %w", err)
	}

	baseDB, err := sql.NewBaseDatabase(moduleOutputType, rootMessageDescriptor, useProtoOptions, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create base database: %w", err)
	}

	database := &Database{
		basePath:     dsn.Database,
		logger:       logger,
		BaseDatabase: baseDB,
		dialect:      dialect,
	}

	return database, nil
}

func (d *Database) Open() error {
	inserter, err := NewAccumulatorInserter(d.logger)
	if err != nil {
		return fmt.Errorf("creating accumulator inserter: %w", err)
	}
	if err := inserter.init(d); err != nil {
		return fmt.Errorf("initializing accumulator inserter: %w", err)
	}
	d.inserter = inserter
	d.flusher = inserter

	return nil
}

func (d *Database) GetDialect() sql.Dialect {
	return d.dialect
}

func (d *Database) CreateDatabase(useConstraints bool) error {
	// For parquet, "creating database" means ensuring the base directory exists
	if err := os.MkdirAll(d.basePath, 0755); err != nil {
		return fmt.Errorf("creating base directory: %w", err)
	}
	return nil
}

func (d *Database) createDatabase() error {
	// No-op for parquet - directories are created during dialect initialization
	return nil
}

func (d *Database) applyConstraints() error {
	// No constraints in parquet file system
	return nil
}

func (d *Database) BeginTransaction() error {
	// For parquet, transactions are handled at the file level
	// We could implement a simple transaction mechanism using temporary files
	return nil
}

func (d *Database) CommitTransaction() error {
	// Commit by moving temporary files to final locations
	return nil
}

func (d *Database) RollbackTransaction() {
	// Rollback by cleaning up temporary files
}

func (d *Database) Insert(table string, values []any) error {
	return d.inserter.insert(table, values)
}

func (d *Database) WalkMessageDescriptorAndInsert(dm *dynamicpb.Message, blockNum uint64, blockTimestamp time.Time, parent *sql.Parent) (time.Duration, error) {
	return d.WalkMessageDescriptorAndInsertWithDialect(dm, blockNum, blockTimestamp, parent, d.dialect, d)
}

func (d *Database) InsertBlock(blockNum uint64, hash string, timestamp time.Time) error {
	// For parquet, we might want to store block metadata in a separate file
	d.logger.Debug("inserting block metadata", zap.Uint64("block_num", blockNum), zap.String("block_hash", hash))
	return nil
}

func (d *Database) Flush() (time.Duration, error) {
	startFlush := time.Now()
	err := d.flusher.flush(d)
	if err != nil {
		return 0, fmt.Errorf("flushing: %w", err)
	}
	return time.Since(startFlush), nil
}

func (d *Database) FetchSinkInfo(schemaName string) (*sql.SinkInfo, error) {
	// For parquet, sink info could be stored in a metadata file
	sinkInfoPath := filepath.Join(d.basePath, "_sink_info.json")
	if _, err := os.Stat(sinkInfoPath); os.IsNotExist(err) {
		return nil, nil
	}

	// TODO: Implement reading sink info from JSON file
	return nil, nil
}

func (d *Database) StoreSinkInfo(schemaName string, schemaHash string) error {
	// TODO: Implement storing sink info to JSON file
	return nil
}

func (d *Database) UpdateSinkInfoHash(schemaName string, newHash string) error {
	// TODO: Implement updating sink info hash
	return nil
}

func (d *Database) FetchCursor() (*sink.Cursor, error) {
	// For parquet, cursor could be stored in a cursor file
	cursorPath := filepath.Join(d.basePath, "_cursor.txt")
	if _, err := os.Stat(cursorPath); os.IsNotExist(err) {
		return nil, nil
	}

	// TODO: Implement reading cursor from file
	return nil, nil
}

func (d *Database) StoreCursor(cursor *sink.Cursor) error {
	// TODO: Implement storing cursor to file
	return nil
}

func (d *Database) HandleBlocksUndo(lastValidBlockNum uint64) error {
	// For parquet, undo would involve removing files for blocks > lastValidBlockNum
	d.logger.Info("undoing blocks", zap.Uint64("last_valid_block_num", lastValidBlockNum))

	// TODO: Implement block undo logic for parquet files
	return nil
}

func (d *Database) Clone() sql.Database {
	base := d.BaseClone()
	d.BaseDatabase = base
	return d
}

func (d *Database) DatabaseHash(schemaName string) (uint64, error) {
	// For parquet, we could hash the file structure
	// For now, return a simple hash
	return 0, nil
}
