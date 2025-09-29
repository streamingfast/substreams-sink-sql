package clickhouse

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/streamingfast/logging"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/schema"
	"github.com/streamingfast/substreams-sink-sql/dsn"
	"go.uber.org/zap"
)

type Database struct {
	*sql.BaseDatabase
	schema         *schema.Schema
	sinkInfoFolder string
	cursorFilePath string
	logger         *zap.Logger
	dialect        *DialectClickHouse
	cachedClient   *ch.Client
	dsn            *dsn.DSN
	ctx            context.Context
	inserter       *AccumulatorInserter
}

func NewDatabase(
	ctx context.Context,
	schema *schema.Schema,
	dsn *dsn.DSN,
	moduleOutputType string,
	rootMessageDescriptor *desc.MessageDescriptor,
	sinkInfoFolder string,
	cursorFilePath string,
	useProtoOptions bool,
	logger *zap.Logger,
	tracer logging.Tracer,
) (*Database, error) {
	baseDB, err := sql.NewBaseDatabase(moduleOutputType, rootMessageDescriptor, useProtoOptions, logger)
	if err != nil {
		return nil, fmt.Errorf("creating base database: %w", err)
	}
	dialect, err := NewDialectClickHouse(schema, logger)
	if err != nil {
		return nil, fmt.Errorf("creating dialect: %w", err)
	}

	database := &Database{
		ctx:            ctx,
		dsn:            dsn,
		BaseDatabase:   baseDB,
		dialect:        dialect,
		schema:         schema,
		sinkInfoFolder: sinkInfoFolder,
		cursorFilePath: cursorFilePath,
		logger:         logger,
	}
	inserter, err := NewAccumulatorInserter(database, logger, tracer)
	if err != nil {
		return nil, fmt.Errorf("creating accumulator inserter: %w", err)
	}
	database.inserter = inserter

	return database, nil
}

func (d *Database) Open() error {
	return nil
}

func newClient(dsn *dsn.DSN) (*ch.Client, error) {
	chOption := ch.Options{
		Address:     fmt.Sprintf("%s:%d", dsn.Host, dsn.Port),
		Database:    dsn.Database,
		User:        dsn.Username,
		Password:    dsn.Password,
		DialTimeout: 30 * time.Second,
	}

	for key, value := range dsn.Options.Iter() {
		if key == "secure" && value == "true" {
			chOption.TLS = &tls.Config{}
			continue
		}
		if key == "username" {
			chOption.User = value
			continue
		}
		if key == "password" {
			chOption.Password = value
			continue
		}
		if key == "compress" && value == "true" {
			chOption.Compression = ch.CompressionLZ4
			continue
		}
	}

	client, err := ch.Dial(context.Background(), chOption)

	if err != nil {
		return nil, fmt.Errorf("dialing clickhouse: %w", err)
	}
	return client, nil
}

func (d *Database) clientNoCache(dsn *dsn.DSN) (*ch.Client, error) {
	d.logger.Info("creating clickhouse client no cache", zap.String("connection_string", d.dsn.ConnString()))
	client, err := newClient(dsn)
	if err != nil {
		return nil, fmt.Errorf("creating clickhouse client: %w", err)
	}
	return client, nil
}

func (d *Database) client() (*ch.Client, error) {
	if d.cachedClient == nil || d.cachedClient.IsClosed() {
		d.logger.Info("creating clickhouse client", zap.String("connection_string", d.dsn.ConnString()))
		client, err := newClient(d.dsn)
		if err != nil {
			return nil, fmt.Errorf("creating clickhouse client: %w", err)
		}
		d.cachedClient = client
		return client, nil

	}

	return d.cachedClient, nil
}

func (d *Database) CreateDatabase(useConstraints bool) error {
	dsn := d.dsn.Clone()
	dsn.Database = "default"
	client, err := d.clientNoCache(dsn)
	if err != nil {
		return fmt.Errorf("creating clickhouse client for default database: %w", err)
	}

	d.logger.Info("creating database", zap.String("database_name", d.dsn.Database))

	err = client.Ping(d.ctx)
	if err != nil {
		return fmt.Errorf("pinging clickhouse: %w", err)
	}

	if err := client.Do(d.ctx, ch.Query{
		Body: fmt.Sprintf(staticSqlCreatDatabase, d.dsn.Database),
	}); err != nil {
		return fmt.Errorf("executing create database sql: %w", err)
	}

	client, err = d.client()
	if err != nil {
		return fmt.Errorf("creating clickhouse client for database %q: %w", d.dsn.Database, err)
	}

	d.logger.Info("database created", zap.String("database_name", d.dsn.Database))

	if err := client.Do(d.ctx, ch.Query{
		Body: fmt.Sprintf(staticSqlCreateBlock),
	}); err != nil {
		return fmt.Errorf("executing create block sql: %w", err)
	}

	d.logger.Info("block table created", zap.String("database_name", d.dsn.Database))

	for _, statement := range d.dialect.CreateTableSql {
		if err := client.Do(d.ctx, ch.Query{
			Body: statement,
		}); err != nil {
			return fmt.Errorf("executing create table sql: %w %q", err, statement)
		}
		d.logger.Info("table created", zap.String("table_name", statement), zap.String("database_name", d.dsn.Database))
	}

	return nil
}

func (d *Database) Insert(table string, values []any) error {
	return d.inserter.insert(table, values)
}

func (d *Database) WalkMessageDescriptorAndInsert(dm *dynamic.Message, blockNum uint64, blockTimestamp time.Time, parent *sql.Parent) (time.Duration, error) {
	return d.BaseDatabase.WalkMessageDescriptorAndInsertWithDialect(dm, blockNum, blockTimestamp, parent, d.dialect, d)
}

func (d *Database) BeginTransaction() error {
	return nil
}

func (d *Database) CommitTransaction() error {
	return nil
}

func (d *Database) RollbackTransaction() {
}

func (d *Database) Flush() (time.Duration, error) {
	d.logger.Debug("flushing")

	startFlush := time.Now()
	err := d.inserter.flush(d)
	if err != nil {
		return 0, fmt.Errorf("flushing: %w", err)
	}
	return time.Since(startFlush), nil
}

func (d *Database) GetDialect() sql.Dialect {
	return d.dialect
}

func (d *Database) InsertBlock(blockNum uint64, hash string, timestamp time.Time) error {
	d.logger.Debug("inserting _block_", zap.Uint64("block_num", blockNum), zap.String("block_hash", hash))
	err := d.inserter.insert("_blocks_", []any{blockNum, hash, timestamp, time.Now().UnixNano(), false})
	if err != nil {
		return fmt.Errorf("inserting block %d: %w", blockNum, err)
	}

	return nil
}

func (d *Database) FetchSinkInfo(databaseName string) (*sql.SinkInfo, error) {
	fileName := fmt.Sprintf("%s_db_hash.txt", databaseName)
	filePath := path.Join(d.sinkInfoFolder, fileName)
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			d.logger.Warn("database hash file does not exist", zap.String("file_path", filePath))
			return nil, nil
		}
		return nil, fmt.Errorf("opening database hash file: %w", err)
	}
	defer file.Close()

	var hash string
	_, err = fmt.Fscanf(file, "%s", &hash)
	if err != nil {
		return nil, fmt.Errorf("reading schema hash from file: %w", err)
	}

	return &sql.SinkInfo{SchemaHash: hash}, nil
}

func (d *Database) StoreSinkInfo(schemaName string, schemaHash string) error {
	fileName := fmt.Sprintf("%s_schema_hash.txt", schemaName)
	schemaFilePath := path.Join(d.sinkInfoFolder, fileName)

	file, err := os.Create(schemaFilePath)
	if err != nil {
		return fmt.Errorf("creating schema hash file: %w", err)
	}
	defer file.Close()

	_, err = file.WriteString(schemaHash)
	if err != nil {
		return fmt.Errorf("writing schema hash to file: %w", err)
	}

	return nil
}

func (d *Database) UpdateSinkInfoHash(schemaName string, newHash string) error {
	panic("implement me")
}

func (d *Database) FetchCursor() (*sink.Cursor, error) {
	if d.cursorFilePath == "" {
		return nil, fmt.Errorf("cursor file path is not set")
	}

	file, err := os.Open(d.cursorFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("opening cursor file: %w", err)
	}
	defer file.Close()

	cursorData, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("reading cursor file: %w", err)
	}

	cursor, err := sink.NewCursor(string(cursorData))
	if err != nil {
		return nil, fmt.Errorf("parsing cursor: %w", err)
	}

	return cursor, nil

}

func (d *Database) StoreCursor(cursor *sink.Cursor) error {
	if d.cursorFilePath == "" {
		return fmt.Errorf("cursor file path is not set")
	}

	file, err := os.Create(d.cursorFilePath)
	if err != nil {
		return fmt.Errorf("creating cursor file: %w", err)
	}
	defer file.Close()

	_, err = file.WriteString(cursor.String())
	if err != nil {
		return fmt.Errorf("writing cursor to file: %w", err)
	}

	return nil
}

func (d *Database) HandleBlocksUndo(lastValidBlockNum uint64) error {
	tables := d.dialect.GetTables()

	// Sort tables in descending order based on their Ordinal field
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].Ordinal > tables[j].Ordinal
	})

	client, err := d.client()
	if err != nil {
		return fmt.Errorf("creating clickhouse client: %w", err)
	}

	err = d.BeginTransaction()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	version := time.Now().UnixNano()

	d.logger.Info("undoing blocks", zap.String("table", "_block_"), zap.Uint64("last_valid_block_num", lastValidBlockNum))
	start := time.Now()
	insertDeleteBlocks := fmt.Sprintf(`
		INSERT INTO _blocks_
		SELECT number, hash, timestamp, %d, true
		FROM _blocks_ WHERE number > %d
		`, version, lastValidBlockNum)

	err = client.Do(d.ctx, ch.Query{
		Body: insertDeleteBlocks,
	})
	if err != nil {
		return fmt.Errorf("deleting block from %d: %w", lastValidBlockNum, err)
	}
	d.logger.Info("undo completed", zap.String("table", "_block_"), zap.Duration("duration", time.Since(start)))

	for _, table := range tables {
		d.logger.Info("undoing blocks", zap.String("table", table.Name), zap.Uint64("last_valid_block_num", lastValidBlockNum))
		start := time.Now()
		tableName := table.Name
		fields := ""

		if table.ChildOf != nil {
			parentTable, parentFound := d.dialect.TableRegistry[table.ChildOf.ParentTable]
			if !parentFound {
				return fmt.Errorf("parent table %q not found", table.ChildOf.ParentTable)
			}
			fieldFound := false
			for _, parentField := range parentTable.Columns {

				if parentField.Name == table.ChildOf.ParentTableField {
					fields += fmt.Sprintf(", %s", parentField.Name)
					fieldFound = true
					break
				}
			}
			if !fieldFound {
				return fmt.Errorf("field %q not found in table %q", table.ChildOf.ParentTableField, table.ChildOf.ParentTable)
			}
		}

		for _, column := range table.Columns {
			fields += fmt.Sprintf(", %s", column.Name)
		}
		query := fmt.Sprintf(`
			INSERT INTO %s
			SELECT %s, %s, %d, true %s
			FROM %s WHERE %s > %d
			`, tableName, sql.DialectFieldBlockNumber, sql.DialectFieldBlockTimestamp, version, fields, tableName, sql.DialectFieldBlockNumber, lastValidBlockNum)

		err := client.Do(d.ctx, ch.Query{
			Body: query,
		})
		if err != nil {
			return fmt.Errorf("deleting block from %d: %w", lastValidBlockNum, err)
		}

		d.logger.Info("undo completed", zap.String("table", table.Name), zap.Duration("duration", time.Since(start)))
	}
	err = d.CommitTransaction()
	if err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	return nil
}

func (d *Database) Clone() sql.Database {
	base := d.BaseClone()
	d.BaseDatabase = base
	return d
}

func (d *Database) DatabaseHash(schemaName string) (uint64, error) {
	panic("not implemented")
}
