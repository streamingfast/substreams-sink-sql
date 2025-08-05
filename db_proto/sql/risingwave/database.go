package risingwave

import (
	pqsql "database/sql"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams-sink-sql/db_changes/db"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/schema"
	"go.uber.org/zap"
)

type Database struct {
	*sql.BaseDatabase
	db             *pqsql.DB
	tx             *pqsql.Tx
	schema         *schema.Schema
	logger         *zap.Logger
	dialect        *DialectRisingwave
	inserter       rwInserter
	flusher        rwFlusher
	useConstraints bool
}

type rwInserter interface {
	Insert(tableName string, data []any, wrapInsertFunc func(string, []any) (string, []any)) error
}

type rwFlusher interface {
	Flush() (time.Duration, error)
}

func NewDatabase(schema *schema.Schema, dsn *db.DSN, moduleOutputType string, rootMessageDescriptor *desc.MessageDescriptor, useProtoOptions bool, useConstraints bool, logger *zap.Logger) (*Database, error) {
	logger = logger.Named("risingwave")

	connectionString := dsn.ConnString()
	logger.Info("connecting to db", zap.String("dsn", connectionString))
	sqlDB, err := pqsql.Open(dsn.Driver(), connectionString)
	if err != nil {
		return nil, fmt.Errorf("open db connection: %w", err)
	}

	dialect, err := NewDialectRisingwave(schema, logger)
	if err != nil {
		return nil, fmt.Errorf("creating risingwave dialect: %w", err)
	}

	baseDB, err := sql.NewBaseDatabase(moduleOutputType, rootMessageDescriptor, useProtoOptions, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create base database: %w", err)
	}
	database := &Database{
		db:             sqlDB,
		schema:         schema,
		useConstraints: useConstraints,
		BaseDatabase:   baseDB,
		dialect:        dialect,
		logger:         logger,
	}

	return database, nil
}

func (d *Database) InsertBlock(blockNum uint64, hash string, timestamp time.Time) error {
	d.logger.Debug("inserting _blocks_", zap.Uint64("block_num", blockNum), zap.String("block_hash", hash))
	err := d.inserter.Insert("_blocks_", []any{blockNum, hash, timestamp}, d.WrapInsertStatement)
	if err != nil {
		return fmt.Errorf("inserting block %d: %w", blockNum, err)
	}

	return nil
}

func (d *Database) FetchSinkInfo(schemaName string) (*sql.SinkInfo, error) {
	query := fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '_sink_info_')", schemaName)

	var exist bool
	err := d.db.QueryRow(query).Scan(&exist)
	if err != nil {
		return nil, fmt.Errorf("checking if sync_info table exists: %w", err)
	}
	if !exist {
		return nil, nil
	}

	out := &sql.SinkInfo{}

	err = d.db.QueryRow(fmt.Sprintf("SELECT schema_hash FROM %s._sink_info_", d.schema.Name)).Scan(&out.SchemaHash)
	if err != nil {
		return nil, fmt.Errorf("fetching sync info: %w", err)
	}
	return out, nil

}

func (d *Database) StoreSinkInfo(schemaName string, schemaHash string) error {
	_, err := d.tx.Exec(fmt.Sprintf("INSERT INTO %s._sink_info_ (schema_hash) VALUES ($1)", schemaName), schemaHash)
	if err != nil {
		return fmt.Errorf("storing schema hash: %w", err)
	}
	return nil
}

func (d *Database) UpdateSinkInfoHash(schemaName string, newHash string) error {
	_, err := d.tx.Exec(fmt.Sprintf("UPDATE %s._sink_info_ SET schema_hash = $1", schemaName), newHash)
	if err != nil {
		return fmt.Errorf("updating schema hash: %w", err)
	}
	return nil
}

func (d *Database) FetchCursor() (*sink.Cursor, error) {
	query := fmt.Sprintf("SELECT cursor FROM %s WHERE name = $1", tableName(d.schema.Name, "_cursor_"))

	rows, err := d.db.Query(query, "cursor")
	if err != nil {
		return nil, fmt.Errorf("selecting cursor: %w", err)
	}
	defer rows.Close()

	if rows.Next() {
		var cursor string
		err = rows.Scan(&cursor)

		return sink.NewCursor(cursor)
	}
	return nil, nil
}

func (d *Database) StoreCursor(cursor *sink.Cursor) error {
	err := d.inserter.Insert("_cursor_", []any{"cursor", cursor.String()}, d.WrapInsertStatement)
	if err != nil {
		return fmt.Errorf("inserting cursor: %w", err)
	}

	return err
}

func (d *Database) HandleBlocksUndo(lastValidBlockNum uint64) (err error) {
	tx, err := d.db.Begin()
	if err != nil {
		return fmt.Errorf("HandleBlocksUndo beginning transaction: %w", err)
	}
	defer func() {
		if err != nil {
			e := tx.Rollback()
			if e != nil {
				err = fmt.Errorf("HandleBlocksUndo rolling back transaction: %w", e)
			}
			err = fmt.Errorf("HandleBlocksUndo processing entity: %w", err)

			return
		}
		err = tx.Commit()
	}()

	d.logger.Info("undoing blocks", zap.Uint64("last_valid_block_num", lastValidBlockNum))
	query := fmt.Sprintf(`DELETE FROM %s._blocks_ WHERE "number" > $1`, d.schema.Name)
	result, err := tx.Exec(query, lastValidBlockNum)
	if err != nil {
		return fmt.Errorf("deleting block from %d: %w", lastValidBlockNum, err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("fetching rows affected: %w", err)
	}
	d.logger.Info("undo completed", zap.Int64("row_affected", rowsAffected))

	return nil
}

func (d *Database) DatabaseHash(schemaName string) (uint64, error) {
	query := `
SELECT
    c.table_name,
    c.column_name,
    c.is_nullable,
    c.data_type,
    c.character_maximum_length,
    c.numeric_precision,
    c.numeric_precision_radix,
    c.numeric_scale,
    c.datetime_precision,
    c.interval_precision,
    c.is_generated,
    c.is_updatable,
    tc.constraint_name,
    tc.table_name,
    tc.constraint_type,
    kcu.column_name,
    kcu.table_name,
    kcu.column_name,
    ccu.constraint_name,
    ccu.table_name,
    ccu.column_name
FROM
    information_schema.columns c
        LEFT JOIN
    information_schema.constraint_column_usage ccu
    ON c.table_name = ccu.table_name
        AND c.column_name = ccu.column_name
        AND c.table_schema = ccu.table_schema
        LEFT JOIN
    information_schema.key_column_usage kcu
    ON ccu.constraint_name = kcu.constraint_name
        AND c.table_schema = kcu.table_schema
        LEFT JOIN
    information_schema.table_constraints tc
    ON kcu.constraint_name = tc.constraint_name
        AND kcu.table_schema = tc.table_schema
WHERE
    c.table_schema = '%s'
ORDER BY
    c.table_name,
    c.column_name,
    tc.table_name,
    tc.constraint_name,
    kcu.table_name,
    kcu.column_name,
    kcu.constraint_name;
`

	query = fmt.Sprintf(query, schemaName)

	rows, err := d.db.Query(query)
	if err != nil {
		return 0, fmt.Errorf("executing query to compute schema hash: %w", err)
	}
	defer rows.Close()

	h := fnv.New64a()
	columns, err := rows.Columns()
	if err != nil {
		return 0, fmt.Errorf("fetching columns for hashing: %w", err)
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(valuePtrs...)
		if err != nil {
			return 0, fmt.Errorf("scanning row for hashing: %w", err)
		}

		for _, val := range values {
			var str string
			if val != nil {
				str = fmt.Sprintf("%v", val)
			}
			_, err = h.Write([]byte(str))
			if err != nil {
				return 0, fmt.Errorf("hashing value %q: %w", str, err)
			}
		}
	}

	if err = rows.Err(); err != nil {
		return 0, fmt.Errorf("iterating rows: %w", err)
	}

	return h.Sum64(), nil
}

func (d *Database) Open() error {
	if d.useConstraints {
		inserter, err := NewRowInserter(d, d.logger)
		if err != nil {
			return fmt.Errorf("creating row inserter: %w", err)
		}
		d.inserter = &rowInserterAdapter{inserter: inserter, tx: d.tx}
		d.flusher = &rowFlusherAdapter{inserter: inserter, tx: d.tx}
	} else {
		inserter, err := NewAccumulatorInserter(d, d.logger)
		if err != nil {
			return fmt.Errorf("creating accumulator inserter: %w", err)
		}
		d.inserter = &accumulatorInserterAdapter{inserter: inserter, tx: d.tx}
		d.flusher = &accumulatorFlusherAdapter{inserter: inserter, tx: d.tx}
	}
	return nil
}

func (d *Database) GetDialect() sql.Dialect {
	return d.dialect
}

func (d *Database) CreateDatabase(useConstraints bool) error {
	err := d.createDatabase()
	if err != nil {
		return fmt.Errorf("creating database: %w", err)
	}

	if useConstraints {
		err = d.applyConstraints()
		if err != nil {
			return fmt.Errorf("applying constraints: %w", err)
		}
	}
	return nil
}

func (d *Database) createDatabase() error {
	return d.dialect.CreateDatabase(d.tx)
}

func (d *Database) applyConstraints() error {
	return d.dialect.ApplyConstraints(d.tx)
}

func (d *Database) BeginTransaction() error {
	tx, err := d.db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	d.tx = tx
	return nil
}

func (d *Database) CommitTransaction() error {
	if d.tx == nil {
		return fmt.Errorf("no transaction to commit")
	}
	err := d.tx.Commit()
	if err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	d.tx = nil
	return nil
}

func (d *Database) RollbackTransaction() {
	if d.tx == nil {
		d.logger.Warn("no transaction to rollback")
		return
	}
	err := d.tx.Rollback()
	if err != nil {
		d.logger.Error("rollback transaction failed", zap.Error(err))
	}
	d.tx = nil
}

func (d *Database) Flush() (time.Duration, error) {
	return d.flusher.Flush()
}

func (d *Database) WrapInsertStatement(tableName string, data []any) (string, []any) {
	// RisingWave specific insert statement wrapper if needed
	return "", data
}

func (d *Database) Clone() sql.Database {
	// Create a new connection for parallel processing
	connectionString := d.schema.Name // This would need the DSN stored
	newDB, err := pqsql.Open("risingwave", connectionString)
	if err != nil {
		d.logger.Error("failed to clone database connection", zap.Error(err))
		return nil
	}

	return &Database{
		db:             newDB,
		schema:         d.schema,
		useConstraints: d.useConstraints,
		BaseDatabase:   d.BaseDatabase,
		dialect:        d.dialect,
		logger:         d.logger,
		inserter:       d.inserter,
		flusher:        d.flusher,
	}
}

func (d *Database) WalkMessageDescriptorAndInsert(dm *dynamic.Message, blockNum uint64, blockTimestamp time.Time, parent *sql.Parent) (time.Duration, error) {
	return d.BaseDatabase.WalkMessageDescriptorAndInsertWithDialect(dm, blockNum, blockTimestamp, parent, d.dialect, d)
}

func (d *Database) Insert(table string, values []any) error {
	return d.inserter.Insert(table, values, d.WrapInsertStatement)
}

// Adapter types to match interface expectations
type rowInserterAdapter struct {
	inserter *RowInserter
	tx       *pqsql.Tx
}

func (r *rowInserterAdapter) Insert(tableName string, data []any, wrapInsertFunc func(string, []any) (string, []any)) error {
	// Adapter to convert the wrapper function
	txWrapper := func(stmt *pqsql.Stmt) *pqsql.Stmt {
		if r.tx != nil {
			return r.tx.Stmt(stmt)
		}
		return stmt
	}
	return r.inserter.Insert(tableName, data, txWrapper)
}

type rowFlusherAdapter struct {
	inserter *RowInserter
	tx       *pqsql.Tx
}

func (r *rowFlusherAdapter) Flush() (time.Duration, error) {
	startTime := time.Now()
	err := r.inserter.Flush(r.tx)
	return time.Since(startTime), err
}

type accumulatorInserterAdapter struct {
	inserter *AccumulatorInserter
	tx       *pqsql.Tx
}

func (a *accumulatorInserterAdapter) Insert(tableName string, data []any, wrapInsertFunc func(string, []any) (string, []any)) error {
	txWrapper := func(stmt *pqsql.Stmt) *pqsql.Stmt {
		if a.tx != nil {
			return a.tx.Stmt(stmt)
		}
		return stmt
	}
	return a.inserter.Insert(tableName, data, txWrapper)
}

type accumulatorFlusherAdapter struct {
	inserter *AccumulatorInserter
	tx       *pqsql.Tx
}

func (a *accumulatorFlusherAdapter) Flush() (time.Duration, error) {
	startTime := time.Now()
	err := a.inserter.Flush(a.tx)
	return time.Since(startTime), err
}
