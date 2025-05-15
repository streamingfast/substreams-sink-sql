package postgres

import (
	pqsql "database/sql"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/jhump/protoreflect/desc"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql"
	"go.uber.org/zap"
)

type Database struct {
	*sql.BaseDatabase
	schemaName string
	logger     *zap.Logger
}

func NewDatabase(schemaName string, dialect *DialectPostgres, db *pqsql.DB, moduleOutputType string, rootMessageDescriptor *desc.MessageDescriptor, useProtoOptions bool, logger *zap.Logger) (*Database, error) {
	baseDB, err := sql.NewBaseDatabase(dialect, db, moduleOutputType, rootMessageDescriptor, useProtoOptions, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create base database: %w", err)
	}
	return &Database{
		BaseDatabase: baseDB,
		schemaName:   schemaName,
		logger:       logger,
	}, nil
}

func (d *Database) InsertBlock(blockNum uint64, hash string, timestamp time.Time) error {
	d.logger.Debug("inserting _blocks_", zap.Uint64("block_num", blockNum), zap.String("block_hash", hash))
	err := d.BaseDatabase.Inserter.Insert("_blocks_", []any{blockNum, hash, timestamp}, d.WrapInsertStatement)
	if err != nil {
		return fmt.Errorf("inserting block %d: %w", blockNum, err)
	}

	return nil
}

func (d *Database) FetchSinkInfo(schemaName string) (*sql.SinkInfo, error) {
	query := fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '_sink_info_')", schemaName)

	var exist bool
	err := d.BaseDatabase.DB.QueryRow(query).Scan(&exist)
	if err != nil {
		return nil, fmt.Errorf("checking if sync_info table exists: %w", err)
	}
	if !exist {
		return nil, nil
	}

	out := &sql.SinkInfo{}

	err = d.BaseDatabase.DB.QueryRow(fmt.Sprintf("SELECT schema_hash FROM %s._sink_info_", d.schemaName)).Scan(&out.SchemaHash)
	if err != nil {
		return nil, fmt.Errorf("fetching sync info: %w", err)
	}
	return out, nil

}

func (d *Database) StoreSinkInfo(schemaName string, schemaHash string) error {
	_, err := d.BaseDatabase.Tx.Exec(fmt.Sprintf("INSERT INTO %s._sink_info_ (schema_hash) VALUES ($1)", schemaName), schemaHash)
	if err != nil {
		return fmt.Errorf("storing schema hash: %w", err)
	}
	return nil
}

func (d *Database) UpdateSinkInfoHash(schemaName string, newHash string) error {
	_, err := d.BaseDatabase.Tx.Exec(fmt.Sprintf("UPDATE %s._sink_info_ SET schema_hash = $1", schemaName), newHash)
	if err != nil {
		return fmt.Errorf("updating schema hash: %w", err)
	}
	return nil
}

func (d *Database) FetchCursor() (*sink.Cursor, error) {
	query := fmt.Sprintf("SELECT cursor FROM %s WHERE name = $1", tableName(d.schemaName, "_cursor_"))

	rows, err := d.BaseDatabase.DB.Query(query, "cursor")
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
	err := d.BaseDatabase.Inserter.Insert("_cursor_", []any{"cursor", cursor.String()}, d.WrapInsertStatement)
	if err != nil {
		return fmt.Errorf("inserting cursor: %w", err)
	}

	return err
}

func (d *Database) HandleBlocksUndo(lastValidBlockNum uint64) (err error) {
	tx, err := d.DB.Begin()
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
	query := fmt.Sprintf(`DELETE FROM %s._blocks_ WHERE "number" > $1`, d.schemaName)
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

func (d *Database) Clone() sql.Database {
	base := d.BaseClone()
	d.BaseDatabase = base
	return d
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

	rows, err := d.DB.Query(query)
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
