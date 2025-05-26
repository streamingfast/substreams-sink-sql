package clickhouse

import (
	pqsql "database/sql"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"time"

	"github.com/jhump/protoreflect/desc"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql"
	"go.uber.org/zap"
)

type Database struct {
	*sql.BaseDatabase
	schemaName     string
	sinkInfoFolder string
	cursorFilePath string
	logger         *zap.Logger
}

func NewDatabase(
	schemaName string,
	dialect *DialectClickHouse,
	db *pqsql.DB,
	moduleOutputType string,
	rootMessageDescriptor *desc.MessageDescriptor,
	sinkInfoFolder string,
	cursorFilePath string,
	useProtoOptions bool,
	logger *zap.Logger,
) (*Database, error) {
	baseDB, err := sql.NewBaseDatabase(dialect, db, moduleOutputType, rootMessageDescriptor, useProtoOptions, logger)
	if err != nil {
		return nil, fmt.Errorf("creating base database: %w", err)
	}
	return &Database{
		BaseDatabase:   baseDB,
		schemaName:     schemaName,
		sinkInfoFolder: sinkInfoFolder,
		cursorFilePath: cursorFilePath,
		logger:         logger,
	}, nil
}

func (d *Database) InsertBlock(blockNum uint64, hash string, timestamp time.Time) error {
	d.logger.Debug("inserting _block_", zap.Uint64("block_num", blockNum), zap.String("block_hash", hash))
	err := d.BaseDatabase.Inserter.Insert("_blocks_", []any{blockNum, hash, timestamp, time.Now().UnixNano(), false}, d.WrapInsertStatement)
	if err != nil {
		return fmt.Errorf("inserting block %d: %w", blockNum, err)
	}

	return nil
}

func (d *Database) FetchSinkInfo(schemaName string) (*sql.SinkInfo, error) {
	fileName := fmt.Sprintf("%s_schema_hash.txt", schemaName)
	schemaFilePath := path.Join(d.sinkInfoFolder, fileName)
	file, err := os.Open(schemaFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			d.logger.Warn("schema hash file does not exist", zap.String("file_path", schemaFilePath))
			return nil, nil
		}
		return nil, fmt.Errorf("opening schema hash file: %w", err)
	}
	defer file.Close()

	var schemaHash string
	_, err = fmt.Fscanf(file, "%s", &schemaHash)
	if err != nil {
		return nil, fmt.Errorf("reading schema hash from file: %w", err)
	}

	return &sql.SinkInfo{SchemaHash: schemaHash}, nil
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
	_, err := d.BaseDatabase.Tx.Exec(fmt.Sprintf("UPDATE %s._sink_info_ SET schema_hash = $1", schemaName), newHash)
	if err != nil {
		return fmt.Errorf("updating schema hash: %w", err)
	}
	return nil
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

	tables := d.Dialect.GetTables()

	// Sort tables in descending order based on their Ordinal field
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].Ordinal > tables[j].Ordinal
	})

	err := d.BeginTransaction()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	version := time.Now().UnixNano()

	d.logger.Info("undoing blocks", zap.String("table", "_block_"), zap.Uint64("last_valid_block_num", lastValidBlockNum))
	start := time.Now()
	insertDeleteBlocks := fmt.Sprintf(`
		INSERT INTO %s._blocks_
		SELECT number, hash, timestamp, %d, true 
		FROM %s._blocks_ WHERE number > %d
		`, d.schemaName, version, d.schemaName, lastValidBlockNum)
	result, err := d.BaseDatabase.Tx.Exec(insertDeleteBlocks, lastValidBlockNum)
	if err != nil {
		d.RollbackTransaction()
		return fmt.Errorf("deleting block from %d: %w", lastValidBlockNum, err)
	}
	rowEffected, err := result.RowsAffected()
	if err != nil {
		d.RollbackTransaction()
		return fmt.Errorf("fetching rows affected: %w", err)
	}
	d.logger.Info("undo completed", zap.String("table", "_block_"), zap.Int64("row_affected", rowEffected), zap.Duration("duration", time.Since(start)))

	for _, table := range tables {
		d.logger.Info("undoing blocks", zap.String("table", table.Name), zap.Uint64("last_valid_block_num", lastValidBlockNum))
		start := time.Now()
		tableFullName := d.Dialect.FullTableName(table)
		fields := ""

		dialect := d.Dialect.(*DialectClickHouse)
		if table.ChildOf != nil {
			parentTable, parentFound := dialect.TableRegistry[table.ChildOf.ParentTable]
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
			SELECT block_number, block_timestamp, %d, true %s 
			FROM %s WHERE block_number > %d
			`, tableFullName, version, fields, tableFullName, lastValidBlockNum)

		result, err = d.BaseDatabase.Tx.Exec(query, lastValidBlockNum)
		if err != nil {
			d.RollbackTransaction()
			return fmt.Errorf("deleting block from %d: %w", lastValidBlockNum, err)
		}

		rowEffected, err = result.RowsAffected()
		if err != nil {
			d.RollbackTransaction()
			return fmt.Errorf("fetching rows affected: %w", err)
		}
		d.logger.Info("undo completed", zap.String("table", table.Name), zap.Int64("row_affected", rowEffected), zap.Duration("duration", time.Since(start)))
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
