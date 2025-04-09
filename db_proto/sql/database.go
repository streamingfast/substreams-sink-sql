package sql

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"strings"
	"time"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	pq "github.com/lib/pq"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams-sink-sql/db_proto/stats"
	"github.com/streamingfast/substreams-sink-sql/proto"
	"go.uber.org/zap"
)

type Database struct {
	Schema                *Schema
	Db                    *sql.DB
	logger                *zap.Logger
	mapOutputType         string
	insertStatements      map[string]*sql.Stmt
	RootMessageDescriptor *desc.MessageDescriptor
	tx                    *sql.Tx
}

func NewDatabase(schema *Schema, db *sql.DB, moduleOutputType string, rootMessageDescriptor *desc.MessageDescriptor, useConstraints bool, logger *zap.Logger) (database *Database, err error) {
	logger = logger.Named("database")

	if reachable, err := isDatabaseReachable(db); !reachable {
		return nil, fmt.Errorf("database not reachable: %w", err)
	}

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return nil, fmt.Errorf("beginning transaction: %w", err)
	}

	defer func() {
		if tx == nil {
			return
		}
		if err != nil {
			_ = tx.Rollback()
			err = fmt.Errorf("database not created cause by: %w", err)
			return
		}
	}()

	sinkInfo, err := getSinkInfo(db, schema.Name)
	if err != nil {
		return nil, fmt.Errorf("fetching sink info: %w", err)
	}

	originalSchemaName := schema.Name
	generateTempSchema := false
	if sinkInfo != nil && sinkInfo.SchemaHash != schema.Hash() {
		fmt.Println("mismatch between schema hash and sink info hash", sinkInfo.SchemaHash, schema.Hash())
		tempSchemaName := schema.Name + "_" + schema.Hash()

		tempSinkInfo, err := getSinkInfo(db, tempSchemaName)
		if err != nil {
			return nil, fmt.Errorf("fetching temp schema sink info: %w", err)
		}
		if tempSinkInfo != nil {
			hash, err := dbHashForSchema(schema.Name, db)
			if err != nil {
				return nil, fmt.Errorf("fetching schema %q hash: %w", schema.Name, err)
			}
			dbTempHash, err := dbHashForSchema(tempSchemaName, db)
			if err != nil {
				return nil, fmt.Errorf("fetching temp schema %q hash: %w", tempSchemaName, err)
			}

			if hash != dbTempHash {
				return nil, fmt.Errorf("schema %s and temp schema %s have different hash", schema.Name, tempSchemaName)
			}

			err = UpdateSinkInfoHash(tx, schema, tempSinkInfo.SchemaHash)
			if err != nil {
				return nil, fmt.Errorf("updating sink info hash: %w", err)
			}
		} else {
			err = schema.ChangeName(tempSchemaName)
			if err != nil {
				return nil, fmt.Errorf("changing schema name: %w", err)
			}
			generateTempSchema = true
		}

	}

	if sinkInfo == nil || generateTempSchema {
		fmt.Println("sinkInfo", sinkInfo)

		staticSql := fmt.Sprintf(static_sql, schema.String(), schema.String(), schema.String(), schema.String())
		_, err = tx.Exec(staticSql)
		if err != nil {
			return nil, fmt.Errorf("executing static staticSql: %w\n%s", err, staticSql)
		}

		for _, statement := range schema.tableCreateStatements {
			logger.Info("executing create statement", zap.String("sql", statement))
			_, err := tx.Exec(statement)
			if err != nil {
				return nil, fmt.Errorf("executing create statement: %w %s", err, statement)
			}
		}

		if useConstraints {
			err := ApplyConstraints(schema, tx, logger)
			if err != nil {
				return nil, fmt.Errorf("applying constraints: %w", err)
			}
		}

		err = StoreSinkInfo(tx, schema)
		if err != nil {
			return nil, fmt.Errorf("storing sink info: %w", err)
		}
		err := tx.Commit()
		if err != nil {
			return nil, fmt.Errorf("committing transaction: %w", err)
		}
	}

	if generateTempSchema {
		fmt.Println("Adjust schema named", originalSchemaName, "to match", schema.Name+"_temp")
		err := tx.Commit()
		tx = nil
		if err != nil {
			return nil, fmt.Errorf("committing transaction: %w", err)
		}

		return nil, fmt.Errorf("schema hash mismatch")
	}

	insertStatements, err := generateInsertStatements(schema, db)
	if err != nil {
		return nil, fmt.Errorf("generating insertSql: %w", err)
	}

	return &Database{
		Schema:                schema,
		Db:                    db,
		logger:                logger,
		mapOutputType:         moduleOutputType,
		RootMessageDescriptor: rootMessageDescriptor,
		insertStatements:      insertStatements,
	}, nil
}

func ApplyConstraints(schema *Schema, tx *sql.Tx, logger *zap.Logger) error {
	startAt := time.Now()
	for _, constraint := range schema.PrimaryKeyStatements {
		logger.Info("executing pk statement", zap.String("sql", constraint.sql))
		_, err := tx.Exec(constraint.sql)
		if err != nil {
			return fmt.Errorf("executing pk statement: %w %s", err, constraint.sql)
		}
	}
	for _, constraint := range schema.UniqueConstraintStatements {
		logger.Info("executing unique statement", zap.String("sql", constraint.sql))
		_, err := tx.Exec(constraint.sql)
		if err != nil {
			return fmt.Errorf("executing unique statement: %w %s", err, constraint.sql)
		}
	}
	for _, constraint := range schema.ForeignKeyStatements {
		logger.Info("executing fk constraint statement", zap.String("sql", constraint.sql))
		_, err := tx.Exec(constraint.sql)
		if err != nil {
			return fmt.Errorf("executing fk constraint statement: %w %s", err, constraint.sql)
		}
	}
	logger.Info("applying constraints", zap.Duration("duration", time.Since(startAt)))
	return nil
}

func (d *Database) BeginTransaction() (err error) {
	d.tx, err = d.Db.Begin()
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	return nil
}

func (d *Database) CommitTransaction() (err error) {
	err = d.tx.Commit()
	if err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	d.tx = nil
	return nil
}

func (d *Database) RollbackTransaction() {
	err := d.tx.Rollback()
	if err != nil {
		panic("RollbackTransaction failed: " + err.Error())
	}
}

func (d *Database) insertStatement(table string) *sql.Stmt {
	stmt, found := d.insertStatements[table]
	if !found {
		panic(fmt.Sprintf("insert statement not found for table %q", table))
	}
	if d.tx != nil {
		stmt = d.tx.Stmt(stmt)
	}
	return stmt
}

func (d *Database) ProcessMessage(dm *dynamic.Message, blockNum uint64, blockHash string, blockTimestamp time.Time, stats *stats.Stats) error {
	startInsertBlock := time.Now()
	id, err := d.insertBlock(blockNum, blockHash, blockTimestamp)
	if err != nil {
		return fmt.Errorf("inserting block: %w", err)
	}
	stats.BlockInsertDuration.Add(time.Since(startInsertBlock))

	_, sqlDuration, err := d.walkMessageDescriptorAndInsert(dm, id, nil, stats)
	if err != nil {
		return fmt.Errorf("processing message %q: %w", dm.GetMessageDescriptor().GetFullyQualifiedName(), err)
	}
	stats.EntitiesInsertDuration.Add(sqlDuration)

	return nil
}

func (d *Database) walkMessageDescriptorAndInsert(dm *dynamic.Message, blockId int, parent *Parent, stats *stats.Stats) (interface{}, time.Duration, error) {
	if dm == nil {
		return 0, 0, fmt.Errorf("received a nil message")
	}

	totalSqlDuration := time.Duration(0)
	var fieldValues []any
	fieldValues = append(fieldValues, blockId)

	if parent != nil {
		fieldValues = append(fieldValues, parent.id)
	}

	var childs [][]interface{}
	for _, fd := range dm.GetKnownFields() {
		fv := dm.GetField(fd)
		if v, ok := fv.([]interface{}); ok {
			childs = append(childs, v) //need to be handled after current message inserted
		} else if fm, ok := fv.(*dynamic.Message); ok {
			if fm == nil {
				fieldValues = append(fieldValues, nil)
				continue //un-use oneOf field
			}
			id, sqlDuration, err := d.walkMessageDescriptorAndInsert(fm, blockId, nil, stats)
			if err != nil {
				return 0, 0, fmt.Errorf("walking nested message descriptor %q: %w", fd.GetName(), err)
			}
			totalSqlDuration += sqlDuration
			fieldValues = append(fieldValues, id)
		} else {
			fieldValues = append(fieldValues, fv)
		}
	}

	md := dm.GetMessageDescriptor()
	var p *Parent
	tableInfo := proto.TableInfo(md)
	var id interface{}
	if tableInfo != nil {
		insertStartAt := time.Now()
		table := d.Schema.tableRegistry[tableInfo.Name]
		tableFullName := table.FullName(d.Schema)
		stmt := d.insertStatement(tableFullName)

		row := stmt.QueryRow(fieldValues...)
		err := row.Err()
		if err != nil {
			insert := d.Schema.insertSql[tableFullName]
			return 0, 0, fmt.Errorf("querying insert %q: %w", insert, err)
		}
		err = row.Scan(&id)

		p = &Parent{
			field: strings.ToLower(md.GetName()),
			id:    id,
		}
		totalSqlDuration += time.Since(insertStartAt)

	}

	for _, child := range childs {
		for _, c := range child {
			fm, ok := c.(*dynamic.Message)
			if !ok {
				panic("expected *dynamic.Message")
			}
			_, sqlDuration, err := d.walkMessageDescriptorAndInsert(fm, blockId, p, stats)
			if err != nil {
				return 0, 0, fmt.Errorf("processing child %q: %w", fm.GetMessageDescriptor().GetFullyQualifiedName(), err)
			}
			totalSqlDuration += sqlDuration
		}
	}

	return id, totalSqlDuration, nil
}

type Parent struct {
	field string
	id    interface{}
}

func (d *Database) HandleBlocksUndo(lastValidBlockNum uint64, cursor *sink.Cursor) (err error) {
	tx, err := d.Db.Begin()
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
	query := fmt.Sprintf(`DELETE FROM %s.block WHERE "number" > $1`, d.Schema.String())
	result, err := tx.Exec(query, lastValidBlockNum)
	if err != nil {
		return fmt.Errorf("deleting block from %d: %w", lastValidBlockNum, err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("fetching rows affected: %w", err)
	}
	d.logger.Info("undo completed", zap.Int64("row_affected", rowsAffected))

	err = d.InsertCursor(cursor)
	if err != nil {
		return fmt.Errorf("store cursor: %w", err)
	}

	return nil
}

func generateInsertStatements(schema *Schema, db *sql.DB) (map[string]*sql.Stmt, error) {
	statements := make(map[string]*sql.Stmt)
	for n, s := range schema.insertSql {
		stmt, err := db.Prepare(s)
		if err != nil {
			return nil, fmt.Errorf("preparing statement %q: %w", s, err)
		}
		statements[n] = stmt
	}

	return statements, nil
}

func (d *Database) insertBlock(blockNum uint64, hash string, timestamp time.Time) (block_db_id int, err error) {
	d.logger.Debug("inserting block", zap.Uint64("block_num", blockNum), zap.String("block_hash", hash))
	stmt := d.insertStatement("block")
	row := stmt.QueryRow(blockNum, hash, timestamp)

	err = row.Err()
	if err != nil {
		return -1, fmt.Errorf("inserting block %d: %w", blockNum, err)
	}

	var id int
	err = row.Scan(&id)

	return id, err
}

func (d *Database) InsertCursor(cursor *sink.Cursor) error {
	stmt := d.insertStatement("cursor")
	_, err := stmt.Exec("cursor", cursor.String())

	if err != nil {
		return fmt.Errorf("inserting cursor: %w", err)
	}

	return err
}

func FetchCursor(db *sql.DB, schema *Schema) (*sink.Cursor, error) {
	rows, err := db.Query(fmt.Sprintf("SELECT cursor FROM %s WHERE name = $1", TableName(schema, "cursor")), "cursor")
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

type SinkInfo struct {
	SchemaHash string `json:"schema_hash"`
}

func getSinkInfo(db *sql.DB, schemaName string) (*SinkInfo, error) {

	query := ""
	switch db.Driver().(type) {
	case *pq.Driver:
		query = fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '%s' AND table_name = 'sink_info')", schemaName)
		fmt.Println("sink info exist query", query)
	default:
		panic(fmt.Sprintf("unsupported database driver %T", db.Driver()))
	}

	var exist bool
	err := db.QueryRow(query).Scan(&exist)
	if err != nil {
		return nil, fmt.Errorf("checking if sync_info table exists: %w", err)
	}
	if !exist {
		return nil, nil
	}

	out := &SinkInfo{}

	err = db.QueryRow(fmt.Sprintf("SELECT schema_hash FROM %s.sink_info", schemaName)).Scan(&out.SchemaHash)
	if err != nil {
		return nil, fmt.Errorf("fetching sync info: %w", err)
	}
	return out, nil
}

func StoreSinkInfo(tx *sql.Tx, schema *Schema) error {
	_, err := tx.Exec(fmt.Sprintf("INSERT INTO %s.sink_info (schema_hash) VALUES ($1)", schema.Name), schema.Hash())
	if err != nil {
		return fmt.Errorf("storing schema hash: %w", err)
	}
	return nil
}

func UpdateSinkInfoHash(tx *sql.Tx, schema *Schema, newHash string) error {
	_, err := tx.Exec(fmt.Sprintf("UPDATE %s.sink_info SET schema_hash = $1", schema.Name), newHash)
	if err != nil {
		return fmt.Errorf("updating schema hash: %w", err)
	}
	return nil
}

func isDatabaseReachable(db *sql.DB) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err := db.PingContext(ctx)
	if err != nil {
		return false, err
	}
	return true, nil
}

func dbHashForSchema(schemaName string, db *sql.DB) (uint64, error) {
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

	rows, err := db.Query(query)
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
