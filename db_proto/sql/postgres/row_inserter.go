package postgres

import (
	"database/sql"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	sql2 "github.com/streamingfast/substreams-sink-sql/db_proto/sql"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/schema"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type RowInserter struct {
	insertQueries    map[string]string
	insertStatements map[string]*sql.Stmt
	logger           *zap.Logger
}

func NewRowInserter(database *Database, logger *zap.Logger) (*RowInserter, error) {
	logger = logger.Named("postgres inserter")
	tables := database.Dialect.GetTables()
	insertStatements := map[string]*sql.Stmt{}
	insertQueries := map[string]string{}

	for _, table := range tables {
		query, err := createInsertFromDescriptor(table, database.Dialect)
		if err != nil {
			return nil, fmt.Errorf("creating insert from descriptor for table %q: %w", table.Name, err)
		}
		insertQueries[table.Name] = query

		stmt, err := database.DB.Prepare(query)
		if err != nil {
			return nil, fmt.Errorf("preparing statement %q: %w", query, err)
		}
		insertStatements[table.Name] = stmt
	}

	insertQueries["_blocks_"] = fmt.Sprintf("INSERT INTO %s (number, hash, timestamp) VALUES ($1, $2, $3) RETURNING number", tableName(database.schemaName, "_blocks_"))
	bs, err := database.DB.Prepare(insertQueries["_blocks_"])
	if err != nil {
		return nil, fmt.Errorf("preparing statement %q: %w", insertQueries["_blocks_"], err)
	}
	insertStatements["_blocks_"] = bs

	insertQueries["_cursor_"] = fmt.Sprintf("INSERT INTO %s (name, cursor) VALUES ($1, $2) ON CONFLICT (name) DO UPDATE SET cursor = $2", tableName(database.schemaName, "_cursor_"))
	cs, err := database.DB.Prepare(insertQueries["_cursor_"])
	if err != nil {
		return nil, fmt.Errorf("preparing statement %q: %w", insertQueries["_cursor_"], err)
	}
	insertStatements["_cursor_"] = cs

	return &RowInserter{
		insertStatements: insertStatements,
		insertQueries:    insertQueries,
		logger:           logger,
	}, nil
}

func createInsertFromDescriptor(table *schema.Table, dialect sql2.Dialect) (string, error) {
	tableName := dialect.FullTableName(table)
	fields := table.Columns

	var fieldNames []string
	var placeholders []string

	fieldCount := 0
	returningField := ""
	if table.PrimaryKey != nil {
		returningField = table.PrimaryKey.Name
	}

	fieldCount++
	fieldNames = append(fieldNames, "block_number")
	placeholders = append(placeholders, fmt.Sprintf("$%d", fieldCount))

	if pk := table.PrimaryKey; pk != nil {
		fieldCount++
		returningField = pk.Name
		fieldNames = append(fieldNames, pk.Name)
		placeholders = append(placeholders, fmt.Sprintf("$%d", fieldCount)) //$1
	}

	if table.ChildOf != nil {
		fieldCount++
		fieldNames = append(fieldNames, table.ChildOf.ParentTableField)
		placeholders = append(placeholders, fmt.Sprintf("$%d", fieldCount))
	}

	for _, field := range fields {
		if field.Name == returningField {
			continue
		}
		if field.IsRepeated || field.IsExtension { //not a direct child
			continue
		}
		fieldCount++
		fieldNames = append(fieldNames, field.QuotedName())
		placeholders = append(placeholders, fmt.Sprintf("$%d", fieldCount))
	}

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(fieldNames, ", "),
		strings.Join(placeholders, ", "),
	), nil

}

func (i *RowInserter) Insert(table string, values []any, txWrapper func(stmt *sql.Stmt) *sql.Stmt) error {
	i.logger.Debug("inserting row", zap.String("table", table), zap.Any("values", values))
	stmt := i.insertStatements[table]
	stmt = txWrapper(stmt)

	for i, value := range values {
		switch v := value.(type) {
		case uint64:
			values[i] = strconv.FormatUint(v, 10)
		case []uint8:
			values[i] = base64.StdEncoding.EncodeToString(v)
		case *timestamppb.Timestamp:
			values[i] = "'" + v.AsTime().Format(time.RFC3339) + "'"
		}
	}

	_, err := stmt.Exec(values...)
	if err != nil {
		insert := i.insertQueries[table]
		return fmt.Errorf("querying insert %q: %w", insert, err)
	}

	return nil
}

func (i *RowInserter) Flush(tx *sql.Tx) error {
	return nil
}
