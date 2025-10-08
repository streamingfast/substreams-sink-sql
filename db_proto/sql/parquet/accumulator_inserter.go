package parquet

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	"github.com/segmentio/parquet-go"
	sql2 "github.com/streamingfast/substreams-sink-sql/db_proto/sql"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/schema"
	"go.uber.org/zap"
)

type accumulator struct {
	ordinal   int
	tableName string
	columns   map[int]*schema.Column
}

type AccumulatorInserter struct {
	logger       *zap.Logger
	mu           sync.Mutex
	buffers      map[string][]map[string]any // table -> rows
	tablePaths   map[string]string
	accumulators map[string]*accumulator
}

func NewAccumulatorInserter(logger *zap.Logger) (*AccumulatorInserter, error) {
	return &AccumulatorInserter{
		logger:       logger,
		buffers:      make(map[string][]map[string]any),
		tablePaths:   make(map[string]string),
		accumulators: make(map[string]*accumulator),
	}, nil
}

func (a *AccumulatorInserter) init(db *Database) error {
	// Initialize table paths
	for tableName := range db.dialect.TableRegistry {
		a.tablePaths[tableName] = db.dialect.GetTablePath(tableName)
	}

	// Create accumulators for each table
	accumulators, err := createAccumulators(db.dialect)
	if err != nil {
		return fmt.Errorf("creating accumulators: %w", err)
	}
	a.accumulators = accumulators

	return nil
}

func createAccumulators(dialect *DialectParquet) (map[string]*accumulator, error) {
	accumulators := map[string]*accumulator{}

	tables := dialect.GetTables()
	for _, table := range tables {
		columns := map[int]*schema.Column{}
		colIndex := 0

		// Add system columns first (following the same order as WalkMessageDescriptorAndInsertWithDialect)
		columns[colIndex] = &schema.Column{Name: sql2.DialectFieldBlockNumber}
		colIndex++
		columns[colIndex] = &schema.Column{Name: sql2.DialectFieldBlockTimestamp}
		colIndex++

		if dialect.UseVersionField() {
			columns[colIndex] = &schema.Column{Name: sql2.DialectFieldVersion}
			colIndex++
		}

		if dialect.UseDeletedField() {
			columns[colIndex] = &schema.Column{Name: sql2.DialectFieldDeleted}
			colIndex++
		}

		// Add primary key if exists
		if table.PrimaryKey != nil {
			columns[colIndex] = &schema.Column{Name: table.PrimaryKey.Name}
			colIndex++
		}

		// Add parent foreign key if child table
		if table.ChildOf != nil {
			parentTable, parentFound := dialect.TableRegistry[table.ChildOf.ParentTable]
			if parentFound {
				for _, parentField := range parentTable.Columns {
					if parentField.Name == table.ChildOf.ParentTableField {
						columns[colIndex] = parentField
						colIndex++
						break
					}
				}
			}
		}

		// Add remaining table columns (excluding primary key which was handled above)
		for _, column := range table.Columns {
			if table.PrimaryKey != nil && column.Name == table.PrimaryKey.Name {
				continue // Skip primary key, already handled
			}
			columns[colIndex] = column
			colIndex++
		}

		accumulators[table.Name] = &accumulator{
			tableName: table.Name,
			ordinal:   table.Ordinal,
			columns:   columns,
		}
	}

	return accumulators, nil
}

func (a *AccumulatorInserter) insert(table string, values []any) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	accumulator := a.accumulators[table]
	if accumulator == nil {
		return fmt.Errorf("accumulator not found for table %q", table)
	}

	// Convert values to a map for parquet writing using accumulator column mapping
	row := make(map[string]any)

	for idx, value := range values {
		column, found := accumulator.columns[idx]
		if !found {
			return fmt.Errorf("column not found for table %q at idx %d", table, idx)
		}

		// Unwrap interface{} to get concrete value
		if value != nil {
			valRef := reflect.ValueOf(value)
			if valRef.Kind() == reflect.Interface && !valRef.IsNil() {
				value = valRef.Elem().Interface()
			}
		}

		row[column.Name] = value
	}

	// Initialize buffer for table if not exists
	if a.buffers[table] == nil {
		a.buffers[table] = make([]map[string]any, 0)
	}

	// Add row to buffer
	a.buffers[table] = append(a.buffers[table], row)

	return nil
}

func (a *AccumulatorInserter) flush(db *Database) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	start := time.Now()
	defer func() {
		db.logger.Debug("flushed parquet files", zap.Duration("duration", time.Since(start)))
	}()

	// Write buffered data to parquet files
	for tableName, rows := range a.buffers {
		if len(rows) == 0 {
			continue
		}

		tablePath := a.tablePaths[tableName]
		if err := db.dialect.EnsureTableDirectory(tableName); err != nil {
			return fmt.Errorf("ensuring table directory for %q: %w", tableName, err)
		}

		// Generate filename with timestamp
		filename := fmt.Sprintf("%s_%d.parquet", tableName, time.Now().Unix())
		filePath := filepath.Join(tablePath, filename)

		// Write parquet file
		if err := a.writeParquetFile(filePath, rows, db); err != nil {
			return fmt.Errorf("writing parquet file for table %q: %w", tableName, err)
		}

		db.logger.Info("wrote parquet file",
			zap.String("table", tableName),
			zap.String("file", filePath),
			zap.Int("rows", len(rows)))

		// Clear buffer after successful write
		a.buffers[tableName] = nil
	}

	return nil
}

func (a *AccumulatorInserter) writeParquetFile(filePath string, rows []map[string]any, _ *Database) error {
	if len(rows) == 0 {
		return nil
	}

	// Create file
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("creating file: %w", err)
	}
	defer file.Close()

	// Create schema from the first row
	schema, err := a.createSchemaFromMap(rows[0])
	if err != nil {
		return fmt.Errorf("creating schema: %w", err)
	}

	// Create writer with schema
	writer := parquet.NewWriter(file, schema)

	// Convert maps to Rows
	parquetRows := make([]parquet.Row, len(rows))
	for i, rowMap := range rows {
		row, err := a.mapToParquetRow(rowMap, schema)
		if err != nil {
			return fmt.Errorf("converting row %d: %w", i, err)
		}
		parquetRows[i] = row
	}

	// Write rows
	if _, err := writer.WriteRows(parquetRows); err != nil {
		return fmt.Errorf("writing rows: %w", err)
	}

	// Close writer
	if err := writer.Close(); err != nil {
		return fmt.Errorf("closing writer: %w", err)
	}

	return nil
}

func (a *AccumulatorInserter) createSchemaFromMap(row map[string]any) (*parquet.Schema, error) {
	group := make(parquet.Group)

	for columnName, value := range row {
		node, err := a.valueToNode(value)
		if err != nil {
			return nil, fmt.Errorf("creating node for %q: %w", columnName, err)
		}
		group[columnName] = node
	}

	return parquet.NewSchema("row", group), nil
}

func (a *AccumulatorInserter) valueToNode(value any) (parquet.Node, error) {
	switch v := value.(type) {
	case bool:
		return parquet.Leaf(parquet.BooleanType), nil
	case int32:
		return parquet.Leaf(parquet.Int32Type), nil
	case int64:
		return parquet.Leaf(parquet.Int64Type), nil
	case float32:
		return parquet.Leaf(parquet.FloatType), nil
	case float64:
		return parquet.Leaf(parquet.DoubleType), nil
	case string:
		return parquet.String(), nil
	case []byte:
		return parquet.Leaf(parquet.ByteArrayType), nil
	case time.Time:
		return parquet.Timestamp(parquet.Millisecond), nil
	case []any:
		if len(v) > 0 {
			elementNode, err := a.valueToNode(v[0])
			if err != nil {
				return nil, err
			}
			return parquet.List(elementNode), nil
		}
		return parquet.List(parquet.String()), nil
	default:
		return parquet.String(), nil
	}
}

func (a *AccumulatorInserter) mapToParquetRow(rowMap map[string]any, schema *parquet.Schema) (parquet.Row, error) {
	row := make(parquet.Row, 0, len(schema.Fields()))

	for _, field := range schema.Fields() {
		columnName := field.Name()
		if value, exists := rowMap[columnName]; exists {
			parquetValue, err := a.anyToParquetValue(value)
			if err != nil {
				return nil, fmt.Errorf("converting value for %q: %w", columnName, err)
			}
			row = append(row, parquetValue)
		} else {
			row = append(row, parquet.NullValue())
		}
	}

	return row, nil
}

func (a *AccumulatorInserter) anyToParquetValue(val any) (parquet.Value, error) {
	switch v := val.(type) {
	case bool:
		return parquet.BooleanValue(v), nil
	case int32:
		return parquet.Int32Value(v), nil
	case int64:
		return parquet.Int64Value(v), nil
	case float32:
		return parquet.FloatValue(v), nil
	case float64:
		return parquet.DoubleValue(v), nil
	case string:
		return parquet.ValueOf(v), nil
	case []byte:
		return parquet.ByteArrayValue(v), nil
	case time.Time:
		return parquet.ValueOf(v), nil
	default:
		return parquet.ValueOf(fmt.Sprintf("%v", v)), nil
	}
}
