package parquet

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/parquet-go"
	sql2 "github.com/streamingfast/substreams-sink-sql/db_proto/sql"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/schema"
	"go.uber.org/zap"
	"google.golang.org/protobuf/reflect/protoreflect"
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
		if err := a.writeParquetFile(tableName, filePath, rows, db); err != nil {
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

func (a *AccumulatorInserter) writeParquetFile(tableName string, filePath string, rows []map[string]any, db *Database) error {
	if len(rows) == 0 {
		return nil
	}

	// Create file
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("creating file: %w", err)
	}
	defer file.Close()

	// Extract table name from file path to get the right accumulator
	accumulator := a.accumulators[tableName]
	if accumulator == nil {
		return fmt.Errorf("accumulator not found for table %q", tableName)
	}

	// Create schema from db_proto schema definitions
	schema, err := a.createSchemaFromAccumulator(accumulator)
	if err != nil {
		return fmt.Errorf("creating schema from accumulator: %w", err)
	}

	// Create writer with schema
	writer := parquet.NewWriter(file, schema)
	defer writer.Close()

	// Write rows using RowBuilder pattern
	for _, rowMap := range rows {
		builder := parquet.NewRowBuilder(schema)

		if err := a.buildRowFromMap(builder, rowMap, accumulator, schema); err != nil {
			return fmt.Errorf("building row: %w", err)
		}

		parquetRow := builder.Row()
		if _, err := writer.WriteRows([]parquet.Row{parquetRow}); err != nil {
			return fmt.Errorf("writing row: %w", err)
		}
	}

	return nil
}

// getTableNameFromFilePath extracts table name from file path
func (a *AccumulatorInserter) getTableNameFromFilePath(filePath string) string {
	filename := filepath.Base(filePath)
	// Remove extension and timestamp suffix
	// Expected format: tablename_timestamp.parquet
	parts := strings.Split(filename, "_")
	if len(parts) > 0 {
		return parts[0]
	}
	return filename
}

// createSchemaFromAccumulator builds parquet schema from db_proto schema definitions
func (a *AccumulatorInserter) createSchemaFromAccumulator(acc *accumulator) (*parquet.Schema, error) {
	group := make(parquet.Group)

	// Build schema based on column order and types
	for i := 0; i < len(acc.columns); i++ {
		column, found := acc.columns[i]
		if !found {
			continue
		}

		node, err := a.columnToParquetNode(column)
		if err != nil {
			return nil, fmt.Errorf("creating node for column %q: %w", column.Name, err)
		}
		group[column.Name] = node
	}

	return parquet.NewSchema(acc.tableName, group), nil
}

// columnToParquetNode converts db_proto column definition to parquet node
func (a *AccumulatorInserter) columnToParquetNode(column *schema.Column) (parquet.Node, error) {
	if column.FieldDescriptor != nil {
		// Use protobuf field descriptor to determine type
		fieldDesc := column.FieldDescriptor

		if fieldDesc.IsList() {
			// Handle repeated fields
			elementNode, err := a.protoKindToParquetNode(fieldDesc.Kind())
			if err != nil {
				return nil, err
			}
			return parquet.Repeated(elementNode), nil
		}

		return a.protoKindToParquetNode(fieldDesc.Kind())
	}

	// For system columns without field descriptors, use appropriate types based on column name
	switch column.Name {
	case sql2.DialectFieldBlockNumber:
		return parquet.Int(64), nil // Block number is typically int64
	case sql2.DialectFieldBlockTimestamp:
		return parquet.Timestamp(parquet.Millisecond), nil // Timestamp
	case sql2.DialectFieldVersion:
		return parquet.Int(64), nil // Version is typically int64
	case sql2.DialectFieldDeleted:
		return parquet.Leaf(parquet.BooleanType), nil // Deleted is boolean
	default:
		return parquet.String(), nil // Default to string for unknown system columns
	}
}

// protoKindToParquetNode maps protobuf kinds to parquet nodes
func (a *AccumulatorInserter) protoKindToParquetNode(kind protoreflect.Kind) (parquet.Node, error) {
	switch kind {
	case protoreflect.BoolKind:
		return parquet.Leaf(parquet.BooleanType), nil
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return parquet.Int(32), nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return parquet.Int(64), nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return parquet.Uint(32), nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return parquet.Uint(64), nil
	case protoreflect.FloatKind:
		return parquet.Leaf(parquet.FloatType), nil
	case protoreflect.DoubleKind:
		return parquet.Leaf(parquet.DoubleType), nil
	case protoreflect.StringKind:
		return parquet.String(), nil
	case protoreflect.BytesKind:
		return parquet.Leaf(parquet.ByteArrayType), nil
	case protoreflect.MessageKind:
		return parquet.Timestamp(parquet.Millisecond), nil // Assume timestamp for message types
	default:
		return parquet.String(), nil // Default to string for unknown types
	}
}

// buildRowFromMap builds a parquet row using RowBuilder pattern following the example
func (a *AccumulatorInserter) buildRowFromMap(builder *parquet.RowBuilder, rowMap map[string]any, acc *accumulator, schema *parquet.Schema) error {
	// Iterate through schema fields in order, similar to the provided example
	for fieldIndex, field := range schema.Fields() {
		columnName := field.Name()
		value, exists := rowMap[columnName]

		if !exists {
			// Skip missing fields - RowBuilder handles this
			continue
		}

		// Find the corresponding column definition
		var foundColumn interface{}
		for _, col := range acc.columns {
			if col.Name == columnName {
				foundColumn = col
				break
			}
		}

		// Handle different value types similar to the example
		if err := a.addValueToBuilder(builder, fieldIndex, value, foundColumn, field); err != nil {
			return fmt.Errorf("adding value for field %q: %w", columnName, err)
		}
	}

	return nil
}

func (a *AccumulatorInserter) addValueToBuilder(builder *parquet.RowBuilder, fieldIndex int, value any, column interface{}, field parquet.Field) error {
	if value == nil {
		// Skip null values - RowBuilder handles missing fields
		return nil
	}

	// Handle repeated fields specially - check if column is repeated
	isRepeated := false
	if column != nil {
		if col, ok := column.(*schema.Column); ok {
			isRepeated = col.IsRepeated
		}
	}

	if isRepeated {
		switch v := value.(type) {
		case []int32:
			for _, elem := range v {
				builder.Add(fieldIndex, parquet.ValueOf(elem))
			}
			builder.Next(fieldIndex)
		case []int64:
			for _, elem := range v {
				builder.Add(fieldIndex, parquet.ValueOf(elem))
			}
			builder.Next(fieldIndex)
		case []string:
			for _, elem := range v {
				builder.Add(fieldIndex, parquet.ValueOf(elem))
			}
			builder.Next(fieldIndex)
		case []any:
			for _, elem := range v {
				builder.Add(fieldIndex, parquet.ValueOf(elem))
			}
			builder.Next(fieldIndex)
		default:
			// Convert single value to parquet value
			pValue, err := a.anyToParquetValueNew(value)
			if err != nil {
				return err
			}
			builder.Add(fieldIndex, pValue)
		}
	} else {
		// Handle single values
		pValue, err := a.anyToParquetValueNew(value)
		if err != nil {
			return err
		}
		builder.Add(fieldIndex, pValue)
	}

	return nil
}

// anyToParquetValueNew converts any value to parquet.Value with proper array handling
func (a *AccumulatorInserter) anyToParquetValueNew(val any) (parquet.Value, error) {
	if val == nil {
		return parquet.NullValue(), nil
	}

	// Unwrap interface{} to get concrete value
	if valRef := reflect.ValueOf(val); valRef.Kind() == reflect.Interface && !valRef.IsNil() {
		val = valRef.Elem().Interface()
	}

	switch v := val.(type) {
	case bool:
		return parquet.BooleanValue(v), nil
	case int32:
		return parquet.Int32Value(v), nil
	case int:
		return parquet.Int64Value(int64(v)), nil
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
		// Convert unknown types to string representation
		return parquet.ValueOf(fmt.Sprintf("%v", v)), nil
	}
}

func (a *AccumulatorInserter) valueToNode(value any) (parquet.Node, error) {
	switch v := value.(type) {
	case bool:
		return parquet.Leaf(parquet.BooleanType), nil
	case int32:
		return parquet.Leaf(parquet.Int32Type), nil
	case int64, int:
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
	case []int32:
		return parquet.List(parquet.Leaf(parquet.Int32Type)), nil
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
		panic(fmt.Sprintf("unsupported type %T", v))
		//return parquet.String(), nil
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
	case int:
		return parquet.Int64Value(int64(v)), nil
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
	case []int32:
		// Handle []int32 arrays - convert to string representation for compatibility
		if len(v) == 0 {
			return parquet.ValueOf(""), nil
		}
		result := "["
		for i, elem := range v {
			if i > 0 {
				result += ", "
			}
			result += fmt.Sprintf("%d", elem)
		}
		result += "]"
		return parquet.ValueOf(result), nil
	case []any:
		// NOTE: Native parquet LIST structures are not supported in our map-based approach
		// because parquet.ValueOf() doesn't support slice types. The parquet-go library
		// requires structured data with proper schema definitions to handle native arrays.
		// Since we're working with dynamic maps, we convert arrays to readable string representations.
		if len(v) == 0 {
			return parquet.ValueOf(""), nil
		}

		// Create a string representation like "[elem1, elem2, elem3]"
		result := "["
		for i, elem := range v {
			if i > 0 {
				result += ", "
			}
			result += fmt.Sprintf("%v", elem)
		}
		result += "]"
		return parquet.ValueOf(result), nil
	default:
		panic(fmt.Sprintf("unsupported type %T", v))
		//return parquet.ValueOf(fmt.Sprintf("%v", v)), nil
	}
}
