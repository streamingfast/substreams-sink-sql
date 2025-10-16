package schema

import (
	"testing"

	"github.com/streamingfast/substreams-sink-sql/pb/test/relations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestSchema_NewSchema(t *testing.T) {
	logger := zap.NewNop()

	// Get the message descriptor from the generated protobuf code
	outputDesc := (&relations.Output{}).ProtoReflect().Descriptor()

	tests := []struct {
		name            string
		schemaName      string
		withProtoOption bool
		expectedTables  []string
	}{
		{
			name:            "schema with proto options",
			schemaName:      "test_relations",
			withProtoOption: true,
			expectedTables:  []string{"types_tests", "customers", "orders", "order_extensions", "order_items", "items"},
		},
		{
			name:            "schema without proto options",
			schemaName:      "test_relations_no_options",
			withProtoOption: false,
			expectedTables:  []string{"Output", "Entity", "TypesTest", "Customer", "Order", "NestedLevel1", "OrderExtension", "OrderItem", "Item"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema, err := NewSchema(tt.schemaName, outputDesc, tt.withProtoOption, logger)
			require.NoError(t, err)
			require.NotNil(t, schema)

			assert.Equal(t, tt.schemaName, schema.Name)
			assert.NotNil(t, schema.TableRegistry)
			assert.Equal(t, tt.withProtoOption, schema.withProtoOption)

			// Check that expected tables are created
			for _, expectedTable := range tt.expectedTables {
				table, exists := schema.TableRegistry[expectedTable]
				if tt.withProtoOption {
					// With proto options, we should have the annotated tables
					assert.True(t, exists, "Table %s should exist", expectedTable)
					if exists {
						assert.NotNil(t, table)
						assert.Equal(t, expectedTable, table.Name)
					}
				} else {
					// Without proto options, we should have all message types as tables
					if expectedTable != "Output" && expectedTable != "Entity" {
						// Skip Output and Entity as they don't have table annotations
						continue
					}
				}
			}
		})
	}
}

func TestSchema_TypesTestTable(t *testing.T) {
	logger := zap.NewNop()
	outputDesc := (&relations.Output{}).ProtoReflect().Descriptor()

	schema, err := NewSchema("test_schema", outputDesc, true, logger)
	require.NoError(t, err)

	// Test TypesTest table
	typesTestTable, exists := schema.TableRegistry["types_tests"]
	require.True(t, exists, "types_tests table should exist")
	require.NotNil(t, typesTestTable)

	assert.Equal(t, "types_tests", typesTestTable.Name)
	assert.NotNil(t, typesTestTable.Columns)

	// Test primary key field
	idColumn := findColumnByName(typesTestTable, "id")
	require.NotNil(t, idColumn, "id column should exist")
	assert.True(t, idColumn.IsPrimaryKey, "id should be primary key")

	// Test various field types
	expectedColumns := map[string]string{
		"double_field":    "double",
		"float_field":     "float",
		"int32_field":     "int32",
		"int64_field":     "int64",
		"uint32_field":    "uint32",
		"uint64_field":    "uint64",
		"bool_field":      "bool",
		"string_field":    "string",
		"bytes_field":     "bytes",
		"timestamp_field": "message",
	}

	for columnName, expectedType := range expectedColumns {
		column := findColumnByName(typesTestTable, columnName)
		assert.NotNil(t, column, "Column %s should exist", columnName)
		if column != nil {
			assert.Contains(t, column.FieldDescriptor.Kind().String(), expectedType, "Column %s should have correct type", columnName)
		}
	}

	// Test repeated fields
	repeatedColumns := []string{
		"repeated_int32_field",
		"repeated_string_field",
		"repeated_bool_field",
	}

	for _, columnName := range repeatedColumns {
		column := findColumnByName(typesTestTable, columnName)
		assert.NotNil(t, column, "Repeated column %s should exist", columnName)
		if column != nil {
			assert.True(t, column.IsRepeated, "Column %s should be marked as repeated", columnName)
		}
	}

	// Test conversion fields
	conversionColumns := []string{
		"str_2_int128",
		"str_2_uint128",
		"str_2_int256",
		"str_2_uint256",
		"str_2_decimal128",
		"str_2_decimal256",
	}

	for _, columnName := range conversionColumns {
		column := findColumnByName(typesTestTable, columnName)
		assert.NotNil(t, column, "Conversion column %s should exist", columnName)
		if column != nil {
			assert.NotNil(t, column.ConvertTo, "Column %s should have conversion", columnName)
		}
	}
}

func TestSchema_CustomerTable(t *testing.T) {
	logger := zap.NewNop()
	outputDesc := (&relations.Output{}).ProtoReflect().Descriptor()

	schema, err := NewSchema("test_schema", outputDesc, true, logger)
	require.NoError(t, err)

	customerTable, exists := schema.TableRegistry["customers"]
	require.True(t, exists, "customers table should exist")
	require.NotNil(t, customerTable)

	assert.Equal(t, "customers", customerTable.Name)

	// Test primary key
	customerIdColumn := findColumnByName(customerTable, "customer_id")
	require.NotNil(t, customerIdColumn, "customer_id column should exist")
	assert.True(t, customerIdColumn.IsPrimaryKey, "customer_id should be primary key")

	// Test regular field
	nameColumn := findColumnByName(customerTable, "name")
	require.NotNil(t, nameColumn, "name column should exist")
	assert.False(t, nameColumn.IsPrimaryKey, "name should not be primary key")
}

func TestSchema_OrderTable(t *testing.T) {
	logger := zap.NewNop()
	outputDesc := (&relations.Output{}).ProtoReflect().Descriptor()

	schema, err := NewSchema("test_schema", outputDesc, true, logger)
	require.NoError(t, err)

	orderTable, exists := schema.TableRegistry["orders"]
	require.True(t, exists, "orders table should exist")
	require.NotNil(t, orderTable)

	assert.Equal(t, "orders", orderTable.Name)

	// Test primary key
	orderIdColumn := findColumnByName(orderTable, "order_id")
	require.NotNil(t, orderIdColumn, "order_id column should exist")
	assert.True(t, orderIdColumn.IsPrimaryKey, "order_id should be primary key")

	// Test foreign key
	customerRefIdColumn := findColumnByName(orderTable, "customer_ref_id")
	require.NotNil(t, customerRefIdColumn, "customer_ref_id column should exist")
	assert.NotNil(t, customerRefIdColumn.ForeignKey, "customer_ref_id should have foreign key")
	if customerRefIdColumn.ForeignKey != nil {
		assert.Equal(t, "customers", customerRefIdColumn.ForeignKey.Table)
		assert.Equal(t, "customer_id", customerRefIdColumn.ForeignKey.TableField)
	}
}

func TestSchema_OrderExtensionTable(t *testing.T) {
	logger := zap.NewNop()
	outputDesc := (&relations.Output{}).ProtoReflect().Descriptor()

	schema, err := NewSchema("test_schema", outputDesc, true, logger)
	require.NoError(t, err)

	orderExtensionTable, exists := schema.TableRegistry["order_extensions"]
	require.True(t, exists, "order_extensions table should exist")
	require.NotNil(t, orderExtensionTable)

	assert.Equal(t, "order_extensions", orderExtensionTable.Name)
	require.NotNil(t, orderExtensionTable.ChildOf, "order_extensions should have ChildOf")
	assert.Equal(t, "orders", orderExtensionTable.ChildOf.ParentTable)
	assert.Equal(t, "`order_id`", orderExtensionTable.ChildOf.ParentTableField)
}

func TestSchema_OrderItemTable(t *testing.T) {
	logger := zap.NewNop()
	outputDesc := (&relations.Output{}).ProtoReflect().Descriptor()

	schema, err := NewSchema("test_schema", outputDesc, true, logger)
	require.NoError(t, err)

	orderItemTable, exists := schema.TableRegistry["order_items"]
	require.True(t, exists, "order_items table should exist")
	require.NotNil(t, orderItemTable)

	assert.Equal(t, "order_items", orderItemTable.Name)
	require.NotNil(t, orderItemTable.ChildOf, "order_items should have ChildOf")
	assert.Equal(t, "orders", orderItemTable.ChildOf.ParentTable)
	assert.Equal(t, "order_id", orderItemTable.ChildOf.ParentTableField)

	// Test foreign key
	itemIdColumn := findColumnByName(orderItemTable, "item_id")
	require.NotNil(t, itemIdColumn, "item_id column should exist")
	assert.NotNil(t, itemIdColumn.ForeignKey, "item_id should have foreign key")
	if itemIdColumn.ForeignKey != nil {
		assert.Equal(t, "items", itemIdColumn.ForeignKey.Table)
		assert.Equal(t, "item_id", itemIdColumn.ForeignKey.TableField)
	}
}

func TestSchema_ItemTable(t *testing.T) {
	logger := zap.NewNop()
	outputDesc := (&relations.Output{}).ProtoReflect().Descriptor()

	schema, err := NewSchema("test_schema", outputDesc, true, logger)
	require.NoError(t, err)

	itemTable, exists := schema.TableRegistry["items"]
	require.True(t, exists, "items table should exist")
	require.NotNil(t, itemTable)

	assert.Equal(t, "items", itemTable.Name)

	// Test unique field
	itemIdColumn := findColumnByName(itemTable, "item_id")
	require.NotNil(t, itemIdColumn, "item_id column should exist")
	assert.True(t, itemIdColumn.IsUnique, "item_id should be unique")
}

func TestSchema_ChangeName(t *testing.T) {
	logger := zap.NewNop()
	outputDesc := (&relations.Output{}).ProtoReflect().Descriptor()

	schema, err := NewSchema("original_name", outputDesc, true, logger)
	require.NoError(t, err)

	originalTableCount := len(schema.TableRegistry)
	assert.Equal(t, "original_name", schema.Name)

	// Change schema name
	err = schema.ChangeName("new_name")
	require.NoError(t, err)

	assert.Equal(t, "new_name", schema.Name)
	assert.Equal(t, originalTableCount, len(schema.TableRegistry), "Table count should remain the same")
}

func TestSchema_WalkMessageDescriptor(t *testing.T) {
	logger := zap.NewNop()
	outputDesc := (&relations.Output{}).ProtoReflect().Descriptor()

	schema := &Schema{
		Name:                  "test",
		TableRegistry:         make(map[string]*Table),
		logger:                logger,
		rootMessageDescriptor: outputDesc,
		withProtoOption:       true,
	}

	visitedMessages := make(map[string]int)

	err := schema.walkMessageDescriptor(outputDesc, 0, func(md protoreflect.MessageDescriptor, ordinal int) error {
		visitedMessages[string(md.Name())] = ordinal
		return nil
	})

	require.NoError(t, err)

	// Verify that all expected messages were visited
	expectedMessages := []string{"Output", "Entity", "TypesTest", "Customer", "Order", "OrderExtension", "OrderItem", "Item"}

	for _, expectedMessage := range expectedMessages {
		_, visited := visitedMessages[expectedMessage]
		assert.True(t, visited, "Message %s should have been visited", expectedMessage)
	}
}

func TestSchema_OptionalFields(t *testing.T) {
	logger := zap.NewNop()
	outputDesc := (&relations.Output{}).ProtoReflect().Descriptor()

	schema, err := NewSchema("test_schema", outputDesc, true, logger)
	require.NoError(t, err)

	typesTestTable, exists := schema.TableRegistry["types_tests"]
	require.True(t, exists)

	// Test optional fields
	optionalColumns := []string{
		"optional_string_set",
		"optional_string_not_set",
		"optional_int32_field_set",
		"optional_int32_field_not_set",
		"optional_str_2_uint256",
	}

	for _, columnName := range optionalColumns {
		column := findColumnByName(typesTestTable, columnName)
		assert.NotNil(t, column, "Optional column %s should exist", columnName)
		if column != nil {
			assert.True(t, column.IsOptional, "Column %s should be marked as optional", columnName)
		}
	}
}

func TestSchema_ErrorHandling(t *testing.T) {
	logger := zap.NewNop()
	outputDesc := (&relations.Output{}).ProtoReflect().Descriptor()

	// Test schema creation with empty name
	schema, err := NewSchema("", outputDesc, true, logger)
	require.NoError(t, err)
	assert.NotNil(t, schema)
	assert.Equal(t, "", schema.Name)
}

// Helper function to find a column by name in a table
func findColumnByName(table *Table, columnName string) *Column {
	if table == nil || table.Columns == nil {
		return nil
	}

	for _, column := range table.Columns {
		if column.Name == columnName {
			return column
		}
	}
	return nil
}
