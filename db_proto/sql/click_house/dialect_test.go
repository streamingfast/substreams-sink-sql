package clickhouse

import (
	"testing"

	"github.com/streamingfast/substreams-sink-sql/bytes"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/schema"
	"github.com/streamingfast/substreams-sink-sql/pb/test/relations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestDialectClickHouse_SchemaFromRelationsProto(t *testing.T) {
	logger := zap.NewNop()

	// Get the message descriptor from the generated protobuf code
	outputDesc := (&relations.Output{}).ProtoReflect().Descriptor()

	// Create schema from the relations.proto file
	testSchema, err := schema.NewSchema("test_relations", outputDesc, true, logger)
	require.NoError(t, err)
	require.NotNil(t, testSchema)

	// Create ClickHouse dialect (automatically initializes during construction)
	dialect, err := NewDialectClickHouse(testSchema, bytes.EncodingHex, logger)
	require.NoError(t, err)
	require.NotNil(t, dialect)

	// Test that expected tables are created
	expectedTables := []string{"types_tests", "customers", "orders", "order_extensions", "order_items", "items"}

	for _, expectedTable := range expectedTables {
		table, exists := testSchema.TableRegistry[expectedTable]
		assert.True(t, exists, "Table %s should exist in schema", expectedTable)
		assert.NotNil(t, table, "Table %s should not be nil", expectedTable)

		// Check that CREATE TABLE SQL was generated
		sql, exists := dialect.CreateTableSql[expectedTable]
		assert.True(t, exists, "CREATE TABLE SQL should exist for table %s", expectedTable)
		assert.NotEmpty(t, sql, "CREATE TABLE SQL should not be empty for table %s", expectedTable)

		// Verify SQL contains expected elements
		assert.Contains(t, sql, "CREATE TABLE IF NOT EXISTS", "SQL should contain CREATE TABLE")
		assert.Contains(t, sql, expectedTable, "SQL should contain table name")
		assert.Contains(t, sql, "ENGINE = ReplacingMergeTree", "SQL should contain ClickHouse engine")
	}
}

func TestDialectClickHouse_TypesTestTable(t *testing.T) {
	logger := zap.NewNop()
	outputDesc := (&relations.Output{}).ProtoReflect().Descriptor()

	testSchema, err := schema.NewSchema("test_relations", outputDesc, true, logger)
	require.NoError(t, err)

	dialect, err := NewDialectClickHouse(testSchema, bytes.EncodingHex, logger)
	require.NoError(t, err)

	// Test the types_tests table specifically
	sql, exists := dialect.CreateTableSql["types_tests"]
	require.True(t, exists, "CREATE TABLE SQL should exist for types_tests")
	require.NotEmpty(t, sql, "CREATE TABLE SQL should not be empty")

	// Test various field type mappings
	expectedFieldTypes := map[string]string{
		"double_field":    "Float64",
		"float_field":     "Float32",
		"int32_field":     "Int32",
		"int64_field":     "Int64",
		"uint32_field":    "UInt32",
		"uint64_field":    "UInt64",
		"bool_field":      "Bool",
		"string_field":    "VARCHAR",
		"timestamp_field": "DateTime",
	}

	for fieldName, expectedType := range expectedFieldTypes {
		assert.Contains(t, sql, fieldName, "SQL should contain field %s", fieldName)
		assert.Contains(t, sql, expectedType, "SQL should contain correct type %s for field %s", expectedType, fieldName)
	}

	// Test repeated fields (should be arrays)
	repeatedFields := []string{
		"repeated_int32_field",
		"repeated_string_field",
		"repeated_bool_field",
	}

	for _, fieldName := range repeatedFields {
		assert.Contains(t, sql, fieldName, "SQL should contain repeated field %s", fieldName)
		assert.Contains(t, sql, "Array(", "SQL should contain Array type for repeated field %s", fieldName)
	}

	// Test conversion fields
	conversionFields := map[string]string{
		"str_2_int128":     "Int128",
		"str_2_uint128":    "UInt128",
		"str_2_int256":     "Int256",
		"str_2_uint256":    "UInt256",
		"str_2_decimal128": "Decimal128(4)",
		"str_2_decimal256": "Decimal256(4)",
	}

	for fieldName, expectedType := range conversionFields {
		assert.Contains(t, sql, fieldName, "SQL should contain conversion field %s", fieldName)
		assert.Contains(t, sql, expectedType, "SQL should contain correct conversion type %s for field %s", expectedType, fieldName)
	}

	// Test primary key
	assert.Contains(t, sql, "PRIMARY KEY (id)", "SQL should contain primary key definition")
}

func TestDialectClickHouse_OrderTableWithNested(t *testing.T) {
	logger := zap.NewNop()
	outputDesc := (&relations.Output{}).ProtoReflect().Descriptor()

	testSchema, err := schema.NewSchema("test_relations", outputDesc, true, logger)
	require.NoError(t, err)

	dialect, err := NewDialectClickHouse(testSchema, bytes.EncodingHex, logger)
	require.NoError(t, err)

	// Test the types_tests table which has nested fields
	sql, exists := dialect.CreateTableSql["types_tests"]
	require.True(t, exists, "CREATE TABLE SQL should exist for types_tests")
	require.NotEmpty(t, sql, "CREATE TABLE SQL should not be empty")

	// Test that nested structure is handled with ClickHouse Nested() syntax
	assert.Contains(t, sql, "level1 Nested(", "SQL should contain nested structure using ClickHouse Nested() syntax")

	// Test ClickHouse specific options
	assert.Contains(t, sql, "ORDER BY", "SQL should contain ORDER BY clause")
	assert.Contains(t, sql, "PRIMARY KEY (id)", "SQL should contain primary key")
}

func TestDialectClickHouse_ChildTables(t *testing.T) {
	logger := zap.NewNop()
	outputDesc := (&relations.Output{}).ProtoReflect().Descriptor()

	testSchema, err := schema.NewSchema("test_relations", outputDesc, true, logger)
	require.NoError(t, err)

	dialect, err := NewDialectClickHouse(testSchema, bytes.EncodingHex, logger)
	require.NoError(t, err)

	// Test child tables (order_extensions, order_items)
	childTables := []string{"order_extensions", "order_items"}

	for _, tableName := range childTables {
		sql, exists := dialect.CreateTableSql[tableName]
		assert.True(t, exists, "CREATE TABLE SQL should exist for child table %s", tableName)
		assert.NotEmpty(t, sql, "CREATE TABLE SQL should not be empty for child table %s", tableName)

		// Child tables should have parent key field
		if tableName == "order_extensions" || tableName == "order_items" {
			assert.Contains(t, sql, "order_id", "Child table %s should contain parent key field", tableName)
		}
	}
}

func TestDialectClickHouse_SchemaHash(t *testing.T) {
	logger := zap.NewNop()
	outputDesc := (&relations.Output{}).ProtoReflect().Descriptor()

	testSchema, err := schema.NewSchema("test_relations", outputDesc, true, logger)
	require.NoError(t, err)

	dialect, err := NewDialectClickHouse(testSchema, bytes.EncodingHex, logger)
	require.NoError(t, err)

	// Test that schema hash is generated
	hash := dialect.SchemaHash()
	assert.NotEmpty(t, hash, "Schema hash should not be empty")
	assert.True(t, len(hash) > 0, "Schema hash should have content")

	// Hash should be consistent
	hash2 := dialect.SchemaHash()
	assert.Equal(t, hash, hash2, "Schema hash should be consistent")
}
