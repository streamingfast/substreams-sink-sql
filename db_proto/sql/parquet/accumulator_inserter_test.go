package parquet

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/segmentio/parquet-go"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/schema"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestAccumulatorInserter_SystemColumns(t *testing.T) {
	logger := zap.NewNop()

	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "parquet_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create a simple table schema
	table := &schema.Table{
		Name: "test_table",
		Columns: []*schema.Column{
			{Name: "id"},
			{Name: "name"},
		},
		PrimaryKey: &schema.PrimaryKey{Name: "id"},
	}

	tableRegistry := map[string]*schema.Table{
		"test_table": table,
	}

	// Create dialect
	dialect, err := NewDialectParquet(&schema.Schema{TableRegistry: tableRegistry}, tempDir, logger)
	require.NoError(t, err)

	// Create accumulator inserter
	inserter, err := NewAccumulatorInserter(logger)
	require.NoError(t, err)

	// Create database
	db := &Database{
		basePath: tempDir,
		logger:   logger,
		dialect:  dialect,
		inserter: inserter,
	}

	// Initialize inserter
	err = inserter.init(db)
	require.NoError(t, err)

	// Insert test data - values should be: [block_number, block_timestamp, deleted, primary_key, other_columns...]
	testTime := time.Now()
	values := []any{
		uint64(12345), // _block_number_
		testTime,      // _block_timestamp_
		false,         // _deleted_
		int64(1),      // id (primary key)
		"test_name",   // name
	}

	// Insert the row
	err = inserter.insert("test_table", values)
	require.NoError(t, err)

	// Flush to write parquet file
	err = inserter.flush(db)
	require.NoError(t, err)

	// Find the parquet file
	tableDir := filepath.Join(tempDir, "test_table")
	entries, err := os.ReadDir(tableDir)
	require.NoError(t, err)
	require.Len(t, entries, 1)

	parquetFile := filepath.Join(tableDir, entries[0].Name())

	// Read the parquet file and check schema
	file, err := os.Open(parquetFile)
	require.NoError(t, err)
	defer file.Close()

	reader := parquet.NewReader(file)
	defer reader.Close()

	schema := reader.Schema()
	fields := schema.Fields()

	// Check that system columns are present
	fieldNames := make([]string, len(fields))
	for i, field := range fields {
		fieldNames[i] = field.Name()
	}

	require.Contains(t, fieldNames, "_block_number_", "System column _block_number_ should be present")
	require.Contains(t, fieldNames, "_block_timestamp_", "System column _block_timestamp_ should be present")
	require.Contains(t, fieldNames, "_deleted_", "System column _deleted_ should be present")
	require.Contains(t, fieldNames, "id", "Primary key column id should be present")
	require.Contains(t, fieldNames, "name", "Regular column name should be present")

	// Read the row and verify values
	rows := make([]parquet.Row, 1)
	_, err = reader.ReadRows(rows)
	require.NoError(t, err)

	row := rows[0]
	require.Len(t, row, len(fields))

	// Find column indices
	blockNumIdx := -1
	blockTimeIdx := -1
	deletedIdx := -1
	idIdx := -1
	nameIdx := -1

	for i, field := range fields {
		switch field.Name() {
		case "_block_number_":
			blockNumIdx = i
		case "_block_timestamp_":
			blockTimeIdx = i
		case "_deleted_":
			deletedIdx = i
		case "id":
			idIdx = i
		case "name":
			nameIdx = i
		}
	}

	require.NotEqual(t, -1, blockNumIdx, "Should find _block_number_ column")
	require.NotEqual(t, -1, blockTimeIdx, "Should find _block_timestamp_ column")
	require.NotEqual(t, -1, deletedIdx, "Should find _deleted_ column")
	require.NotEqual(t, -1, idIdx, "Should find id column")
	require.NotEqual(t, -1, nameIdx, "Should find name column")

	// Verify values
	blockNumStr := row[blockNumIdx].String()
	blockNumParsed, _ := strconv.ParseUint(blockNumStr, 10, 64)
	require.Equal(t, uint64(12345), blockNumParsed, "Block number should match")

	require.Equal(t, "false", row[deletedIdx].String(), "Deleted flag should be false")
	require.Equal(t, "1", row[idIdx].String(), "ID should match")
	require.Equal(t, "test_name", row[nameIdx].String(), "Name should match")

	// Timestamp check - parquet may store as different precision
	blockTimeStr := row[blockTimeIdx].String()
	blockTimeParsed, _ := strconv.ParseInt(blockTimeStr, 10, 64)
	require.True(t, blockTimeParsed >= testTime.UnixMilli()*1000000 && blockTimeParsed <= testTime.UnixNano(), "Block timestamp should be in valid range")
}
