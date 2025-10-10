package parquet

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/segmentio/parquet-go"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/schema"
	pbrelations "github.com/streamingfast/substreams-sink-sql/pb/test/relations"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestNewAccumulatorInserter(t *testing.T) {
	// Get the field descriptor for a repeated int32 field from the test protobuf
	typesTestDesc := (&pbrelations.TypesTest{}).ProtoReflect().Descriptor()
	repeatedInt32FieldDesc := typesTestDesc.Fields().ByName("repeated_int32_field")

	// Create a table with a repeated int32 column
	table := &schema.Table{
		Name: "foo",
		Columns: []*schema.Column{
			{
				Name:            "bar",
				IsRepeated:      true,
				FieldDescriptor: repeatedInt32FieldDesc,
			},
		},
	}

	// Create schema with table registry
	testSchema := &schema.Schema{
		TableRegistry: map[string]*schema.Table{
			"foo": table,
		},
	}

	// Delete existing test directory if it exists
	if err := os.RemoveAll("/tmp/test_parquet"); err != nil {
		t.Fatalf("Failed to remove existing test directory: %v", err)
	}

	// Create database with parquet dialect
	dialect, err := NewDialectParquet(testSchema, "/tmp/test_parquet", zap.NewNop())
	require.NoError(t, err)

	db := &Database{
		logger:  zap.NewNop(),
		dialect: dialect,
	}

	// Create accumulator and initialize it with the database
	accumulator, err := NewAccumulatorInserter(zap.NewNop())
	require.NoError(t, err)

	err = accumulator.init(db)
	require.NoError(t, err)

	// Test inserting repeated values
	list := []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	values := []any{
		999, time.Now(), false,
		list,
	}
	err = accumulator.insert("foo", values)
	require.NoError(t, err)

	err = accumulator.flush(db)
	require.NoError(t, err)

	// Query the parquet files and display the rows
	err = queryAndDisplayParquetFiles(t, "/tmp/test_parquet", "foo")
	require.NoError(t, err)
}

// queryAndDisplayParquetFiles reads parquet files from the specified table directory and displays their contents
func queryAndDisplayParquetFiles(t *testing.T, basePath, tableName string) error {
	tableDir := filepath.Join(basePath, tableName)

	// Check if table directory exists
	if _, err := os.Stat(tableDir); os.IsNotExist(err) {
		return fmt.Errorf("table directory does not exist: %s", tableDir)
	}

	// Find all parquet files in the table directory
	files, err := filepath.Glob(filepath.Join(tableDir, "*.parquet"))
	if err != nil {
		return fmt.Errorf("finding parquet files: %w", err)
	}

	if len(files) == 0 {
		return fmt.Errorf("no parquet files found in %s", tableDir)
	}

	fmt.Printf("Found %d parquet file(s) in table '%s':\n", len(files), tableName)

	// Read and display contents of each parquet file
	for _, filePath := range files {
		fmt.Printf("\n--- Reading file: %s ---\n", filepath.Base(filePath))

		// Open parquet file
		file, err := os.Open(filePath)
		if err != nil {
			return fmt.Errorf("opening parquet file %s: %w", filePath, err)
		}
		defer file.Close()

		// Create parquet reader
		reader := parquet.NewReader(file)

		// Display schema
		schema := reader.Schema()
		fmt.Printf("Schema: %s\n", schema)

		// Read rows using a generic approach
		rowCount := 0
		for {
			// Create a map to hold the row data
			rowData := make(map[string]any)

			// Read into the map
			err := reader.Read(&rowData)
			if err != nil {
				break // End of file or error
			}

			rowCount++
			fmt.Printf("Row %d: ", rowCount)

			// Display the row data
			first := true
			for key, value := range rowData {
				if !first {
					fmt.Printf(", ")
				}
				fmt.Printf("%s=%v", key, value)
				first = false
			}
			fmt.Printf("\n")
		}

		fmt.Printf("Total rows read: %d\n", rowCount)

		reader.Close()
		file.Close()
	}

	return nil
}
