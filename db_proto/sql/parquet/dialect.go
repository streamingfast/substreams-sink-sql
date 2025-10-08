package parquet

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/streamingfast/substreams-sink-sql/db_proto/sql"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/schema"
	"go.uber.org/zap"
)

type DialectParquet struct {
	*sql.BaseDialect
	basePath string
	logger   *zap.Logger
}

func NewDialectParquet(schema *schema.Schema, basePath string, logger *zap.Logger) (*DialectParquet, error) {
	d := &DialectParquet{
		BaseDialect: sql.NewBaseDialect(schema.TableRegistry, logger),
		basePath:    basePath,
		logger:      logger,
	}

	err := d.init()
	if err != nil {
		return nil, fmt.Errorf("initializing parquet dialect: %w", err)
	}

	for _, table := range schema.TableRegistry {
		err := d.createTableDirectory(table)
		if err != nil {
			return nil, fmt.Errorf("handling table %q: %w", table.Name, err)
		}
	}

	return d, nil
}

func (d *DialectParquet) UseVersionField() bool {
	return false // Parquet handles versioning through file structure
}

func (d *DialectParquet) UseDeletedField() bool {
	return true // Include _deleted_ field for consistency with other implementations
}

func (d *DialectParquet) init() error {
	// Ensure base directory exists
	if err := os.MkdirAll(d.basePath, 0755); err != nil {
		return fmt.Errorf("creating base directory %q: %w", d.basePath, err)
	}
	return nil
}

func (d *DialectParquet) createTableDirectory(table *schema.Table) error {
	tablePath := d.FullTableName(table)

	// Create table directory
	if err := os.MkdirAll(tablePath, 0755); err != nil {
		return fmt.Errorf("creating table directory %q: %w", tablePath, err)
	}

	// For parquet, we don't generate SQL, but we can store the schema information
	// This could be used for validation or metadata
	d.AddCreateTableSql(table.Name, fmt.Sprintf("CREATE DIRECTORY %s", tablePath))

	return nil
}

func (d *DialectParquet) FullTableName(table *schema.Table) string {
	return filepath.Join(d.basePath, table.Name)
}

func (d *DialectParquet) SchemaHash() string {
	h := sha256.New()

	// Hash table directory structure
	var tableDirs []string
	for tableName := range d.TableRegistry {
		tableDirs = append(tableDirs, tableName)
	}
	sort.Strings(tableDirs)

	for _, tableName := range tableDirs {
		tablePath := filepath.Join(d.basePath, tableName)
		if info, err := os.Stat(tablePath); err == nil && info.IsDir() {
			h.Write([]byte(tableName))
			h.Write([]byte(tablePath))
		}
	}

	data := h.Sum(nil)
	return fmt.Sprintf("%x", data)
}

// GetTablePath returns the full path to a table's directory
func (d *DialectParquet) GetTablePath(tableName string) string {
	return filepath.Join(d.basePath, tableName)
}

// EnsureTableDirectory ensures the table directory exists
func (d *DialectParquet) EnsureTableDirectory(tableName string) error {
	tablePath := d.GetTablePath(tableName)
	return os.MkdirAll(tablePath, 0755)
}
