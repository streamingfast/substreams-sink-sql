package clickhouse

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	sql2 "github.com/streamingfast/substreams-sink-sql/db_proto/sql"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/schema"
	"go.uber.org/zap"
)

type accumulator struct {
	ordinal   int
	tableName string
	query     string
	rowValues [][]string
}

type AccumulatorInserter struct {
	accumulators map[string]*accumulator
	cursorStmt   *sql.Stmt
	logger       *zap.Logger
}

func NewAccumulatorInserter(database *Database, logger *zap.Logger) (*AccumulatorInserter, error) {
	logger = logger.Named("postgres inserter")
	tables := database.Dialect.GetTables()
	accumulators := map[string]*accumulator{}

	for _, table := range tables {
		query, err := createInsertFromDescriptorAcc(table, database.Dialect)
		if err != nil {
			return nil, fmt.Errorf("creating insert from descriptor for table %q: %w", table.Name, err)
		}
		accumulators[table.Name] = &accumulator{
			tableName: table.Name,
			ordinal:   table.Ordinal,
			query:     query,
		}
	}
	accumulators["_blocks_"] = &accumulator{
		ordinal: -1,
		query:   fmt.Sprintf("INSERT INTO %s (number, hash, timestamp, version, deleted) VALUES ", tableName(database.schemaName, "_blocks_")),
	}

	//cursorQuery := fmt.Sprintf("INSERT INTO %s (name, cursor) VALUES ($1, $2) ON CONFLICT (name) DO UPDATE SET cursor = $2", tableName(database.schemaName, "cursor"))
	//cs, err := database.DB.Prepare(cursorQuery)
	//if err != nil {
	//	return nil, fmt.Errorf("preparing statement %q: %w", cursorQuery, err)
	//}

	return &AccumulatorInserter{
		//cursorStmt:   cs,
		accumulators: accumulators,
		logger:       logger,
	}, nil
}

func createInsertFromDescriptorAcc(table *schema.Table, dialect sql2.Dialect) (string, error) {
	tableName := dialect.FullTableName(table)
	fields := table.Columns

	var fieldNames []string
	fieldNames = append(fieldNames, "block_number")
	fieldNames = append(fieldNames, "block_timestamp")
	fieldNames = append(fieldNames, "version")
	fieldNames = append(fieldNames, "deleted")

	if pk := table.PrimaryKey; pk != nil {
		fieldNames = append(fieldNames, pk.Name)
	}

	if table.ChildOf != nil {
		fieldNames = append(fieldNames, table.ChildOf.ParentTableField)
	}

	for _, field := range fields {
		if table.PrimaryKey != nil && field.Name == table.PrimaryKey.Name {
			continue
		}

		if field.IsRepeated || field.IsExtension { //not a direct child
			continue
		}
		fieldNames = append(fieldNames, field.Name)
	}

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES ",
		tableName,
		strings.Join(fieldNames, ", "),
	), nil

}

func (i *AccumulatorInserter) Insert(table string, values []any, txWrapper func(stmt *sql.Stmt) *sql.Stmt) error {
	var v []string
	for _, value := range values {
		v = append(v, ValueToString(value))
	}
	accumulator := i.accumulators[table]
	if accumulator == nil {
		return fmt.Errorf("accumulator not found for table %q", table)
	}
	accumulator.rowValues = append(accumulator.rowValues, v)

	return nil
}

const maxErrorQueryLength = 256

func truncateQuery(query string) string {
	if len(query) <= maxErrorQueryLength {
		return query
	}
	return query[:maxErrorQueryLength] + "..."
}

func (i *AccumulatorInserter) Flush(tx *sql.Tx) error {
	var accumulators []accumulator

	start := time.Now()
	for _, acc := range i.accumulators {
		accumulators = append(accumulators, *acc)
	}

	sort.Slice(accumulators, func(i, j int) bool {
		return accumulators[i].ordinal < accumulators[j].ordinal
	})

	queryDuration := time.Duration(0)
	for _, acc := range accumulators {
		i.logger.Debug("flushing table", zap.String("table", acc.tableName), zap.Int("ordinal", acc.ordinal), zap.Int("row_count", len(acc.rowValues)))
		if len(acc.rowValues) == 0 {
			continue
		}
		insert := acc.query
		var b strings.Builder
		b.WriteString(acc.query)
		for _, values := range acc.rowValues {
			b.WriteString("(")
			b.WriteString(strings.Join(values, ","))
			b.WriteString("),")
		}
		insert = strings.Trim(b.String(), ",")

		qStart := time.Now()
		_, err := tx.Exec(insert)
		if err != nil {
			return fmt.Errorf("clickhouse accumalator inserter: executing insert %s: %w", truncateQuery(insert), err)
		}
		queryDuration += time.Since(qStart)
	}

	//reset
	for _, acc := range i.accumulators {
		acc.rowValues = acc.rowValues[:0]
	}

	i.logger.Info("flushing done", zap.Duration("duration", time.Since(start)), zap.Duration("query_duration", queryDuration))

	return nil
}
