package clickhouse

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/logging/zapx"
	sql2 "github.com/streamingfast/substreams-sink-sql/db_proto/sql"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type accumulator struct {
	ordinal   int
	tableName string
	columns   map[int]string
	input     map[string]proto.ColInput
}

type AccumulatorInserter struct {
	accumulators map[string]*accumulator
	cursorStmt   *sql.Stmt
	logger       *zap.Logger
	tracer       logging.Tracer
}

func NewAccumulatorInserter(database *Database, logger *zap.Logger, tracer logging.Tracer) (*AccumulatorInserter, error) {
	logger = logger.Named("clickhouse inserter")

	accumulators, err := createAccumulators(database.dialect)
	if err != nil {
		return nil, fmt.Errorf("creating accumulators: %w", err)
	}
	return &AccumulatorInserter{
		accumulators: accumulators,
		logger:       logger,
		tracer:       tracer,
	}, nil
}

func createAccumulators(dialect *DialectClickHouse) (map[string]*accumulator, error) {
	if dialect == nil {
		panic("dialect is nil")
	}

	accumulators := map[string]*accumulator{}

	accumulators[sql2.DialectTableBlock] = &accumulator{
		ordinal:   -1,
		tableName: sql2.DialectTableBlock,
		columns: map[int]string{
			0: "number",
			1: "hash",
			2: "timestamp",
			3: "version",
			4: "deleted",
		},
		input: map[string]proto.ColInput{
			"number":    &proto.ColUInt64{},
			"hash":      &proto.ColStr{},
			"timestamp": &proto.ColDateTime{},
			"version":   &proto.ColInt64{},
			"deleted":   &proto.ColBool{},
		},
	}

	tables := dialect.GetTables()
	for _, table := range tables {
		input := map[string]proto.ColInput{}
		columns := map[int]string{}

		input[sql2.DialectFieldBlockNumber] = &proto.ColUInt64{}
		columns[0] = sql2.DialectFieldBlockNumber

		input[sql2.DialectFieldBlockTimestamp] = &proto.ColDateTime{}
		columns[1] = sql2.DialectFieldBlockTimestamp

		input[sql2.DialectFieldVersion] = &proto.ColInt64{}
		columns[2] = sql2.DialectFieldVersion

		input[sql2.DialectFieldDeleted] = &proto.ColBool{}
		columns[3] = sql2.DialectFieldDeleted

		primaryName := ""
		if table.PrimaryKey != nil {
			pk := table.PrimaryKey
			primaryName = pk.Name

			input[pk.Name] = ColInputForColumn(pk.FieldDescriptor)
			columns[4] = pk.Name
		}

		offset := len(columns)
		if table.ChildOf != nil {
			parentTable, parentFound := dialect.TableRegistry[table.ChildOf.ParentTable]
			if !parentFound {
				return nil, fmt.Errorf("parent table %q not found", table.ChildOf.ParentTable)
			}
			fieldFound := false
			for _, parentField := range parentTable.Columns {

				if parentField.Name == table.ChildOf.ParentTableField {
					input[parentField.Name] = ColInputForColumn(parentField.FieldDescriptor)
					columns[offset] = parentField.Name
					fieldFound = true
					break
				}
			}
			if !fieldFound {
				return nil, fmt.Errorf("field %q not found in table %q", table.ChildOf.ParentTableField, table.ChildOf.ParentTable)
			}
		}

		offset = len(columns)
		skipCount := 0
		for i, column := range table.Columns {
			if column.Name == primaryName {
				skipCount++
				continue
			}
			input[column.Name] = ColInputForColumn(column.FieldDescriptor)
			columns[i+offset-skipCount] = column.Name
		}

		accumulators[table.Name] = &accumulator{
			tableName: table.Name,
			ordinal:   table.Ordinal,
			columns:   columns,
			input:     input,
		}
	}

	return accumulators, nil
}

func (i *AccumulatorInserter) insert(table string, values []any) error {
	accumulator := i.accumulators[table]
	if accumulator == nil {
		return fmt.Errorf("accumulator not found for table %q", table)
	}
	i.logger.Debug("inserting", zap.String("table", table), zap.Int("values", len(values)))
	for idx, value := range values {
		colName, found := accumulator.columns[idx]
		if !found {
			return fmt.Errorf("column %q not found for table %q at idx %d", colName, table, idx)
		}
		input := accumulator.input[colName]

		if i.tracer.Enabled() {
			i.logger.Debug("inserting column value",
				zap.String("table", table),
				zap.String("column", colName),
				zapx.Type("column_type", input),
				zapx.Type("value_type", value),
			)
		}

		switch input := input.(type) {
		case *proto.ColDateTime:
			if t, ok := value.(*timestamppb.Timestamp); ok {
				input.Append(t.AsTime())
			} else if t, ok := value.(time.Time); ok {
				input.Append(t)
			} else {
				panic(fmt.Sprintf("unknown time base input type %T for column %s of table %s", input, colName, table))
			}
		case *proto.ColInt32:
			input.Append(value.(int32))
		case *proto.ColInt64:
			input.Append(value.(int64))
		case *proto.ColUInt32:
			input.Append(value.(uint32))
		case *proto.ColUInt64:
			input.Append(value.(uint64))
		case *proto.ColFloat32:
			input.Append(value.(float32))
		case *proto.ColFloat64:
			input.Append(value.(float64))
		case *proto.ColStr:
			input.Append(value.(string))
		case *proto.ColBytes:
			input.Append(value.([]byte))
		case *proto.ColBool:
			input.Append(value.(bool))
		default:
			panic(fmt.Sprintf("unknown input type %T for column %s of table %s", input, colName, table))
		}
	}

	return nil
}

func (i *AccumulatorInserter) flush(database *Database) error {
	i.logger.Debug("flushing started", zap.Int("accumulators", len(i.accumulators)))
	var accumulators []accumulator

	start := time.Now()
	for _, acc := range i.accumulators {
		accumulators = append(accumulators, *acc)
	}

	sort.Slice(accumulators, func(i, j int) bool {
		return accumulators[i].ordinal < accumulators[j].ordinal
	})

	client, err := database.client()
	if err != nil {
		return fmt.Errorf("clickhouse accumulator inserter: creating client: %w", err)
	}

	queryDuration := time.Duration(0)

	rowCount := 0
	for _, acc := range accumulators {
		qStart := time.Now()

		input := proto.Input{}
		for n, i := range acc.input {
			if n == "block_number" {
				rowCount += i.Rows()
			}
			input = append(input, proto.InputColumn{
				Name: n,
				Data: i,
			})
		}

		if err := client.Do(database.ctx, ch.Query{
			Body:  input.Into(acc.tableName), // helper that generates INSERT INTO query with all columns
			Input: input,
		}); err != nil {
			return fmt.Errorf("clickhouse accumulator inserter: executing query on %q: %w", acc.debugTableAndColumns(), err)
		}

		queryDuration += time.Since(qStart)
	}

	//reset
	accs, err := createAccumulators(database.dialect)
	if err != nil {
		return fmt.Errorf("clickhouse accumulator inserter: creating accumulators: %w", err)
	}
	i.accumulators = accs

	i.logger.Debug("flushing done", zap.Duration("duration", time.Since(start)), zap.Int("rows", rowCount))

	return nil
}

func (acc *accumulator) debugTableAndColumns() string {
	var b strings.Builder
	b.WriteString(acc.tableName)
	b.WriteString(" (")
	for idx, col := range acc.columns {
		if idx > 0 {
			b.WriteString(", ")
		}
		b.WriteString(col)
	}
	b.WriteString(")")
	return b.String()
}
