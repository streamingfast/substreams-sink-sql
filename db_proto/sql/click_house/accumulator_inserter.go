package clickhouse

import (
	"database/sql"
	"fmt"
	"sort"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"go.uber.org/zap"
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
}

func NewAccumulatorInserter(database *Database, logger *zap.Logger) (*AccumulatorInserter, error) {
	logger = logger.Named("clickhouse inserter")

	accumulators, err := createAccumulators(database.dialect)
	if err != nil {
		return nil, fmt.Errorf("creating accumulators: %w", err)
	}
	return &AccumulatorInserter{
		accumulators: accumulators,
		logger:       logger,
	}, nil
}

func createAccumulators(dialect *DialectClickHouse) (map[string]*accumulator, error) {
	accumulators := map[string]*accumulator{}

	accumulators["_blocks_"] = &accumulator{
		ordinal:   -1,
		tableName: "_blocks_",
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

		input["block_number"] = &proto.ColUInt64{}
		columns[0] = "block_number"
		input["block_timestamp"] = &proto.ColDateTime{}
		columns[1] = "block_timestamp"
		input["version"] = &proto.ColInt64{}
		columns[2] = "version"
		input["deleted"] = &proto.ColBool{}
		columns[3] = "deleted"

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
			return fmt.Errorf("column not found for table %q at idx %d", table, idx)
		}
		input := accumulator.input[colName]

		switch input := input.(type) {
		case *proto.ColDateTime:
			input.Append(value.(time.Time))
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

const maxErrorQueryLength = 256

func (i *AccumulatorInserter) flush(database *Database) error {
	i.logger.Info("flushing started", zap.Int("accumulators", len(i.accumulators)))
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
		return fmt.Errorf("clickhouse accumalator inserter: creating client: %w", err)
	}

	queryDuration := time.Duration(0)

	for _, acc := range accumulators {
		qStart := time.Now()

		input := proto.Input{}
		for n, i := range acc.input {
			input = append(input, proto.InputColumn{
				Name: n,
				Data: i,
			})
		}

		if err := client.Do(database.ctx, ch.Query{
			Body:  input.Into(acc.tableName), // helper that generates INSERT INTO query with all columns
			Input: input,
		}); err != nil {
			return fmt.Errorf("clickhouse accumalator inserter: executing query: %w", err)
		}

		queryDuration += time.Since(qStart)
	}

	//reset
	accs, err := createAccumulators(database.dialect)
	if err != nil {
		return fmt.Errorf("clickhouse accumalator inserter: creating accumulators: %w", err)
	}
	i.accumulators = accs

	i.logger.Info("flushing done", zap.Duration("duration", time.Since(start)), zap.Duration("query_duration", queryDuration))

	return nil
}
