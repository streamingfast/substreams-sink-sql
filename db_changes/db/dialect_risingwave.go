package db

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/streamingfast/cli"
	sink "github.com/streamingfast/substreams-sink"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
)

type RisingwaveDialect struct {
	cursorTableName  string
	historyTableName string
	schemaName       string
}

func NewRisingwaveDialect(schemaName string, cursorTableName string, historyTableName string) *RisingwaveDialect {
	return &RisingwaveDialect{
		cursorTableName:  cursorTableName,
		historyTableName: historyTableName,
		schemaName:       schemaName,
	}
}

func (d RisingwaveDialect) Revert(tx Tx, ctx context.Context, l *Loader, lastValidFinalBlock uint64) error {
	query := fmt.Sprintf(`SELECT op,table_name,pk,prev_value,block_num FROM %s WHERE "block_num" > %d ORDER BY "block_num" DESC`,
		d.historyTable(d.schemaName),
		lastValidFinalBlock,
	)

	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return err
	}

	var reversions []func() error
	l.logger.Info("reverting forked block block(s)", zap.Uint64("last_valid_final_block", lastValidFinalBlock))
	if rows != nil { // rows will be nil with no error only in testing scenarios
		defer rows.Close()
		for rows.Next() {
			var op string
			var table_name string
			var pk string
			var prev_value_nullable sql.NullString
			var block_num uint64
			if err := rows.Scan(&op, &table_name, &pk, &prev_value_nullable, &block_num); err != nil {
				return fmt.Errorf("scanning row: %w", err)
			}
			l.logger.Debug("reverting", zap.String("operation", op), zap.String("table_name", table_name), zap.String("pk", pk), zap.Uint64("block_num", block_num))
			prev_value := prev_value_nullable.String

			// we can't call revertOp inside this loop, because it calls tx.ExecContext,
			// which can't run while this query is "active" or it will silently discard the remaining rows!
			reversions = append(reversions, func() error {
				if err := d.revertOp(tx, ctx, op, table_name, pk, prev_value, block_num); err != nil {
					return fmt.Errorf("revertOp: %w", err)
				}
				return nil
			})
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterating on rows from query %q: %w", query, err)
		}
		for _, reversion := range reversions {
			if err := reversion(); err != nil {
				return fmt.Errorf("execution revert operation: %w", err)
			}
		}
	}
	pruneHistory := fmt.Sprintf(`DELETE FROM %s WHERE "block_num" > %d;`,
		d.historyTable(d.schemaName),
		lastValidFinalBlock,
	)

	_, err = tx.ExecContext(ctx, pruneHistory)
	if err != nil {
		return fmt.Errorf("executing pruneHistory: %w", err)
	}
	return nil
}

func (d RisingwaveDialect) Flush(tx Tx, ctx context.Context, l *Loader, outputModuleHash string, lastFinalBlock uint64) (int, error) {
	var rowCount int
	for entriesPair := l.entries.Oldest(); entriesPair != nil; entriesPair = entriesPair.Next() {
		tableName := entriesPair.Key
		entries := entriesPair.Value

		if l.tracer.Enabled() {
			l.logger.Debug("flushing table rows", zap.String("table_name", tableName), zap.Int("row_count", entries.Len()))
		}
		for entryPair := entries.Oldest(); entryPair != nil; entryPair = entryPair.Next() {
			entry := entryPair.Value

			query, err := d.prepareStatement(d.schemaName, entry)
			if err != nil {
				return 0, fmt.Errorf("failed to prepare statement: %w", err)
			}

			if l.tracer.Enabled() {
				l.logger.Debug("adding query from operation to transaction", zap.Stringer("op", entry), zap.String("query", query))
			}

			if _, err := tx.ExecContext(ctx, query); err != nil {
				return 0, fmt.Errorf("executing query %q: %w", query, err)
			}
		}
		rowCount += entries.Len()
	}

	if err := d.pruneReversibleSegment(tx, ctx, d.schemaName, lastFinalBlock); err != nil {
		return 0, err
	}

	return rowCount, nil
}

func (d RisingwaveDialect) revertOp(tx Tx, ctx context.Context, op, escaped_table_name, pk, prev_value string, block_num uint64) error {

	pkmap := make(map[string]string)
	if err := json.Unmarshal([]byte(pk), &pkmap); err != nil {
		return fmt.Errorf("revertOp: unmarshalling %q: %w", pk, err)
	}
	switch op {
	case "I":
		query := fmt.Sprintf(`DELETE FROM %s WHERE %s;`,
			escaped_table_name,
			getPrimaryKeyWhereClause(pkmap, ""),
		)
		if _, err := tx.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("executing revert query %q: %w", query, err)
		}
	case "D":
		query := fmt.Sprintf(`INSERT INTO %s SELECT * FROM jsonb_populate_record(null::%s,%s);`,
			escaped_table_name,
			escaped_table_name,
			escapeStringValue(prev_value),
		)
		if _, err := tx.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("executing revert query %q: %w", query, err)
		}

	case "U":
		columns, err := sqlColumnNamesFromJSON(prev_value)
		if err != nil {
			return err
		}

		query := fmt.Sprintf(`UPDATE %s SET(%s)=((SELECT %s FROM jsonb_populate_record(null::%s,%s))) WHERE %s;`,
			escaped_table_name,
			columns,
			columns,
			escaped_table_name,
			escapeStringValue(prev_value),
			getPrimaryKeyWhereClause(pkmap, ""),
		)
		if _, err := tx.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("executing revert query %q: %w", query, err)
		}
	default:
		panic("invalid op in revert command")
	}
	return nil
}

func (d RisingwaveDialect) pruneReversibleSegment(tx Tx, ctx context.Context, schema string, highestFinalBlock uint64) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE block_num <= %d;`, d.historyTable(schema), highestFinalBlock)
	if _, err := tx.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("executing prune query %q: %w", query, err)
	}
	return nil
}

func (d RisingwaveDialect) GetCreateCursorQuery(schema string, withPostgraphile bool) string {
	out := fmt.Sprintf(cli.Dedent(`
		create table if not exists %s.%s
		(
			id         varchar not null constraint %s primary key,
			cursor     varchar,
			block_num  bigint,
			block_id   varchar
		);
		`), EscapeIdentifier(schema), EscapeIdentifier(d.cursorTableName), EscapeIdentifier(d.cursorTableName+"_pk"))
	if withPostgraphile {
		out += fmt.Sprintf("COMMENT ON TABLE %s.%s IS E'@omit';",
			EscapeIdentifier(schema), EscapeIdentifier(d.cursorTableName))
	}
	return out
}

func (d RisingwaveDialect) GetCreateHistoryQuery(schema string, withPostgraphile bool) string {
	out := fmt.Sprintf(cli.Dedent(`
		create table if not exists %s
		(
            id           BIGINT NOT NULL,
            op           CHARACTER VARYING,
            table_name   varchar,
			pk           varchar,
            prev_value   varchar,
			block_num    bigint,
			PRIMARY KEY (id)
		);
		`),
		d.historyTable(schema),
	)
	if withPostgraphile {
		out += fmt.Sprintf("COMMENT ON TABLE %s.%s IS E'@omit';",
			EscapeIdentifier(schema), EscapeIdentifier(d.historyTableName))
	}
	return out
}

func (d RisingwaveDialect) ExecuteSetupScript(ctx context.Context, l *Loader, schemaSql string) error {
	if _, err := l.ExecContext(ctx, schemaSql); err != nil {
		return fmt.Errorf("exec schemaName: %w", err)
	}
	return nil
}

func (d RisingwaveDialect) GetUpdateCursorQuery(table, moduleHash string, cursor *sink.Cursor, block_num uint64, block_id string) string {
	return query(`
		UPDATE %s set "cursor" = '%s', block_num = %d, block_id = '%s' WHERE id = '%s';
	`, table, cursor, block_num, block_id, moduleHash)
}

func (d RisingwaveDialect) GetAllCursorsQuery(table string) string {
	return fmt.Sprintf("SELECT id, cursor, block_num, block_id FROM %s", table)
}

func (d RisingwaveDialect) ParseDatetimeNormalization(value string) string {
	return escapeStringValue(value)
}

func (d RisingwaveDialect) DriverSupportRowsAffected() bool {
	return true
}

func (d RisingwaveDialect) OnlyInserts() bool {
	return false
}

func (d RisingwaveDialect) AllowPkDuplicates() bool {
	return false
}

func (d RisingwaveDialect) CreateUser(tx Tx, ctx context.Context, l *Loader, username string, password string, database string, readOnly bool) error {
	user, pass, db := EscapeIdentifier(username), password, EscapeIdentifier(database)
	var q string
	if readOnly {
		q = fmt.Sprintf(`
            CREATE ROLE %s LOGIN PASSWORD '%s';
            GRANT CONNECT ON DATABASE %s TO %s;
            GRANT USAGE ON SCHEMA public TO %s;
            ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO %s;
            GRANT SELECT ON ALL TABLES IN SCHEMA public TO %s;
        `, user, pass, db, user, user, user, user)
	} else {
		q = fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s'; GRANT ALL PRIVILEGES ON DATABASE %s TO %s;", user, pass, db, user)
	}

	_, err := tx.ExecContext(ctx, q)
	if err != nil {
		return fmt.Errorf("executing query %q: %w", q, err)
	}

	return nil
}

func (d RisingwaveDialect) historyTable(schema string) string {
	return fmt.Sprintf("%s.%s", EscapeIdentifier(schema), EscapeIdentifier(d.historyTableName))
}

// generateHistoryID creates a unique ID for history table entries
// using block number and a hash of the operation details
func (d RisingwaveDialect) generateHistoryID(blockNum uint64, op, pk string) int64 {
	// Create a hash from the operation details to ensure uniqueness within a block
	hasher := sha256.New()
	hasher.Write([]byte(fmt.Sprintf("%s:%s:%d", op, pk, time.Now().UnixNano())))
	hash := hasher.Sum(nil)

	// Use first 4 bytes of hash as a 32-bit number
	hashInt := int64(hash[0])<<24 | int64(hash[1])<<16 | int64(hash[2])<<8 | int64(hash[3])

	// Combine block number (shifted left) with hash to ensure global uniqueness
	// Block number in high bits, hash in low bits
	return (int64(blockNum) << 32) | (hashInt & 0xFFFFFFFF)
}

func (d RisingwaveDialect) saveInsert(schema string, table string, primaryKey map[string]string, blockNum uint64) string {
	// Generate unique ID using block number and hash of primary key
	id := d.generateHistoryID(blockNum, "I", primaryKeyToJSON(primaryKey))
	return fmt.Sprintf(`INSERT INTO %s (id,op,table_name,pk,block_num) values (%d,%s,%s,%s,%d);`,
		d.historyTable(schema),
		id,
		escapeStringValue("I"),
		escapeStringValue(table),
		escapeStringValue(primaryKeyToJSON(primaryKey)),
		blockNum,
	)
}

/*
with t as (select 'default' id)
select CASE WHEN block_meta.id is null THEN 'I' ELSE 'U' END AS op, '"public"."block_meta"', 'allo', row_to_json(block_meta),10  from t left join block_meta on block_meta.id='default';
*/
func (d RisingwaveDialect) saveUpsert(schema string, escapedTableName string, primaryKey map[string]string, blockNum uint64) string {
	schemaAndTable := fmt.Sprintf("%s.%s", EscapeIdentifier(schema), escapedTableName)
	// Generate unique ID for this upsert operation
	id := d.generateHistoryID(blockNum, "U", primaryKeyToJSON(primaryKey))

	return fmt.Sprintf(`
		WITH t as (select %s)
		INSERT INTO %s (id,op,table_name,pk,prev_value,block_num)
		SELECT %d, CASE WHEN %s THEN 'I' ELSE 'U' END AS op, %s, %s, to_jsonb(%s),%d from t left join %s.%s on %s;`,

		getPrimaryKeyFakeEmptyValues(primaryKey),
		d.historyTable(schema),
		id,

		getPrimaryKeyFakeEmptyValuesAssertion(primaryKey, escapedTableName),

		escapeStringValue(schemaAndTable), escapeStringValue(primaryKeyToJSON(primaryKey)), escapedTableName, blockNum,
		EscapeIdentifier(schema), escapedTableName,
		getPrimaryKeyWhereClause(primaryKey, escapedTableName),
	)

}

func (d RisingwaveDialect) saveUpdate(schema string, escapedTableName string, primaryKey map[string]string, blockNum uint64) string {
	return d.saveRow("U", schema, escapedTableName, primaryKey, blockNum)
}

func (d RisingwaveDialect) saveDelete(schema string, escapedTableName string, primaryKey map[string]string, blockNum uint64) string {
	return d.saveRow("D", schema, escapedTableName, primaryKey, blockNum)
}

func (d RisingwaveDialect) saveRow(op, schema, escapedTableName string, primaryKey map[string]string, blockNum uint64) string {
	schemaAndTable := fmt.Sprintf("%s.%s", EscapeIdentifier(schema), escapedTableName)
	// Generate unique ID for this operation
	id := d.generateHistoryID(blockNum, op, primaryKeyToJSON(primaryKey))
	return fmt.Sprintf(`INSERT INTO %s (id,op,table_name,pk,prev_value,block_num) SELECT %d,%s,%s,%s,to_jsonb(%s),%d FROM %s.%s WHERE %s;`,
		d.historyTable(schema),
		id,
		escapeStringValue(op), escapeStringValue(schemaAndTable), escapeStringValue(primaryKeyToJSON(primaryKey)), escapedTableName, blockNum,
		EscapeIdentifier(schema), escapedTableName,
		getPrimaryKeyWhereClause(primaryKey, ""),
	)

}

func (d *RisingwaveDialect) prepareStatement(schema string, o *Operation) (string, error) {
	var columns, values []string
	if o.opType == OperationTypeInsert || o.opType == OperationTypeUpsert || o.opType == OperationTypeUpdate {
		var err error
		columns, values, err = d.prepareColValues(o.table, o.data)
		if err != nil {
			return "", fmt.Errorf("preparing column & values: %w", err)
		}
	}

	if o.opType == OperationTypeUpsert || o.opType == OperationTypeUpdate || o.opType == OperationTypeDelete {
		// A table without a primary key set yield a `primaryKey` map with a single entry where the key is an empty string
		if _, found := o.primaryKey[""]; found {
			return "", fmt.Errorf("trying to perform %s operation but table %q don't have a primary key set, this is not accepted", o.opType, o.table.name)
		}
	}

	switch o.opType {
	case OperationTypeInsert:
		insertQuery := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);",
			o.table.identifier,
			strings.Join(columns, ","),
			strings.Join(values, ","),
		)
		if o.reversibleBlockNum != nil {
			return d.saveInsert(schema, o.table.identifier, o.primaryKey, *o.reversibleBlockNum) + insertQuery, nil
		}
		return insertQuery, nil

	case OperationTypeUpsert:
		updates := make([]string, len(columns))
		for i := range columns {
			updates[i] = fmt.Sprintf("%s=EXCLUDED.%s", columns[i], columns[i])
		}

		insertQuery := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s;",
			o.table.identifier,
			strings.Join(columns, ","),
			strings.Join(values, ","),
			strings.Join(maps.Keys(o.primaryKey), ","),
			strings.Join(updates, ", "),
		)

		if o.reversibleBlockNum != nil {
			return d.saveUpsert(schema, o.table.nameEscaped, o.primaryKey, *o.reversibleBlockNum) + insertQuery, nil
		}
		return insertQuery, nil

	case OperationTypeUpdate:
		updates := make([]string, len(columns))
		for i := 0; i < len(columns); i++ {
			updates[i] = fmt.Sprintf("%s=%s", columns[i], values[i])
		}

		primaryKeySelector := getPrimaryKeyWhereClause(o.primaryKey, "")

		updateQuery := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
			o.table.identifier,
			strings.Join(updates, ", "),
			primaryKeySelector,
		)

		if o.reversibleBlockNum != nil {
			return d.saveUpdate(schema, o.table.nameEscaped, o.primaryKey, *o.reversibleBlockNum) + updateQuery, nil
		}
		return updateQuery, nil

	case OperationTypeDelete:
		primaryKeyWhereClause := getPrimaryKeyWhereClause(o.primaryKey, "")
		deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE %s",
			o.table.identifier,
			primaryKeyWhereClause,
		)
		if o.reversibleBlockNum != nil {
			return d.saveDelete(schema, o.table.nameEscaped, o.primaryKey, *o.reversibleBlockNum) + deleteQuery, nil
		}
		return deleteQuery, nil

	default:
		panic(fmt.Errorf("unknown operation type %q", o.opType))
	}
}

func (d *RisingwaveDialect) prepareColValues(table *TableInfo, colValues map[string]string) (columns []string, values []string, err error) {
	if len(colValues) == 0 {
		return
	}

	columns = make([]string, len(colValues))
	values = make([]string, len(colValues))

	i := 0
	for colName := range colValues {
		columns[i] = colName
		i++
	}
	sort.Strings(columns) // sorted for determinism in tests

	for i, columnName := range columns {
		value := colValues[columnName]
		columnInfo, found := table.columnsByName[columnName]
		if !found {
			return nil, nil, fmt.Errorf("cannot find column %q for table %q (valid columns are %q)", columnName, table.identifier, strings.Join(maps.Keys(table.columnsByName), ", "))
		}

		normalizedValue, err := d.normalizeValueType(value, columnInfo.scanType)
		if err != nil {
			return nil, nil, fmt.Errorf("getting sql value from table %s for column %q raw value %q: %w", table.identifier, columnName, value, err)
		}

		values[i] = normalizedValue
		columns[i] = columnInfo.escapedName // escape the column name
	}
	return
}

// Format based on type, value returned unescaped
func (d *RisingwaveDialect) normalizeValueType(value string, valueType reflect.Type) (string, error) {
	switch valueType.Kind() {
	case reflect.String:
		// replace unicode null character with empty string
		value = strings.ReplaceAll(value, "\u0000", "")
		return escapeStringValue(value), nil

	// BYTES in Postgres must be escaped, we receive a Vec<u8> from substreams
	case reflect.Slice:
		return escapeStringValue(value), nil

	case reflect.Bool:
		return fmt.Sprintf("'%s'", value), nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return value, nil

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return value, nil

	case reflect.Float32, reflect.Float64:
		return value, nil

	case reflect.Struct:
		if valueType == reflectTypeTime {
			if integerRegex.MatchString(value) {
				i, err := strconv.Atoi(value)
				if err != nil {
					return "", fmt.Errorf("could not convert %s to int: %w", value, err)
				}

				return escapeStringValue(time.Unix(int64(i), 0).Format(time.RFC3339)), nil
			}

			// It's a plain string, parse by dialect it and pass it to the databaseName
			return d.ParseDatetimeNormalization(value), nil
		}

		return "", fmt.Errorf("unsupported struct type %s", valueType)
	default:
		// It's a column's type the schemaName parsing don't know how to represents as
		// a Go type. In that case, we pass it unmodified to the databaseName engine. It
		// will be the responsibility of the one sending the data to correctly represent
		// it in the way accepted by the databaseName.
		//
		// In most cases, it going to just work.
		return value, nil
	}
}
