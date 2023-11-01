package db

import (
	"context"
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

type postgresDialect struct{}

func (d postgresDialect) Flush(tx Tx, ctx context.Context, l *Loader, outputModuleHash string, lastFinalBlock uint64) (int, error) {
	var rowCount int
	for entriesPair := l.entries.Oldest(); entriesPair != nil; entriesPair = entriesPair.Next() {
		tableName := entriesPair.Key
		entries := entriesPair.Value

		if l.tracer.Enabled() {
			l.logger.Debug("flushing table rows", zap.String("table_name", tableName), zap.Int("row_count", entries.Len()))
		}
		for entryPair := entries.Oldest(); entryPair != nil; entryPair = entryPair.Next() {
			entry := entryPair.Value

			query, err := d.prepareStatement(l.schema, entry)
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

	if err := d.pruneReversibleSegment(tx, ctx, l.schema, lastFinalBlock); err != nil {
		return 0, err
	}

	return rowCount, nil
}

func (d postgresDialect) pruneReversibleSegment(tx Tx, ctx context.Context, schema string, highestFinalBlock uint64) error {
	pruneInserts := fmt.Sprintf(`DELETE FROM %s WHERE block_num <= %d;`, d.insertsTable(schema), highestFinalBlock)
	pruneUpdates := fmt.Sprintf(`DELETE FROM %s WHERE block_num <= %d;`, d.updatesTable(schema), highestFinalBlock)
	pruneDeletes := fmt.Sprintf(`DELETE FROM %s WHERE block_num <= %d;`, d.deletesTable(schema), highestFinalBlock)
	query := pruneInserts + pruneUpdates + pruneDeletes

	if _, err := tx.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("executing prune query %q: %w", query, err)
	}
	return nil
}

func (d postgresDialect) GetCreateCursorQuery(schema string, withPostgraphile bool) string {
	out := fmt.Sprintf(cli.Dedent(`
		create table if not exists %s.%s
		(
			id         text not null constraint cursor_pk primary key,
			cursor     text,
			block_num  bigint,
			block_id   text
		);
		`), EscapeIdentifier(schema), EscapeIdentifier("cursors"))
	if withPostgraphile {
		out += fmt.Sprintf("COMMENT ON TABLE %s.%s IS E'@omit';",
			EscapeIdentifier(schema), EscapeIdentifier("cursors"))
	}
	return out
}

func (d postgresDialect) GetCreateSubstreamsHistoryTableQuery(schema string) string {
	out := fmt.Sprintf(cli.Dedent(`
		create table if not exists %s
		(
            table_name      text,
			id              text,
			block_num       bigint
		);
		create table if not exists %s
		(
            table_name      text,
			id              text,
            prev_value      text,
			block_num       bigint
		);
		create table if not exists %s
		(
            table_name      text,
			id              text,
            prev_value      text,
			block_num       bigint
		);
		`),
		d.insertsTable(schema),
		d.updatesTable(schema),
		d.deletesTable(schema),
	)
	return out
}

func (d postgresDialect) ExecuteSetupScript(ctx context.Context, l *Loader, schemaSql string) error {
	if _, err := l.ExecContext(ctx, schemaSql); err != nil {
		return fmt.Errorf("exec schema: %w", err)
	}
	return nil
}

func (d postgresDialect) GetUpdateCursorQuery(table, moduleHash string, cursor *sink.Cursor, block_num uint64, block_id string) string {
	return query(`
		UPDATE %s set cursor = '%s', block_num = %d, block_id = '%s' WHERE id = '%s';
	`, table, cursor, block_num, block_id, moduleHash)
}

func (d postgresDialect) ParseDatetimeNormalization(value string) string {
	return escapeStringValue(value)
}

func (d postgresDialect) DriverSupportRowsAffected() bool {
	return true
}

func (d postgresDialect) OnlyInserts() bool {
	return false
}

func (d postgresDialect) insertsTable(schema string) string {
	return fmt.Sprintf("%s.%s", EscapeIdentifier(schema), EscapeIdentifier("inserts_history"))
}

func (d postgresDialect) saveInsert(schema string, table string, primaryKey map[string]string, blockNum uint64) string {
	return fmt.Sprintf(`INSERT INTO %s (table_name, id, block_num) values (%s, %s, %d);`,
		d.insertsTable(schema),
		escapeStringValue(table),
		escapeStringValue(primaryKeyToJSON(primaryKey)),
		blockNum,
	)
}

func (d postgresDialect) updatesTable(schema string) string {
	return fmt.Sprintf("%s.%s", EscapeIdentifier(schema), EscapeIdentifier("updates_history"))
}

func (d postgresDialect) saveUpdate(schema string, table string, primaryKey map[string]string, blockNum uint64) string {
	return d.saveRow(table, d.updatesTable(schema), primaryKey, blockNum)
}

func (d postgresDialect) deletesTable(schema string) string {
	return fmt.Sprintf("%s.%s", EscapeIdentifier(schema), EscapeIdentifier("deletes_history"))
}

func (d postgresDialect) saveDelete(schema string, table string, primaryKey map[string]string, blockNum uint64) string {
	return d.saveRow(table, d.deletesTable(schema), primaryKey, blockNum)
}

func (d postgresDialect) saveRow(table string, targetTable string, primaryKey map[string]string, blockNum uint64) string {
	// insert into deletes_history (table_name, id, prev_value, block_num)
	// select 'ownership_transferred',
	// '["evt_tx_hash":"00006614dade7f56557b84e5fe674a264a50e83eec52ccec62c9fff4c2de4a2a","evt_index":"132"]',
	// row_to_json(ownership_transferred),
	// 12345678 from ownership_transferred
	// where evt_tx_hash = '22199329b0aa1aa68902a78e3b32ca327c872fab166c7a2838273de6ad383eba' and evt_index = 249

	return fmt.Sprintf(`INSERT INTO %s (table_name, id, prev_value, block_num)
        SELECT %s, %s, row_to_json(%s), %d
        FROM %s
        WHERE %s`,

		targetTable,
		escapeStringValue(table), escapeStringValue(primaryKeyToJSON(primaryKey)), EscapeIdentifier(table), blockNum,
		EscapeIdentifier(table),
		getPrimaryKeyWhereClause(primaryKey),
	)

}

func (d *postgresDialect) prepareStatement(schema string, o *Operation) (string, error) {
	var columns, values []string
	if o.opType == OperationTypeInsert || o.opType == OperationTypeUpdate {
		var err error
		columns, values, err = d.prepareColValues(o.table, o.data)
		if err != nil {
			return "", fmt.Errorf("preparing column & values: %w", err)
		}
	}

	if o.opType == OperationTypeUpdate || o.opType == OperationTypeDelete {
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

	case OperationTypeUpdate:
		updates := make([]string, len(columns))
		for i := 0; i < len(columns); i++ {
			updates[i] = fmt.Sprintf("%s=%s", columns[i], values[i])
		}

		primaryKeySelector := getPrimaryKeyWhereClause(o.primaryKey)

		updateQuery := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
			o.table.identifier,
			strings.Join(updates, ", "),
			primaryKeySelector,
		)

		if o.reversibleBlockNum != nil {
			return d.saveUpdate(schema, o.table.identifier, o.primaryKey, *o.reversibleBlockNum) + updateQuery, nil
		}
		return updateQuery, nil

	case OperationTypeDelete:
		primaryKeyWhereClause := getPrimaryKeyWhereClause(o.primaryKey)
		deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE %s",
			o.table.identifier,
			primaryKeyWhereClause,
		)
		if o.reversibleBlockNum != nil {
			return d.saveDelete(schema, o.table.identifier, o.primaryKey, *o.reversibleBlockNum) + deleteQuery, nil
		}
		return deleteQuery, nil

	default:
		panic(fmt.Errorf("unknown operation type %q", o.opType))
	}
}

func (d *postgresDialect) prepareColValues(table *TableInfo, colValues map[string]string) (columns []string, values []string, err error) {
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
	}
	return
}

func getPrimaryKeyWhereClause(primaryKey map[string]string) string {
	// Avoid any allocation if there is a single primary key
	if len(primaryKey) == 1 {
		for key, value := range primaryKey {
			return EscapeIdentifier(key) + " = " + escapeStringValue(value)
		}
	}

	reg := make([]string, 0, len(primaryKey))
	for key, value := range primaryKey {
		reg = append(reg, EscapeIdentifier(key)+" = "+escapeStringValue(value))
	}

	return strings.Join(reg[:], " AND ")
}

// Format based on type, value returned unescaped
func (d *postgresDialect) normalizeValueType(value string, valueType reflect.Type) (string, error) {
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

			// It's a plain string, parse by dialect it and pass it to the database
			return d.ParseDatetimeNormalization(value), nil
		}

		return "", fmt.Errorf("unsupported struct type %s", valueType)
	default:
		// It's a column's type the schema parsing don't know how to represents as
		// a Go type. In that case, we pass it unmodified to the database engine. It
		// will be the responsibility of the one sending the data to correctly represent
		// it in the way accepted by the database.
		//
		// In most cases, it going to just work.
		return value, nil
	}
}
