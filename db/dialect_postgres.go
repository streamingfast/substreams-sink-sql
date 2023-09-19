package db

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/streamingfast/cli"
	sink "github.com/streamingfast/substreams-sink"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
)

type postgresDialect struct{}

func (d postgresDialect) Flush(tx *sql.Tx, ctx context.Context, l *Loader, outputModuleHash string, cursor *sink.Cursor) (int, error) {
	var rowCount int
	for entriesPair := l.entries.Oldest(); entriesPair != nil; entriesPair = entriesPair.Next() {
		tableName := entriesPair.Key
		entries := entriesPair.Value

		if l.tracer.Enabled() {
			l.logger.Debug("flushing table rows", zap.String("table_name", tableName), zap.Int("row_count", entries.Len()))
		}
		for entryPair := entries.Oldest(); entryPair != nil; entryPair = entryPair.Next() {
			entry := entryPair.Value

			query, err := d.prepareStatement(entry)
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

	return rowCount, nil
}

func (d postgresDialect) GetCreateCursorQuery(schema string) string {
	return fmt.Sprintf(cli.Dedent(`
		create table if not exists %s.%s
		(
			id         text not null constraint cursor_pk primary key,
			cursor     text,
			block_num  bigint,
			block_id   text
		);
		`), EscapeIdentifier(schema), EscapeIdentifier("cursors"))
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

func (d *postgresDialect) prepareStatement(o *Operation) (string, error) {
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
		return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			o.table.identifier,
			strings.Join(columns, ","),
			strings.Join(values, ","),
		), nil

	case OperationTypeUpdate:
		updates := make([]string, len(columns))
		for i := 0; i < len(columns); i++ {
			updates[i] = fmt.Sprintf("%s=%s", columns[i], values[i])
		}

		primaryKeySelector := getPrimaryKeyWhereClause(o.primaryKey)
		return fmt.Sprintf("UPDATE %s SET %s WHERE %s",
			o.table.identifier,
			strings.Join(updates, ", "),
			primaryKeySelector,
		), nil

	case OperationTypeDelete:
		primaryKeyWhereClause := getPrimaryKeyWhereClause(o.primaryKey)
		return fmt.Sprintf("DELETE FROM %s WHERE %s",
			o.table.identifier,
			primaryKeyWhereClause,
		), nil

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
	for columnName, value := range colValues {
		columnInfo, found := table.columnsByName[columnName]
		if !found {
			return nil, nil, fmt.Errorf("cannot find column %q for table %q (valid columns are %q)", columnName, table.identifier, strings.Join(maps.Keys(table.columnsByName), ", "))
		}

		normalizedValue, err := d.normalizeValueType(value, columnInfo.scanType)
		if err != nil {
			return nil, nil, fmt.Errorf("getting sql value from table %s for column %q raw value %q: %w", table.identifier, columnName, value, err)
		}

		columns[i] = columnInfo.escapedName
		values[i] = normalizedValue

		i++
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
