package db

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type TypeGetter func(tableName string, columnName string) (reflect.Type, error)

type Queryable interface {
	query() (string, error)
}

type OperationType string

const (
	OperationTypeInsert OperationType = "INSERT"
	OperationTypeUpdate OperationType = "UPDATE"
	OperationTypeDelete OperationType = "DELETE"
)

type Operation struct {
	table      *TableInfo
	opType     OperationType
	primaryKey string
	data       map[string]string
}

func (o *Operation) String() string {
	return fmt.Sprintf("%s/%s (%s)", o.table.identifier, o.primaryKey, strings.ToLower(string(o.opType)))
}

func (l *Loader) newInsertOperation(table *TableInfo, primaryKey string, data map[string]string) *Operation {
	return &Operation{
		table:      table,
		opType:     OperationTypeInsert,
		primaryKey: primaryKey,
		data:       data,
	}
}

func (l *Loader) newUpdateOperation(table *TableInfo, primaryKey string, data map[string]string) *Operation {
	return &Operation{
		table:      table,
		opType:     OperationTypeUpdate,
		primaryKey: primaryKey,
		data:       data,
	}
}

func (l *Loader) newDeleteOperation(table *TableInfo, primaryKey string) *Operation {
	return &Operation{
		table:      table,
		opType:     OperationTypeDelete,
		primaryKey: primaryKey,
	}
}

func (o *Operation) mergeData(newData map[string]string) error {
	if o.opType == OperationTypeDelete {
		return fmt.Errorf("unable to merge data for a delete operation")
	}

	for k, v := range newData {
		o.data[k] = v
	}
	return nil
}

func (o *Operation) query() (string, error) {
	var columns, values []string
	if o.opType == OperationTypeInsert || o.opType == OperationTypeUpdate {
		var err error
		columns, values, err = prepareColValues(o.table, o.data)
		if err != nil {
			return "", fmt.Errorf("preparing column & values: %w", err)
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

		return fmt.Sprintf("UPDATE %s SET %s WHERE %s = %s",
			o.table.identifier,
			strings.Join(updates, ", "),
			o.table.primaryColumn.escapedName,
			escapeStringValue(o.primaryKey),
		), nil

	case OperationTypeDelete:
		return fmt.Sprintf("DELETE FROM %s WHERE %s = %s",
			o.table.identifier,
			o.table.primaryColumn.escapedName,
			escapeStringValue(o.primaryKey),
		), nil

	default:
		panic(fmt.Errorf("unknown operation type %q", o.opType))
	}
}

func prepareColValues(table *TableInfo, colValues map[string]string) (columns []string, values []string, err error) {
	if len(colValues) == 0 {
		return
	}

	columns = make([]string, len(colValues))
	values = make([]string, len(colValues))

	i := 0
	for columnName, value := range colValues {
		columnInfo, found := table.columnsByName[columnName]
		if !found {
			return nil, nil, fmt.Errorf("cannot find column %q for table %q", columnName, table.identifier)
		}

		normalizedValue, err := normalizeValueType(value, columnInfo.scanType)
		if err != nil {
			return nil, nil, fmt.Errorf("getting sql value from table %s for column %q raw value %q: %w", table.identifier, columnName, value, err)
		}

		columns[i] = columnInfo.escapedName
		values[i] = normalizedValue

		i++
	}
	return
}

var integerRegex = regexp.MustCompile(`^\d+$`)
var reflectTypeTime = reflect.TypeOf(time.Time{})

// Format based on type, value returned unescaped
func normalizeValueType(value string, valueType reflect.Type) (string, error) {
	switch valueType.Kind() {
	case reflect.String:
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

			// It's a plain string, escape it and pass it to the database
			return escapeStringValue(value), nil
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

func escapeIdentifier(valueToEscape string) string {
	if strings.Contains(valueToEscape, `"`) {
		valueToEscape = strings.ReplaceAll(valueToEscape, `"`, `""`)
	}

	return `"` + valueToEscape + `"`
}

func escapeStringValue(valueToEscape string) string {
	if strings.Contains(valueToEscape, `'`) {
		valueToEscape = strings.ReplaceAll(valueToEscape, `'`, `''`)
	}

	return `'` + valueToEscape + `'`
}
