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
	schemaName           string
	tableName            string
	primaryKeyColumnName string
	opType               OperationType
	primaryKey           string
	data                 map[string]string
}

func (o *Operation) String() string {
	return fmt.Sprintf("%s.%s/%s (%s)", o.schemaName, o.tableName, o.primaryKey, strings.ToLower(string(o.opType)))
}

func (l *Loader) newInsertOperation(tableName string, primaryKey string, data map[string]string) *Operation {
	return &Operation{
		schemaName:           l.schema,
		tableName:            tableName,
		opType:               OperationTypeInsert,
		primaryKeyColumnName: l.tablePrimaryKeys[tableName],
		primaryKey:           primaryKey,
		data:                 data,
	}
}

func (l *Loader) newUpdateOperation(tableName string, primaryKey string, data map[string]string) *Operation {
	return &Operation{
		schemaName:           l.schema,
		tableName:            tableName,
		opType:               OperationTypeUpdate,
		primaryKeyColumnName: l.tablePrimaryKeys[tableName],
		primaryKey:           primaryKey,
		data:                 data,
	}
}

func (l *Loader) newDeleteOperation(tableName string, primaryKey string) *Operation {
	return &Operation{
		schemaName:           l.schema,
		tableName:            tableName,
		opType:               OperationTypeDelete,
		primaryKeyColumnName: l.tablePrimaryKeys[tableName],
		primaryKey:           primaryKey,
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

func (o *Operation) query(typeGetter TypeGetter) (string, error) {
	var columns, values []string
	if o.opType == OperationTypeInsert || o.opType == OperationTypeUpdate {
		var err error
		columns, values, err = prepareColValues(o.tableName, o.data, typeGetter)
		if err != nil {
			return "", fmt.Errorf("preparing column & values: %w", err)
		}
	}

	switch o.opType {
	case OperationTypeInsert:
		return fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)",
			o.schemaName,
			o.tableName,
			strings.Join(columns, ","),
			strings.Join(values, ","),
		), nil

	case OperationTypeUpdate:
		updates := make([]string, len(columns))
		for i := 0; i < len(columns); i++ {
			updates[i] = fmt.Sprintf("%s=%s", columns[i], values[i])
		}

		updatesString := strings.Join(updates, ", ")
		return fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s = %s",
			escapeIdentifier(o.schemaName),
			escapeIdentifier(o.tableName),
			updatesString,
			escapeIdentifier(o.primaryKeyColumnName),
			escapeStringValue(o.primaryKey),
		), nil

	case OperationTypeDelete:
		return fmt.Sprintf("DELETE FROM %s.%s WHERE %s = %s",
			escapeIdentifier(o.schemaName),
			escapeIdentifier(o.tableName),
			escapeIdentifier(o.primaryKeyColumnName),
			escapeStringValue(o.primaryKey),
		), nil

	default:
		panic(fmt.Errorf("unknown operation type %q", o.opType))
	}

}

func prepareColValues(tableName string, colValues map[string]string, typeGetter TypeGetter) (columns []string, values []string, err error) {
	if len(colValues) == 0 {
		return
	}

	columns = make([]string, len(colValues))
	values = make([]string, len(colValues))

	i := 0
	for columnName, value := range colValues {
		valueType, err := typeGetter(tableName, columnName)
		if err != nil {
			return nil, nil, fmt.Errorf("get column type %s.%s: %w", tableName, columnName, err)
		}

		normalizedValue, err := normalizeValueType(value, valueType)
		if err != nil {
			return nil, nil, fmt.Errorf("getting sql value from table %s for column %q raw value %q: %w", tableName, columnName, value, err)
		}

		columns[i] = escapeIdentifier(columnName)
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
