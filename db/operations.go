package db

import (
	"fmt"
	"reflect"
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
	if o.opType == OperationTypeDelete {
		return fmt.Sprintf("DELETE FROM %s.%s WHERE %s = %s", o.schemaName, o.tableName, o.primaryKeyColumnName, o.primaryKey), nil
	}

	columns, values, err := prepareColValues(o.tableName, o.data, typeGetter)
	if err != nil {
		return "", fmt.Errorf("preparing column-values: %w", err)
	}
	if o.opType == OperationTypeInsert {
		return fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)",
			o.schemaName,
			o.tableName,
			strings.Join(columns, ","),
			strings.Join(values, ","),
		), nil
	}

	var updates []string
	for i := 0; i < len(columns); i++ {
		// FIXME: merely using %s for key can lead to SQL injection. I
		// know, you should trust the Substreams you're putting in
		// front, but still.
		update := fmt.Sprintf("%s=%s", columns[i], values[i])
		updates = append(updates, update)
	}

	updatesString := strings.Join(updates, ", ")
	return fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s = '%s'", o.schemaName, o.tableName, updatesString, o.primaryKeyColumnName, o.primaryKey), nil
}

func prepareColValues(tableName string, colValues map[string]string, typeGetter TypeGetter) (columns []string, values []string, err error) {
	for columnName, value := range colValues {
		escapedColumn := escapeIdentifier(columnName)

		columns = append(columns, escapedColumn)

		valueType, err := typeGetter(tableName, columnName)
		if err != nil {
			return nil, nil, fmt.Errorf("get column type %s.%s: %w", tableName, columnName, err)
		}

		normalizedValue, err := normalizeValueType(value, valueType)
		if err != nil {
			return nil, nil, fmt.Errorf("getting sql value from table %s for column %q raw value %q: %w", tableName, columnName, value, err)
		}

		escapedValue := escapeStringValue(normalizedValue)

		values = append(values, escapedValue)
	}
	return
}

// Format based on type, value returned unescaped
func normalizeValueType(value string, valueType reflect.Type) (string, error) {

	switch valueType.Kind() {
	case reflect.String:
		return value, nil
	case reflect.Bool:
		return fmt.Sprintf("'%s'", value), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return value, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return value, nil
	case reflect.Float32, reflect.Float64:
		return value, nil
	case reflect.Struct:
		if valueType == reflect.TypeOf(time.Time{}) {
			i, err := strconv.Atoi(value)
			if err != nil {
				return "", fmt.Errorf("could not convert %s to int: %w", value, err)
			}

			v := time.Unix(int64(i), 0).Format(time.RFC3339)
			return fmt.Sprintf("'%s'", v), nil
		}
		return "", fmt.Errorf("unsupported struct type %s", valueType)
	default:
		return "", fmt.Errorf("unsupported type %s", valueType)
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
