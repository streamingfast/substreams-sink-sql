package db

import (
	"fmt"
	"math/big"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/exp/maps"
)

type TypeGetter func(tableName string, columnName string) (reflect.Type, error)

type Queryable interface {
	query(d dialect) (string, error)
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
	primaryKey map[string]string
	data       map[string]string
}

func (o *Operation) String() string {
	return fmt.Sprintf("%s/%s (%s)", o.table.identifier, createRowUniqueID(o.primaryKey), strings.ToLower(string(o.opType)))
}

func (l *Loader) newInsertOperation(table *TableInfo, primaryKey map[string]string, data map[string]string) *Operation {
	return &Operation{
		table:      table,
		opType:     OperationTypeInsert,
		primaryKey: primaryKey,
		data:       data,
	}
}

func (l *Loader) newUpdateOperation(table *TableInfo, primaryKey map[string]string, data map[string]string) *Operation {
	return &Operation{
		table:      table,
		opType:     OperationTypeUpdate,
		primaryKey: primaryKey,
		data:       data,
	}
}

func (l *Loader) newDeleteOperation(table *TableInfo, primaryKey map[string]string) *Operation {
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

func (o *Operation) getValues() ([]any, error) {
	columns := make([]string, len(o.data))
	i := 0
	for column := range o.data {
		columns[i] = column
		i++
	}
	sort.Strings(columns)
	values := make([]any, len(o.data))
	for i, v := range columns {
		convertedType, err := convertToType(o.data[v], o.table.columnsByName[v].scanType)
		if err != nil {
			return nil, fmt.Errorf("converting value %q to type %q: %w", o.data[v], o.table.columnsByName[v].scanType, err)
		}
		values[i] = convertedType
	}
	return values, nil
}

func (o *Operation) query(d dialect) (string, error) {
	var columns, values []string
	if o.opType == OperationTypeInsert || o.opType == OperationTypeUpdate {
		var err error
		columns, values, err = prepareColValues(d, o.table, o.data)
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

func prepareColValues(d dialect, table *TableInfo, colValues map[string]string) (columns []string, values []string, err error) {
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

		normalizedValue, err := normalizeValueType(value, columnInfo.scanType, d)
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

func convertToType(value string, valueType reflect.Type) (any, error) {
	switch valueType.Kind() {
	case reflect.String:
		return value, nil
	case reflect.Slice:
		return value, nil
	case reflect.Bool:
		return strconv.ParseBool(value)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.ParseInt(value, 10, 0)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint64:
		return strconv.ParseUint(value, 10, 0)
	case reflect.Uint32:
		v, err := strconv.ParseUint(value, 10, 32)
		return uint32(v), err
	case reflect.Float32, reflect.Float64:
		return strconv.ParseFloat(value, 10)
	case reflect.Struct:
		if valueType == reflectTypeTime {
			if integerRegex.MatchString(value) {
				i, err := strconv.Atoi(value)
				if err != nil {
					return "", fmt.Errorf("could not convert %s to int: %w", value, err)
				}

				return int64(i), nil
			}

			v, err := time.Parse("2006-01-02T15:04:05Z", value)
			if err != nil {
				return "", fmt.Errorf("could not convert %s to time: %w", value, err)
			}
			return v.Unix(), nil
		}
		return "", fmt.Errorf("unsupported struct type %s", valueType)

	case reflect.Ptr:
		if valueType.String() == "*big.Int" {
			newInt := new(big.Int)
			newInt.SetString(value, 10)
			return newInt, nil
		}
		return "", fmt.Errorf("unsupported pointer type %s", valueType)
	default:
		return value, nil
	}
}

// Format based on type, value returned unescaped
func normalizeValueType(value string, valueType reflect.Type, d dialect) (string, error) {
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

func EscapeIdentifier(valueToEscape string) string {
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
