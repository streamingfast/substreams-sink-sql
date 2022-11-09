package db

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func (l Loader) Insert(schemaName, table string, key string, data map[string]string) error {
	var keys []string
	var values []string
	for k, v := range data {
		keys = append(keys, k)
		value, err := l.value(schemaName, table, k, v)
		if err != nil {
			return fmt.Errorf("getting sql value for column %s raw value %s: %w", k, v, err)
		}
		values = append(values, value)
	}

	keysString := strings.Join(keys, ",")
	valuesString := strings.Join(values, ",")

	query := fmt.Sprintf("insert into %s.%s (%s) values (%s)", schemaName, table, keysString, valuesString)
	return l.exec(query)
}

func (l *Loader) Update(schemaName, table string, key string, data map[string]string) error {
	pk, ok := l.tablePrimaryKeys[[2]string{schemaName, table}]
	if !ok {
		pk = "id"
	}

	var keys []string
	var values []string
	for k, v := range data {
		keys = append(keys, k)
		value, err := l.value(schemaName, table, k, v)
		if err != nil {
			return fmt.Errorf("getting sql value for column %s raw value %s: %w", k, v, err)
		}
		values = append(values, value)
	}

	var updates []string
	for i := 0; i < len(keys); i++ {
		// FIXME: merely using %s for key can lead to SQL injection. I
		// know, you should trust the Substreams you're putting in
		// front, but still.
		update := fmt.Sprintf("%s=%s", keys[i], values[i])
		updates = append(updates, update)
	}

	updatesString := strings.Join(updates, ", ")

	query := fmt.Sprintf("update %s.%s set %s where %s = %s", schemaName, table, updatesString, pk, key)
	return l.exec(query)
}

func (l *Loader) Delete(schemaName, table string, key string) error {
	pk, ok := l.tablePrimaryKeys[[2]string{schemaName, table}]
	if !ok {
		pk = "id"
	}

	query := fmt.Sprintf("delete from %s.%s where %s = %s", schemaName, table, pk, key)
	return l.exec(query)
}

func (l *Loader) exec(query string) error {
	_, err := l.DB.Exec(query)
	if err != nil {
		return fmt.Errorf("executing query:\n`%s`\n err: %w", query, err)
	}

	return nil
}

func (l *Loader) value(schemaName, table, column, value string) (string, error) {
	valType, ok := l.tableRegistry[[2]string{schemaName, table}][column]
	if !ok {
		return "", fmt.Errorf("could not find column %s in table %s.%s", column, schemaName, table)
	}

	// FIXME: all these values need proper escaping of values. merely wrapping in ' is an
	// opening for SQL injection.
	switch valType.Kind() {
	case reflect.String:
		return fmt.Sprintf("'%s'", value), nil
	case reflect.Bool:
		return fmt.Sprintf("'%s'", value), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return value, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return value, nil
	case reflect.Float32, reflect.Float64:
		return value, nil
	case reflect.Struct:
		if valType == reflect.TypeOf(time.Time{}) {
			i, err := strconv.Atoi(value)
			if err != nil {
				return "", fmt.Errorf("could not convert %s to int: %w", value, err)
			}

			v := time.Unix(int64(i), 0).Format(time.RFC3339)
			return fmt.Sprintf("'%s'", v), nil
		}
		return "", fmt.Errorf("unsupported type %s for column %s in table %s.%s", valType, column, schemaName, table)
	default:
		return "", fmt.Errorf("unsupported type %s for column %s in table %s.%s", valType, column, schemaName, table)
	}
}
