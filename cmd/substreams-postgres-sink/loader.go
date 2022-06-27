package main

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/jimsmart/schema"
)

type PostgresLoader struct {
	db *sql.DB

	schema string

	tableRegistry    map[[2]string]map[string]reflect.Type
	tablePrimaryKeys map[[2]string]string
}

func NewPostgresLoader(host, port, username, password, dbname, schemaName string, sslEnabled bool) (*PostgresLoader, error) {
	var sslmode string
	if sslEnabled {
		sslmode = "enable"
	} else {
		sslmode = "disable"
	}

	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s", host, port, username, password, dbname, sslmode))
	if err != nil {
		return nil, err
	}

	tables, err := schema.Tables(db)
	if err != nil {
		return nil, err
	}

	tableRegistry := map[[2]string]map[string]reflect.Type{}
	primaryKeys := map[[2]string]string{}

	for k, t := range tables {
		if k[0] != schemaName {
			continue
		}

		m := map[string]reflect.Type{}
		for _, f := range t {
			m[f.Name()] = f.ScanType()
		}
		tableRegistry[k] = m

		key, err := schema.PrimaryKey(db, k[0], k[1])
		if err != nil {
			return nil, err
		}
		if len(key) > 0 {
			primaryKeys[k] = key[0]
		} else {
			primaryKeys[k] = "id"
		}
	}

	return &PostgresLoader{
		db:               db,
		tableRegistry:    tableRegistry,
		tablePrimaryKeys: primaryKeys,
		schema:           schemaName,
	}, err
}

func (pgm *PostgresLoader) Insert(schemaName, table string, key string, data map[string]string) error {
	var keys []string
	var values []string
	for k, v := range data {
		keys = append(keys, k)
		value, err := pgm.value(schemaName, table, k, v)
		if err != nil {
			return fmt.Errorf("getting sql value for column %s raw value %s: %w", k, v, err)
		}
		values = append(values, value)
	}

	keysString := strings.Join(keys, ",")
	valuesString := strings.Join(values, ",")

	query := fmt.Sprintf("insert into %s.%s (%s) values (%s)", schemaName, table, keysString, valuesString)
	return pgm.exec(query)
}

func (pgm *PostgresLoader) Update(schemaName, table string, key string, data map[string]string) error {
	pk, ok := pgm.tablePrimaryKeys[[2]string{schemaName, table}]
	if !ok {
		pk = "id"
	}

	var keys []string
	var values []string
	for k, v := range data {
		keys = append(keys, k)
		value, err := pgm.value(schemaName, table, k, v)
		if err != nil {
			return fmt.Errorf("getting sql value for column %s raw value %s: %w", k, v, err)
		}
		values = append(values, value)
	}

	var updates []string
	for i := 0; i < len(keys); i++ {
		update := fmt.Sprintf("%s=%s", keys[i], values[i])
		updates = append(updates, update)
	}

	updatesString := strings.Join(updates, ", ")

	query := fmt.Sprintf("update %s.%s set %s where %s = %s", schemaName, table, updatesString, pk, key)
	return pgm.exec(query)
}

func (pgm *PostgresLoader) Delete(schemaName, table string, key string) error {
	pk, ok := pgm.tablePrimaryKeys[[2]string{schemaName, table}]
	if !ok {
		pk = "id"
	}

	query := fmt.Sprintf("delete from %s.%s where %s = %s", schemaName, table, pk, key)
	return pgm.exec(query)
}

func (pgm *PostgresLoader) exec(query string) error {
	_, err := pgm.db.Exec(query)
	if err != nil {
		return fmt.Errorf("executing query:\n`%s`\n err: %w", query, err)
	}

	return nil
}

func (pgm *PostgresLoader) value(schemaName, table, column, value string) (string, error) {
	valType, ok := pgm.tableRegistry[[2]string{schemaName, table}][column]
	if !ok {
		return "", fmt.Errorf("could not find column %s in table %s.%s", column, schemaName, table)
	}

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
