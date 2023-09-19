package db

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"
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

var integerRegex = regexp.MustCompile(`^\d+$`)
var reflectTypeTime = reflect.TypeOf(time.Time{})

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
