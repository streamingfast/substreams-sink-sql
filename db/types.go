package db

import (
	"fmt"
	"reflect"
)

//go:generate go-enum -f=$GOFILE --marshal --names -nocase

// ENUM(
//
//	 Ignore
//		Warn
//		Error
//
// )
type OnModuleHashMismatch uint

type TableInfo struct {
	schema        string
	schemaEscaped string
	name          string
	nameEscaped   string
	columnsByName map[string]*ColumnInfo
	primaryColumn []*ColumnInfo

	// Identifier is equivalent to 'escape(<schema>).escape(<name>)' but pre-computed
	// for usage when computing queries.
	identifier string
}

func NewTableInfo(schema, name string, pkList []string, columnsByName map[string]*ColumnInfo) (*TableInfo, error) {
	schemaEscaped := escapeIdentifier(schema)
	nameEscaped := escapeIdentifier(name)
	var primaryColumns []*ColumnInfo

	for _, primaryKeyColumnName := range(pkList) {
		primaryColumn, found := columnsByName[primaryKeyColumnName]
		if !found {
			return nil, fmt.Errorf("primary key column %q not found", primaryKeyColumnName)
		}
		primaryColumns = append(primaryColumns, primaryColumn)

	}

	return &TableInfo{
		schema:        schema,
		schemaEscaped: schemaEscaped,
		name:          name,
		nameEscaped:   nameEscaped,
		identifier:    schemaEscaped + "." + nameEscaped,
		primaryColumn: primaryColumns,
		columnsByName: columnsByName,
	}, nil
}

type ColumnInfo struct {
	name             string
	escapedName      string
	databaseTypeName string
	scanType         reflect.Type
}

func NewColumnInfo(name string, databaseTypeName string, scanType any) *ColumnInfo {
	return &ColumnInfo{
		name:             name,
		escapedName:      escapeIdentifier(name),
		databaseTypeName: databaseTypeName,
		scanType:         reflect.TypeOf(scanType),
	}
}
