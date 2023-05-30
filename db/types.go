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
	primaryColumn *ColumnInfo

	// Identifier is equivalent to 'escape(<schema>).escape(<name>)' but pre-computed
	// for usage when computing queries.
	identifier string
}

func NewTableInfo(schema, name, primaryKeyColumnName string, columnsByName map[string]*ColumnInfo) (*TableInfo, error) {
	schemaEscaped := escapeIdentifier(schema)
	nameEscaped := escapeIdentifier(name)
	primaryColumn, found := columnsByName[primaryKeyColumnName]
	if !found {
		return nil, fmt.Errorf("primary key column %q not found", primaryKeyColumnName)
	}

	return &TableInfo{
		schema:        schema,
		schemaEscaped: schemaEscaped,
		name:          name,
		nameEscaped:   nameEscaped,
		identifier:    schemaEscaped + "." + nameEscaped,
		primaryColumn: primaryColumn,
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
