package clickhouse

import (
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"

	sql2 "github.com/streamingfast/substreams-sink-sql/db_proto/sql"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/schema"
	"go.uber.org/zap"
)

const staticSqlCreatDatabase = `
	CREATE DATABASE IF NOT EXISTS %s;
`
const staticSqlCreateBlock = `
	CREATE TABLE IF NOT EXISTS %s._blocks_  (
		number    integer,
		hash      text,
		timestamp timestamp,
		version Int64,
		deleted bool

	)
	ENGINE = ReplacingMergeTree(version)
	PARTITION BY (toYYYYMM(timestamp))		
	PRIMARY KEY (number)
	ORDER BY (number);
`

type DialectClickHouse struct {
	*sql2.BaseDialect
	schemaName string
}

func NewDialectClickHouse(schema *schema.Schema, logger *zap.Logger) (*DialectClickHouse, error) {
	d := &DialectClickHouse{
		BaseDialect: sql2.NewBaseDialect(schema.TableRegistry, logger),
		schemaName:  schema.Name,
	}

	err := d.init()
	if err != nil {
		return nil, fmt.Errorf("initializing dialect: %w", err)
	}

	for _, table := range schema.TableRegistry {
		err := d.createTable(table)
		if err != nil {
			return nil, fmt.Errorf("handling table %q: %w", table.Name, err)
		}
	}

	return d, nil
}

func (d *DialectClickHouse) UseVersionField() bool {
	return true
}

func (d *DialectClickHouse) UseDeletedField() bool {
	return true
}

func (d *DialectClickHouse) init() error {
	return nil
}

func (d *DialectClickHouse) createTable(table *schema.Table) error {
	var sb strings.Builder

	tableName := d.FullTableName(table)

	sb.WriteString(fmt.Sprintf("CREATE TABLE  IF NOT EXISTS %s (", tableName))

	sb.WriteString(fmt.Sprintf(" %s Int64 NOT NULL,", sql2.DialectFieldBlockNumber))
	sb.WriteString(fmt.Sprintf(" %s timestamp NOT NULL,", sql2.DialectFieldBlockTimestamp))
	sb.WriteString(fmt.Sprintf(" %s Int64 NOT NULL,", sql2.DialectFieldVersion))
	sb.WriteString(fmt.Sprintf(" %s bool NOT NULL,", sql2.DialectFieldDeleted))

	var primaryKeyFieldName string
	if table.PrimaryKey != nil {
		pk := table.PrimaryKey
		primaryKeyFieldName = pk.Name
		sb.WriteString(fmt.Sprintf("%s %s,", pk.Name, MapFieldType(pk.FieldDescriptor)))
	}

	if table.ChildOf != nil {
		parentTable, parentFound := d.TableRegistry[table.ChildOf.ParentTable]
		if !parentFound {
			return fmt.Errorf("parent table %q not found", table.ChildOf.ParentTable)
		}
		fieldFound := false
		for _, parentField := range parentTable.Columns {

			if parentField.Name == table.ChildOf.ParentTableField {
				sb.WriteString(fmt.Sprintf("%s %s NOT NULL,", parentField.Name, MapFieldType(parentField.FieldDescriptor)))
				fieldFound = true
				break
			}
		}
		if !fieldFound {
			return fmt.Errorf("field %q not found in table %q", table.ChildOf.ParentTableField, table.ChildOf.ParentTable)
		}
	}

	for _, f := range table.Columns {
		if f.Name == primaryKeyFieldName {
			continue
		}

		fieldName := f.Name

		switch {
		case f.IsRepeated:
			continue
		case f.IsMessage:
		case f.ForeignKey != nil:
		}
		//fmt.Printf("Table %s, field %s\n", table.Name, fieldName)
		fieldType := MapFieldType(f.FieldDescriptor)
		sb.WriteString(fmt.Sprintf("%s %s", fieldName, fieldType))
		sb.WriteString(",")
	}

	//removing the last comma since it is complicated to removing it before
	temp := sb.String()
	temp = temp[:len(temp)-1]
	sb = strings.Builder{}
	sb.WriteString(temp)

	orderByFields := make([]string, 0)
	if primaryKeyFieldName != "" {
		orderByFields = append(orderByFields, primaryKeyFieldName)
	}

	//this is tricky. handling one to one relation
	if primaryKeyFieldName == "" && table.ChildOf != nil {
		parentTable, parentFound := d.TableRegistry[table.ChildOf.ParentTable]
		if !parentFound {
			return fmt.Errorf("parent table %q not found", table.ChildOf.ParentTable)
		}

		for _, parentField := range parentTable.Columns {
			if parentField.Name == table.ChildOf.ParentTableField && !parentField.IsRepeated {
				orderByFields = append(orderByFields, parentField.Name)
				break
			}
		}
	}

	if len(orderByFields) == 0 {
		return fmt.Errorf("missing order by fields")
	}

	primaryKey := ""
	if primaryKeyFieldName != "" {
		primaryKey = fmt.Sprintf("PRIMARY KEY (%s)", primaryKeyFieldName)
	}

	orderBy := strings.Join(orderByFields, ",")
	sb.WriteString(fmt.Sprintf(") ENGINE = ReplacingMergeTree(%s) PARTITION BY (toYYYYMM(%s)) %s ORDER BY (%s);", sql2.DialectFieldVersion, sql2.DialectFieldBlockTimestamp, primaryKey, orderBy))

	d.AddCreateTableSql(table.Name, sb.String())

	return nil

}

func (d *DialectClickHouse) FullTableName(table *schema.Table) string {
	return tableName(d.schemaName, table.Name)
}

func (d *DialectClickHouse) SchemaHash() string {
	h := fnv.New64a()

	var buf []byte

	// SchemaHash tableCreateStatements
	var sqls []string
	for _, sql := range d.CreateTableSql {
		sqls = append(sqls, sql)
	}

	sort.Strings(sqls)
	for _, sql := range sqls {
		buf = append(buf, []byte(sql)...)
	}

	var pk []string
	for _, constraint := range d.PrimaryKeySql {
		pk = append(pk, constraint.Sql)
	}
	sort.Strings(pk)
	for _, constraint := range pk {
		buf = append(buf, []byte(constraint)...)
	}

	var fk []string
	for _, constraint := range d.ForeignKeySql {
		fk = append(fk, constraint.Sql)
	}
	sort.Strings(fk)
	for _, constraint := range fk {
		buf = append(buf, []byte(constraint)...)
	}

	var uniques []string
	for _, constraint := range d.UniqueConstraintSql {
		uniques = append(uniques, constraint.Sql)
	}
	sort.Strings(uniques)
	for _, constraint := range uniques {
		buf = append(buf, []byte(constraint)...)
	}

	_, err := h.Write(buf)
	if err != nil {
		panic("unable to write to hash")
	}

	data := h.Sum(nil)
	return hex.EncodeToString(data)
}

func tableName(schemaName string, tableName string) string {
	return fmt.Sprintf("%s.%s", schemaName, tableName)
}
