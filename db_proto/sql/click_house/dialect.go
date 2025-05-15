package clickhouse

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"time"

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
		timestamp timestamp
	)
	ENGINE = MergeTree()
	PRIMARY KEY (number)
`

type DialectClickHouse struct {
	*sql2.BaseDialect
	schemaName string
}

func NewDialectClickHouse(schemaName string, tableRegistry map[string]*schema.Table, logger *zap.Logger) (*DialectClickHouse, error) {
	d := &DialectClickHouse{
		BaseDialect: sql2.NewBaseDialect(tableRegistry, logger),
		schemaName:  schemaName,
	}

	err := d.init()
	if err != nil {
		return nil, fmt.Errorf("initializing dialect: %w", err)
	}

	for _, table := range tableRegistry {
		err := d.createTable(table)
		if err != nil {
			return nil, fmt.Errorf("handling table %q: %w", table.Name, err)
		}
	}

	return d, nil
}

func (d *DialectClickHouse) init() error {
	//d.AddPrimaryKeySql("block", fmt.Sprintf("alter table %s.block add constraint block_pk primary key (number);", d.schemaName))

	return nil
}

func (d *DialectClickHouse) createTable(table *schema.Table) error {
	var sb strings.Builder

	tableName := d.FullTableName(table)

	sb.WriteString(fmt.Sprintf("CREATE TABLE  IF NOT EXISTS %s (", tableName))
	var primaryKeyFieldName string
	if table.PrimaryKey != nil {
		pk := table.PrimaryKey
		primaryKeyFieldName = pk.Name
		//d.AddPrimaryKeySql(table.Name, fmt.Sprintf("alter table %s add constraint %s_pk primary key (%s);", tableName, table.Name, primaryKeyFieldName))
		sb.WriteString(fmt.Sprintf("%s %s,", pk.Name, MapFieldType(pk.FieldDescriptor)))
	}

	sb.WriteString(" block_number Int64 NOT NULL,")

	if table.ChildOf != nil {
		parentTable, parentFound := d.TableRegistry[table.ChildOf.ParentTable]
		if !parentFound {
			return fmt.Errorf("parent table %q not found", table.ChildOf.ParentTable)
		}
		fieldFound := false
		for _, parentField := range parentTable.Columns {

			if parentField.Name == table.ChildOf.ParentTableField {

				sb.WriteString(fmt.Sprintf("%s %s NOT NULL,", parentField.Name, MapFieldType(parentField.FieldDescriptor)))

				//foreignKey := &sql2.ForeignKey{
				//	Name:         "fk_" + table.ChildOf.ParentTable,
				//	Table:        tableName,
				//	Field:        table.ChildOf.ParentTableField,
				//	ForeignTable: d.FullTableName(parentTable),
				//	ForeignField: parentField.Name,
				//}
				//
				//d.AddForeignKeySql(table.Name, foreignKey.String())

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
		//if f.IsUnique {
		//	d.AddUniqueConstraintSql(table.Name, fmt.Sprintf("alter table %s add constraint %s_%s_unique unique (%s);", tableName, table.Name, fieldName, fieldName))
		//}

		switch {
		case f.IsRepeated:
			continue
		case f.IsMessage:
			//childTable, found := d.TableRegistry[f.Message]
			//if !found {
			//	continue
			//}
			//foreignKey := &sql2.ForeignKey{
			//	Name:         "fk_" + childTable.Name,
			//	Table:        tableName,
			//	Field:        f.Name,
			//	ForeignTable: d.FullTableName(childTable),
			//	ForeignField: childTable.PrimaryKey.Name,
			//}
			//d.AddForeignKeySql(table.Name, foreignKey.String())

		case f.ForeignKey != nil:
			//foreignTable, found := d.TableRegistry[f.ForeignKey.Table]
			//if !found {
			//	return fmt.Errorf("foreign table %q not found", f.ForeignKey.Table)
			//}
			//
			//var foreignField *schema.Column
			//for _, field := range foreignTable.Columns {
			//	if field.Name == f.ForeignKey.TableField {
			//		foreignField = field
			//		break
			//	}
			//}
			//if foreignField == nil {
			//	return fmt.Errorf("foreign field %q not found in table %q", f.ForeignKey.TableField, f.ForeignKey.Table)
			//}
			//
			//foreignKey := &sql2.ForeignKey{
			//	Name:         "fk_" + f.Name,
			//	Table:        tableName,
			//	Field:        f.Name,
			//	ForeignTable: d.FullTableName(foreignTable),
			//	ForeignField: foreignField.Name,
			//}
			//d.AddForeignKeySql(table.Name, foreignKey.String())
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

	orderBy := strings.Join(orderByFields, ",")
	sb.WriteString(fmt.Sprintf(") ENGINE = ReplacingMergeTree() ORDER BY (%s);", orderBy))

	//sb.WriteString(");\n")

	//d.AddForeignKeySql(tableName, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT fk_block FOREIGN KEY (block_number) REFERENCES %s.block(number) ON DELETE CASCADE", tableName, d.schemaName))
	d.AddCreateTableSql(table.Name, sb.String())

	return nil

}

func (d *DialectClickHouse) CreateDatabase(tx *sql.Tx) error {
	_, err := tx.Exec(fmt.Sprintf(staticSqlCreatDatabase, d.schemaName))
	if err != nil {
		return fmt.Errorf("executing static staticSqlCreatDatabase: %w\n%s", err, staticSqlCreatDatabase)
	}

	_, err = tx.Exec(fmt.Sprintf(staticSqlCreateBlock, d.schemaName))
	if err != nil {
		return fmt.Errorf("executing static staticSqlCreateBlock: %w\n%s", err, staticSqlCreateBlock)
	}

	for _, statement := range d.CreateTableSql {
		d.Logger.Info("executing create statement", zap.String("sql", statement))
		_, err := tx.Exec(statement)
		if err != nil {
			return fmt.Errorf("executing create statement: %w %s", err, statement)
		}
	}
	return nil
}

func (d *DialectClickHouse) ApplyConstraints(tx *sql.Tx) error {
	startAt := time.Now()
	for _, constraint := range d.PrimaryKeySql {
		d.Logger.Info("executing pk statement", zap.String("sql", constraint.Sql))
		_, err := tx.Exec(constraint.Sql)
		if err != nil {
			return fmt.Errorf("executing pk statement: %w %s", err, constraint.Sql)
		}
	}
	for _, constraint := range d.UniqueConstraintSql {
		d.Logger.Info("executing unique statement", zap.String("sql", constraint.Sql))
		_, err := tx.Exec(constraint.Sql)
		if err != nil {
			return fmt.Errorf("executing unique statement: %w %s", err, constraint.Sql)
		}
	}
	for _, constraint := range d.ForeignKeySql {
		d.Logger.Info("executing fk constraint statement", zap.String("sql", constraint.Sql))
		_, err := tx.Exec(constraint.Sql)
		if err != nil {
			return fmt.Errorf("executing fk constraint statement: %w %s", err, constraint.Sql)
		}
	}
	d.Logger.Info("applying constraints", zap.Duration("duration", time.Since(startAt)))
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
		//buf = append(buf, []byte(sql)...)
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

	//todo: hum... is this useful?
	//var accumulators []string
	//for _, sql := range d.InsertSql {
	//	accumulators = append(accumulators, sql)
	//}
	//sort.Strings(accumulators)
	//for _, sql := range accumulators {
	//	buf = append(buf, []byte(sql)...)
	//}

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
