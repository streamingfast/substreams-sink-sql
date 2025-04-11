package dialect

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"time"

	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/dialect/postgres"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/schema"
	"go.uber.org/zap"
)

const postgres_static_sql = `
	CREATE SCHEMA IF NOT EXISTS "%s";

	CREATE TABLE IF NOT EXISTS "%s".sink_info (
		schema_hash TEXT PRIMARY KEY
	);

	CREATE TABLE IF NOT EXISTS "%s".cursor (
		name TEXT PRIMARY KEY,
		cursor TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS "%s".block (
		number integer,
		hash TEXT NOT NULL,
		timestamp TIMESTAMP NOT NULL
	);
`

type DialectPostgres struct {
	BaseDialect
	schemaName string
}

func NewDialectPostgres(schemaName string, tableRegistry map[string]*schema.Table, logger *zap.Logger) (*DialectPostgres, error) {
	d := &DialectPostgres{
		BaseDialect: BaseDialect{
			tableRegistry:  tableRegistry,
			createTableSql: map[string]string{},
			insertSql:      map[string]string{},
			logger:         logger,
		},
		schemaName: schemaName,
	}
	err := d.init()
	if err != nil {
		return nil, fmt.Errorf("initializing dialect: %w", err)
	}

	for _, table := range tableRegistry {
		err := d.handleTable(table)
		if err != nil {
			return nil, fmt.Errorf("handling table %q: %w", table.Name, err)
		}
	}

	return d, nil
}
func (d *DialectPostgres) GetInsert(table string) string {
	return d.insertSql[table]
}

func (d *DialectPostgres) GetInserts() map[string]string {
	return d.insertSql
}

func (d *DialectPostgres) init() error {

	d.AddInsertSql("block", fmt.Sprintf("INSERT INTO %s (number, hash, timestamp) VALUES ($1, $2, $3) RETURNING number", tableName(d.schemaName, "block")))
	d.AddPrimaryKeySql("block", fmt.Sprintf("alter table %s.block add constraint block_pk primary key (number);", d.schemaName))

	d.primaryKeySql = append(d.primaryKeySql, &Constraint{
		"blocks",
		fmt.Sprintf("alter table %s.block add constraint block_pk primary key (number);", d.schemaName),
	})

	d.AddInsertSql("cursor", fmt.Sprintf("INSERT INTO %s (name, cursor) VALUES ($1, $2) ON CONFLICT (name) DO UPDATE SET cursor = $2", tableName(d.schemaName, "cursor")))

	return nil
}

func (d *DialectPostgres) handleTable(table *schema.Table) error {
	err := d.createTable(table)
	if err != nil {
		return fmt.Errorf("creating table %q: %w", table.Name, err)
	}

	err = d.createInsertFromDescriptor(table)
	if err != nil {
		return fmt.Errorf("creating insert from descriptor for table %q: %w", table.Name, err)
	}
	return nil
}

func (d *DialectPostgres) createTable(table *schema.Table) error {
	var sb strings.Builder

	tableName := d.FullTableName(table)

	sb.WriteString(fmt.Sprintf("CREATE TABLE  IF NOT EXISTS %s (", tableName))
	var primaryKeyFieldName string
	if table.PrimaryKey.Generated {
		d.AddPrimaryKeySql(table.Name, fmt.Sprintf("alter table %s add constraint %s_pk primary key (%s);", tableName, table.Name, table.PrimaryKey.Name))
		sb.WriteString(fmt.Sprintf("%s SERIAL,", table.PrimaryKey.Name))
	} else {
		pk := table.PrimaryKey
		primaryKeyFieldName = pk.Name
		d.AddPrimaryKeySql(table.Name, fmt.Sprintf("alter table %s add constraint %s_pk primary key (%s);", tableName, table.Name, primaryKeyFieldName))
		sb.WriteString(fmt.Sprintf("%s %s,", pk.Name, postgres.MapFieldType(pk.DataType)))
	}

	sb.WriteString(" block_number INTEGER NOT NULL,")

	if table.ChildOf != nil {
		parentTable, parentFound := d.tableRegistry[table.ChildOf.ParentTable]
		if !parentFound {
			return fmt.Errorf("parent table %q not found", table.Name)
		}
		fieldFound := false
		for _, parentField := range parentTable.Columns {

			if parentField.Name == table.ChildOf.ParentTableField {

				sb.WriteString(fmt.Sprintf("%s %s NOT NULL,", parentField.Name, postgres.MapFieldType(parentField.DataType)))

				foreignKey := &foreignKey{
					name:         "fk_" + table.ChildOf.ParentTable,
					table:        tableName,
					field:        table.ChildOf.ParentTableField,
					foreignTable: d.FullTableName(parentTable),
					foreignField: parentField.Name,
				}

				d.AddForeignKeySql(table.Name, foreignKey.String())

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
		fieldType := postgres.MapFieldType(f.DataType)
		if f.IsUnique {
			d.AddUniqueConstraintSql(table.Name, fmt.Sprintf("alter table %s add constraint %s_%s_unique unique (%s);", tableName, table.Name, fieldName, fieldName))
		}

		switch {
		case f.IsRepeated:
			continue
		case f.IsMessage:
			childTable, found := d.tableRegistry[f.Message]
			if !found {
				continue
			}
			foreignKey := &foreignKey{
				name:         "fk_" + childTable.Name,
				table:        tableName,
				field:        f.Name,
				foreignTable: d.FullTableName(childTable),
				foreignField: childTable.PrimaryKey.Name,
			}
			d.AddForeignKeySql(table.Name, foreignKey.String())

		case f.ForeignKey != nil:
			foreignTable, found := d.tableRegistry[f.ForeignKey.Table]
			if !found {
				return fmt.Errorf("foreign table %q not found", f.ForeignKey.Table)
			}

			var foreignField *schema.Column
			for _, field := range foreignTable.Columns {
				if field.Name == f.ForeignKey.TableField {
					foreignField = field
					break
				}
			}
			if foreignField == nil {
				return fmt.Errorf("foreign field %q not found in table %q", f.ForeignKey.TableField, f.ForeignKey.Table)
			}

			foreignKey := &foreignKey{
				name:         "fk_" + f.Name,
				table:        tableName,
				field:        f.Name,
				foreignTable: d.FullTableName(foreignTable),
				foreignField: foreignField.Name,
			}
			d.AddForeignKeySql(table.Name, foreignKey.String())
		}
		sb.WriteString(fmt.Sprintf("%s %s", fieldName, fieldType))
		sb.WriteString(",")
	}

	//removing the last comma since it is complicated to removing it before
	temp := sb.String()
	temp = temp[:len(temp)-1]
	sb = strings.Builder{}
	sb.WriteString(temp)

	sb.WriteString(");\n")

	d.AddForeignKeySql(tableName, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT fk_block FOREIGN KEY (block_number) REFERENCES %s.block(number) ON DELETE CASCADE", tableName, d.schemaName))
	d.AddCreateTableSql(table.Name, sb.String())

	return nil

}

func (d *DialectPostgres) createInsertFromDescriptor(table *schema.Table) error {
	tableName := d.FullTableName(table)
	fields := table.Columns

	var fieldNames []string
	var placeholders []string

	fieldCount := 0
	returningField := table.PrimaryKey.Name

	fieldCount++
	fieldNames = append(fieldNames, "block_number")
	placeholders = append(placeholders, fmt.Sprintf("$%d", fieldCount))

	if pk := table.PrimaryKey; pk != nil && !pk.Generated {
		fieldCount++
		returningField = pk.Name
		fieldNames = append(fieldNames, pk.Name)
		placeholders = append(placeholders, fmt.Sprintf("$%d", fieldCount)) //$1
	}

	if table.ChildOf != nil {
		fieldCount++
		fieldNames = append(fieldNames, table.ChildOf.ParentTableField)
		placeholders = append(placeholders, fmt.Sprintf("$%d", fieldCount))
	}

	for _, field := range fields {
		if field.Name == returningField {
			continue
		}
		if field.IsRepeated || field.IsExtension { //not a direct child
			continue
		}
		fieldCount++
		fieldNames = append(fieldNames, field.Name)
		placeholders = append(placeholders, fmt.Sprintf("$%d", fieldCount))
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) RETURNING %s",
		tableName,
		strings.Join(fieldNames, ", "),
		strings.Join(placeholders, ", "),
		returningField,
	)
	d.AddInsertSql(table.Name, insertSQL)

	return nil
}

func (d *DialectPostgres) CreateDatabase(tx *sql.Tx) error {
	staticSql := fmt.Sprintf(postgres_static_sql, d.schemaName, d.schemaName, d.schemaName, d.schemaName)
	_, err := tx.Exec(staticSql)
	if err != nil {
		return fmt.Errorf("executing static staticSql: %w\n%s", err, staticSql)
	}

	for _, statement := range d.createTableSql {
		d.logger.Info("executing create statement", zap.String("sql", statement))
		_, err := tx.Exec(statement)
		if err != nil {
			return fmt.Errorf("executing create statement: %w %s", err, statement)
		}
	}
	return nil
}

func (d *DialectPostgres) ApplyConstraints(tx *sql.Tx) error {
	startAt := time.Now()
	for _, constraint := range d.primaryKeySql {
		d.logger.Info("executing pk statement", zap.String("sql", constraint.sql))
		_, err := tx.Exec(constraint.sql)
		if err != nil {
			return fmt.Errorf("executing pk statement: %w %s", err, constraint.sql)
		}
	}
	for _, constraint := range d.uniqueConstraintSql {
		d.logger.Info("executing unique statement", zap.String("sql", constraint.sql))
		_, err := tx.Exec(constraint.sql)
		if err != nil {
			return fmt.Errorf("executing unique statement: %w %s", err, constraint.sql)
		}
	}
	for _, constraint := range d.foreignKeySql {
		d.logger.Info("executing fk constraint statement", zap.String("sql", constraint.sql))
		_, err := tx.Exec(constraint.sql)
		if err != nil {
			return fmt.Errorf("executing fk constraint statement: %w %s", err, constraint.sql)
		}
	}
	d.logger.Info("applying constraints", zap.Duration("duration", time.Since(startAt)))
	return nil
}

func (d *DialectPostgres) GetCursorSql() string {
	return fmt.Sprintf("SELECT cursor FROM %s WHERE name = $1", tableName(d.schemaName, "cursor"))
}

func (d *DialectPostgres) FullTableName(table *schema.Table) string {
	return tableName(d.schemaName, table.Name)
}

func (d *DialectPostgres) Hash() string {
	h := fnv.New64a()

	var buf []byte

	// Hash tableCreateStatements
	var sqls []string
	for _, sql := range d.createTableSql {
		sqls = append(sqls, sql)
		//buf = append(buf, []byte(sql)...)
	}

	sort.Strings(sqls)
	for _, sql := range sqls {
		buf = append(buf, []byte(sql)...)
	}

	var pk []string
	for _, constraint := range d.primaryKeySql {
		pk = append(pk, constraint.sql)
	}
	sort.Strings(pk)
	for _, constraint := range pk {
		buf = append(buf, []byte(constraint)...)
	}

	var fk []string
	for _, constraint := range d.foreignKeySql {
		fk = append(fk, constraint.sql)
	}
	sort.Strings(fk)
	for _, constraint := range fk {
		buf = append(buf, []byte(constraint)...)
	}

	var uniques []string
	for _, constraint := range d.uniqueConstraintSql {
		uniques = append(uniques, constraint.sql)
	}
	sort.Strings(uniques)
	for _, constraint := range uniques {
		buf = append(buf, []byte(constraint)...)
	}

	var inserts []string
	for _, sql := range d.insertSql {
		inserts = append(inserts, sql)
	}
	sort.Strings(inserts)
	for _, sql := range inserts {
		buf = append(buf, []byte(sql)...)
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
