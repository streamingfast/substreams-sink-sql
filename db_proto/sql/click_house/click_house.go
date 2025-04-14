package click_house

//import (
//	"fmt"
//	"strings"
//
//	"github.com/streamingfast/substreams-sink-sql/db_proto/sql"
//	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/schema"
//)
//
//type DialectClickHouse struct {
//}
//
//func NewDialectClickHouse() *DialectClickHouse {
//	return &DialectClickHouse{}
//}
//
//func (p *DialectClickHouse) Init(schema *schema.Schema) error {
//
//	schema.insertSql["block"] =
//		fmt.Sprintf("INSERT INTO %s (number, hash, timestamp) VALUES ($1, $2, $3) RETURNING number", sql.TableName(schema, "block"))
//
//	schema.PrimaryKeyStatements = append(schema.PrimaryKeyStatements, &Constraint{
//		"blocks",
//		fmt.Sprintf("alter table %s.block add constraint block_pk primary key (number);", schema.String()),
//	})
//
//	schema.insertSql["cursor"] =
//		fmt.Sprintf("INSERT INTO %s (name, cursor) VALUES ($1, $2) ON CONFLICT (name) DO UPDATE SET cursor = $2", sql.TableName(schema, "cursor"))
//	return nil
//}
//
//func (p *DialectClickHouse) HandleTable(schema *schema.Schema, table *schema.Table) error {
//	err := p.createTable(schema, table)
//	if err != nil {
//		return fmt.Errorf("creating table %q: %w", table.Name, err)
//	}
//
//	err = p.createInsertFromDescriptor(schema, table)
//	if err != nil {
//		return fmt.Errorf("creating insert from descriptor for table %q: %w", table.Name, err)
//	}
//	return nil
//}
//
//func (p *DialectClickHouse) createTable(schema *schema.Schema, table *schema.Table) error {
//
//	var sb strings.Builder
//
//	tableName := table.FullName(schema)
//
//	sb.WriteString(fmt.Sprintf("CREATE TABLE  IF NOT EXISTS %s (", tableName))
//	var primaryKeyFieldName string
//	if table.PrimaryKey.Generated {
//		schema.PrimaryKeyStatements = append(schema.PrimaryKeyStatements, &Constraint{
//			table.Name,
//			fmt.Sprintf("alter table %s add constraint %s_pk primary key (%s);", tableName, table.Name, table.PrimaryKey.Name),
//		})
//		sb.WriteString(fmt.Sprintf("%s SERIAL,", table.PrimaryKey.Name))
//	} else {
//		pk := table.PrimaryKey
//		primaryKeyFieldName = pk.Name
//		schema.PrimaryKeyStatements = append(schema.PrimaryKeyStatements, &Constraint{
//			table.Name,
//			fmt.Sprintf("alter table %s add constraint %s_pk primary key (%s);", tableName, table.Name, primaryKeyFieldName),
//		})
//		sb.WriteString(fmt.Sprintf("%s %s,", pk.Name, pk.DataType))
//	}
//
//	sb.WriteString(" block_number INTEGER NOT NULL,")
//
//	if table.ChildOf != nil {
//		parentTable, parentFound := schema.tableRegistry[table.ChildOf.ParentTable]
//		if !parentFound {
//			return fmt.Errorf("parent table %q not found", table.Name)
//		}
//		fieldFound := false
//		for _, parentField := range parentTable.Columns {
//
//			if parentField.Name == table.ChildOf.ParentTableField {
//
//				sb.WriteString(fmt.Sprintf("%s %s NOT NULL,", parentField.Name, parentField.DataType))
//
//				foreignKey := &foreignKey{
//					name:         "fk_" + table.ChildOf.ParentTable,
//					table:        tableName,
//					field:        table.ChildOf.ParentTableField,
//					foreignTable: parentTable.FullName(schema),
//					foreignField: parentField.Name,
//				}
//				c := &Constraint{
//					table: tableName,
//					sql:   foreignKey.String(),
//				}
//				schema.ForeignKeyStatements = append(schema.ForeignKeyStatements, c)
//
//				fieldFound = true
//				break
//			}
//		}
//		if !fieldFound {
//			return fmt.Errorf("field %q not found in table %q", table.ChildOf.ParentTableField, table.ChildOf.ParentTable)
//		}
//	}
//
//	for _, f := range table.Columns {
//		if f.Name == primaryKeyFieldName {
//			continue
//		}
//
//		fieldName := f.Name
//		fieldType := f.DataType
//		if f.IsUnique {
//			//fieldType = fieldType + " UNIQUE"
//			schema.UniqueConstraintStatements = append(schema.UniqueConstraintStatements, &Constraint{
//				table.Name,
//				fmt.Sprintf("alter table %s add constraint %s_%s_unique unique (%s);", tableName, table.Name, fieldName, fieldName),
//			})
//
//		}
//
//		switch {
//		case f.IsRepeated:
//			continue
//		case f.IsMessage:
//			childTable, found := schema.tableRegistry[f.Message]
//			if !found {
//				continue
//			}
//			foreignKey := &foreignKey{
//				name:         "fk_" + childTable.Name,
//				table:        tableName,
//				field:        f.Name,
//				foreignTable: childTable.FullName(schema),
//				foreignField: childTable.PrimaryKey.Name,
//			}
//			c := &Constraint{
//				table: tableName,
//				sql:   foreignKey.String(),
//			}
//			schema.ForeignKeyStatements = append(schema.ForeignKeyStatements, c)
//		case f.ForeignKey != nil:
//			foreignTable, found := schema.tableRegistry[f.ForeignKey.Table]
//			if !found {
//				return fmt.Errorf("foreign table %q not found", f.ForeignKey.Table)
//			}
//
//			var foreignField *schema.Column
//			for _, field := range foreignTable.Columns {
//				if field.Name == f.ForeignKey.TableField {
//					foreignField = field
//					break
//				}
//			}
//			if foreignField == nil {
//				return fmt.Errorf("foreign field %q not found in table %q", f.ForeignKey.TableField, f.ForeignKey.Table)
//			}
//
//			foreignKey := &foreignKey{
//				name:         "fk_" + f.Name,
//				table:        tableName,
//				field:        f.Name,
//				foreignTable: foreignTable.FullName(schema),
//				foreignField: foreignField.Name,
//			}
//			c := &Constraint{
//				table: tableName,
//				sql:   foreignKey.String(),
//			}
//			schema.ForeignKeyStatements = append(schema.ForeignKeyStatements, c)
//		}
//		sb.WriteString(fmt.Sprintf("%s %s", fieldName, fieldType))
//		sb.WriteString(",")
//	}
//
//	//removing the last comma since it is complicated to removing it before
//	temp := sb.String()
//	temp = temp[:len(temp)-1]
//	sb = strings.Builder{}
//	sb.WriteString(temp)
//
//	sb.WriteString(");\n")
//
//	c := &Constraint{
//		table: tableName,
//		sql:   fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT fk_block FOREIGN KEY (block_number) REFERENCES %s.block(number) ON DELETE CASCADE", tableName, schema.String()),
//	}
//
//	schema.ForeignKeyStatements = append(schema.ForeignKeyStatements, c)
//	schema.TableCreateStatements[tableName] = sb.String()
//
//	return nil
//
//}
//
//func (p *DialectClickHouse) createInsertFromDescriptor(schema *schema.Schema, table *schema.Table) error {
//	tableName := table.FullName(schema)
//	fields := table.Columns
//
//	var fieldNames []string
//	var placeholders []string
//
//	fieldCount := 0
//	returningField := table.PrimaryKey.Name
//
//	fieldCount++
//	fieldNames = append(fieldNames, "block_number")
//	placeholders = append(placeholders, fmt.Sprintf("$%d", fieldCount))
//
//	if pk := table.PrimaryKey; pk != nil && !pk.Generated {
//		fieldCount++
//		returningField = pk.Name
//		fieldNames = append(fieldNames, pk.Name)
//		placeholders = append(placeholders, fmt.Sprintf("$%d", fieldCount)) //$1
//	}
//
//	if table.ChildOf != nil {
//		fieldCount++
//		fieldNames = append(fieldNames, table.ChildOf.ParentTableField)
//		placeholders = append(placeholders, fmt.Sprintf("$%d", fieldCount))
//	}
//
//	for _, field := range fields {
//		if field.Name == returningField {
//			continue
//		}
//		if field.IsRepeated || field.IsExtension { //not a direct child
//			continue
//		}
//		fieldCount++
//		fieldNames = append(fieldNames, field.Name)
//		placeholders = append(placeholders, fmt.Sprintf("$%d", fieldCount))
//	}
//
//	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) RETURNING %s",
//		tableName,
//		strings.Join(fieldNames, ", "),
//		strings.Join(placeholders, ", "),
//		returningField,
//	)
//
//	schema.insertSql[tableName] = insertSQL
//
//	return nil
//}
