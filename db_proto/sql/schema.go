package sql

import (
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"

	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/streamingfast/substreams-sink-sql/proto"
	"go.uber.org/zap"
)

const static_sql = `
	CREATE SCHEMA IF NOT EXISTS "%s";

	CREATE TABLE IF NOT EXISTS "%s".sink_info (
		schema_hash TEXT PRIMARY KEY
	);

	CREATE TABLE IF NOT EXISTS "%s".cursor (
		name TEXT PRIMARY KEY,
		cursor TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS "%s".block (
		number integer PRIMARY KEY,
		hash TEXT NOT NULL,
		timestamp TIMESTAMP NOT NULL
	);
`

type Schema struct {
	Name                  string
	tableRegistry         map[string]*Table
	tableCreateStatements map[string]string
	constraintStatements  []*Constraint
	insertSql             map[string]string
	logger                *zap.Logger
	rootMessageDescriptor *desc.MessageDescriptor
}

func NewSchema(name string, rootMessageDescriptor *desc.MessageDescriptor, logger *zap.Logger) (*Schema, error) {
	s := &Schema{
		Name:                  name,
		insertSql:             make(map[string]string),
		tableCreateStatements: make(map[string]string),
		tableRegistry:         make(map[string]*Table),
		constraintStatements:  make([]*Constraint, 0),
		logger:                logger,
		rootMessageDescriptor: rootMessageDescriptor,
	}

	err := s.init(rootMessageDescriptor)
	if err != nil {
		return nil, fmt.Errorf("initializing schema: %w", err)
	}
	return s, nil
}

func (s *Schema) ChangeName(name string) error {
	s.Name = name
	s.insertSql = make(map[string]string)
	s.tableCreateStatements = make(map[string]string)
	s.tableRegistry = make(map[string]*Table)
	s.constraintStatements = make([]*Constraint, 0)
	err := s.init(s.rootMessageDescriptor)
	if err != nil {
		return fmt.Errorf("changing schema name: %w", err)
	}

	return nil
}

func (s *Schema) init(rootMessageDescriptor *desc.MessageDescriptor) error {

	s.insertSql["block"] =
		fmt.Sprintf("INSERT INTO %s (number, hash, timestamp) VALUES ($1, $2, $3) RETURNING number", TableName(s, "block"))

	s.insertSql["cursor"] =
		fmt.Sprintf("INSERT INTO %s (name, cursor) VALUES ($1, $2) ON CONFLICT (name) DO UPDATE SET cursor = $2", TableName(s, "cursor"))

	err := s.walkMessageDescriptor(rootMessageDescriptor, func(md *desc.MessageDescriptor) error {
		tableInfo := proto.TableInfo(md)
		if tableInfo == nil {
			return nil
		}
		if _, found := s.tableRegistry[tableInfo.Name]; found {
			return nil
		}
		table, err := NewTable(md)
		if err != nil {
			return fmt.Errorf("creating table message descriptor: %w", err)
		}
		s.tableRegistry[tableInfo.Name] = table
		return nil
	})

	if err != nil {
		return fmt.Errorf("walking and creating table message descriptors registry: %q: %w", rootMessageDescriptor.GetName(), err)
	}

	for _, table := range s.tableRegistry {
		err := s.createTableStatement(table)
		if err != nil {
			return fmt.Errorf("creating create table statement for table %q: %w", table.Name, err)
		}

	}

	for _, table := range s.tableRegistry {
		err := s.createInsertFromDescriptor(table)
		if err != nil {
			return fmt.Errorf("walking and creating insert statement: %q: %w", table.Name, err)
		}
	}

	return nil
}

func (s *Schema) walkMessageDescriptor(md *desc.MessageDescriptor, task func(md *desc.MessageDescriptor) error) error {
	for _, field := range md.GetFields() {
		if field.GetType() == descriptor.FieldDescriptorProto_TYPE_MESSAGE {
			err := s.walkMessageDescriptor(field.GetMessageType(), task)
			if err != nil {
				return fmt.Errorf("walking field %q message descriptor: %w", field.GetName(), err)
			}
		}
	}

	err := task(md)
	if err != nil {
		return fmt.Errorf("running task on message descriptor %q: %w", md.GetName(), err)
	}

	return nil
}

func (s *Schema) createTableStatement(table *Table) error {
	if _, found := s.tableCreateStatements[table.FullName(s)]; found {
		return nil
	}

	var sb strings.Builder

	tableName := table.FullName(s)

	sb.WriteString(fmt.Sprintf("CREATE TABLE  IF NOT EXISTS %s (", tableName))
	var primaryKeyFieldName string
	if table.PrimaryKey == nil {
		sb.WriteString("id SERIAL PRIMARY KEY,")
	} else {
		pk := table.PrimaryKey
		primaryKeyFieldName = pk.Name
		sb.WriteString(fmt.Sprintf("%s %s PRIMARY KEY,", pk.Name, pk.DataType))
	}

	sb.WriteString(" block_number INTEGER NOT NULL,")

	if table.ChildOf != nil {
		parentTable, parentFound := s.tableRegistry[table.ChildOf.ParentTable]
		if !parentFound {
			return fmt.Errorf("parent table %q not found", table.Name)
		}
		fieldFound := false
		for _, parentField := range parentTable.Columns {

			if parentField.Name == table.ChildOf.ParentTableField {

				sb.WriteString(fmt.Sprintf("%s %s NOT NULL,", parentField.Name, parentField.DataType))

				foreignKey := &foreignKey{
					name:         "fk_" + table.ChildOf.ParentTable,
					table:        tableName,
					field:        table.ChildOf.ParentTableField,
					foreignTable: parentTable.FullName(s),
					foreignField: parentField.Name,
				}
				c := &Constraint{
					table: tableName,
					sql:   foreignKey.String(),
				}
				s.constraintStatements = append(s.constraintStatements, c)

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
		fieldType := f.DataType
		if f.IsUnique {
			fieldType = fieldType + " UNIQUE"
		}

		switch {
		case f.IsRepeated:
			continue
		case f.IsMessage:
			childTable, found := s.tableRegistry[f.Message]
			if !found {
				continue
			}
			foreignKey := &foreignKey{
				name:         "fk_" + childTable.Name,
				table:        tableName,
				field:        f.Name,
				foreignTable: childTable.FullName(s),
				foreignField: childTable.PrimaryKey.Name,
			}
			c := &Constraint{
				table: tableName,
				sql:   foreignKey.String(),
			}
			s.constraintStatements = append(s.constraintStatements, c)
		case f.ForeignKey != nil:
			foreignTable, found := s.tableRegistry[f.ForeignKey.Table]
			if !found {
				return fmt.Errorf("foreign table %q not found", f.ForeignKey.Table)
			}

			var foreignField *Column
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
				foreignTable: foreignTable.FullName(s),
				foreignField: foreignField.Name,
			}
			c := &Constraint{
				table: tableName,
				sql:   foreignKey.String(),
			}
			s.constraintStatements = append(s.constraintStatements, c)
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

	c := &Constraint{
		table: tableName,
		sql:   fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT fk_block FOREIGN KEY (block_number) REFERENCES %s.block(number) ON DELETE CASCADE", tableName, s.String()),
	}

	s.constraintStatements = append(s.constraintStatements, c)
	s.tableCreateStatements[tableName] = sb.String()

	return nil

}

func (s *Schema) createInsertFromDescriptor(table *Table) error {
	tableName := table.FullName(s)
	fields := table.Columns

	var fieldNames []string
	var placeholders []string

	fieldCount := 0
	returningField := "id"

	fieldCount++
	fieldNames = append(fieldNames, "block_number")
	placeholders = append(placeholders, fmt.Sprintf("$%d", fieldCount))

	if pk := table.PrimaryKey; pk != nil {
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

	s.insertSql[tableName] = insertSQL

	return nil
}

func (s *Schema) Hash() string {

	h := fnv.New64a()

	var buf []byte

	// Hash tableCreateStatements
	var sqls []string
	for _, sql := range s.tableCreateStatements {
		sqls = append(sqls, sql)
		//buf = append(buf, []byte(sql)...)
	}

	sort.Strings(sqls)
	for _, sql := range sqls {
		buf = append(buf, []byte(sql)...)
	}

	var constraints []string
	for _, constraint := range s.constraintStatements {
		constraints = append(constraints, constraint.sql)
	}
	sort.Strings(constraints)
	for _, constraint := range constraints {
		buf = append(buf, []byte(constraint)...)
	}

	var inserts []string
	for _, sql := range s.insertSql {
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

func (s *Schema) String() string {
	return fmt.Sprintf("%s", s.Name)
}
