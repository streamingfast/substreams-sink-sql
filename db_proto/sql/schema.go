package sql

import (
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"sort"

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
		number integer,
		hash TEXT NOT NULL,
		timestamp TIMESTAMP NOT NULL
	);
`

type Schema struct {
	Name                       string
	HasGeneratedPrimaryKey     bool
	tableRegistry              map[string]*Table
	TableCreateStatements      map[string]string
	PrimaryKeyStatements       []*Constraint
	ForeignKeyStatements       []*Constraint
	UniqueConstraintStatements []*Constraint
	insertSql                  map[string]string
	logger                     *zap.Logger
	rootMessageDescriptor      *desc.MessageDescriptor
}

func NewSchema(name string, rootMessageDescriptor *desc.MessageDescriptor, dialect Dialect, logger *zap.Logger) (*Schema, error) {
	s := &Schema{
		Name:                  name,
		insertSql:             make(map[string]string),
		TableCreateStatements: make(map[string]string),
		tableRegistry:         make(map[string]*Table),
		logger:                logger,
		rootMessageDescriptor: rootMessageDescriptor,
	}

	err := s.init(rootMessageDescriptor, dialect)
	if err != nil {
		return nil, fmt.Errorf("initializing schema: %w", err)
	}
	return s, nil
}

func (s *Schema) ChangeName(name string, dialect Dialect) error {
	s.Name = name
	s.insertSql = make(map[string]string)
	s.TableCreateStatements = make(map[string]string)
	s.tableRegistry = make(map[string]*Table)
	s.PrimaryKeyStatements = make([]*Constraint, 0)
	s.ForeignKeyStatements = make([]*Constraint, 0)
	s.UniqueConstraintStatements = make([]*Constraint, 0)
	err := s.init(s.rootMessageDescriptor, dialect)
	if err != nil {
		return fmt.Errorf("changing schema name: %w", err)
	}

	return nil
}

func (s *Schema) init(rootMessageDescriptor *desc.MessageDescriptor, dialect Dialect) error {

	err := dialect.Init(s)
	if err != nil {
		return fmt.Errorf("initializing dialect: %w", err)
	}

	err = s.walkMessageDescriptor(rootMessageDescriptor, func(md *desc.MessageDescriptor) error {
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
		if table.PrimaryKey.Generated {
			s.HasGeneratedPrimaryKey = true
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("walking and creating table message descriptors registry: %q: %w", rootMessageDescriptor.GetName(), err)
	}

	for _, table := range s.tableRegistry {
		if _, found := s.TableCreateStatements[table.FullName(s)]; found {
			return nil
		}

		err := dialect.HandleTable(s, table)
		if err != nil {
			return fmt.Errorf("creating create table statement for table %q: %w", table.Name, err)
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

func (s *Schema) Hash() string {

	h := fnv.New64a()

	var buf []byte

	// Hash tableCreateStatements
	var sqls []string
	for _, sql := range s.TableCreateStatements {
		sqls = append(sqls, sql)
		//buf = append(buf, []byte(sql)...)
	}

	sort.Strings(sqls)
	for _, sql := range sqls {
		buf = append(buf, []byte(sql)...)
	}

	var pk []string
	for _, constraint := range s.PrimaryKeyStatements {
		pk = append(pk, constraint.sql)
	}
	sort.Strings(pk)
	for _, constraint := range pk {
		buf = append(buf, []byte(constraint)...)
	}

	var fk []string
	for _, constraint := range s.ForeignKeyStatements {
		fk = append(fk, constraint.sql)
	}
	sort.Strings(fk)
	for _, constraint := range fk {
		buf = append(buf, []byte(constraint)...)
	}

	var uniques []string
	for _, constraint := range s.UniqueConstraintStatements {
		uniques = append(uniques, constraint.sql)
	}
	sort.Strings(uniques)
	for _, constraint := range uniques {
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
