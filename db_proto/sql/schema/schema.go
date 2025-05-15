package schema

import (
	"fmt"

	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/streamingfast/substreams-sink-sql/pb/sf/substreams/sink/sql/schema/v1"
	"github.com/streamingfast/substreams-sink-sql/proto"
	"go.uber.org/zap"
)

type Schema struct {
	Name                  string
	TableRegistry         map[string]*Table
	logger                *zap.Logger
	rootMessageDescriptor *desc.MessageDescriptor
	withProtoOption       bool
}

func NewSchema(name string, rootMessageDescriptor *desc.MessageDescriptor, withProtoOption bool, logger *zap.Logger) (*Schema, error) {
	logger.Info("creating schema", zap.String("name", name), zap.String("root_message_descriptor", rootMessageDescriptor.GetName()), zap.Bool("with_proto_option", withProtoOption))
	s := &Schema{
		Name:                  name,
		TableRegistry:         make(map[string]*Table),
		logger:                logger,
		rootMessageDescriptor: rootMessageDescriptor,
		withProtoOption:       withProtoOption,
	}

	err := s.init(rootMessageDescriptor)
	if err != nil {
		return nil, fmt.Errorf("initializing schema: %w", err)
	}
	return s, nil
}

func (s *Schema) ChangeName(name string) error {
	s.Name = name
	s.TableRegistry = make(map[string]*Table)
	err := s.init(s.rootMessageDescriptor)
	if err != nil {
		return fmt.Errorf("changing schema name: %w", err)
	}

	return nil
}

func (s *Schema) init(rootMessageDescriptor *desc.MessageDescriptor) error {
	s.logger.Info("initializing schema", zap.String("name", s.Name), zap.String("root_message_descriptor", rootMessageDescriptor.GetName()))
	err := s.walkMessageDescriptor(rootMessageDescriptor, 0, func(md *desc.MessageDescriptor, ordinal int) error {
		s.logger.Debug("creating table message descriptor", zap.String("message_descriptor_name", md.GetName()), zap.Int("ordinal", ordinal))
		tableInfo := proto.TableInfo(md)
		if tableInfo == nil {
			if s.withProtoOption {
				return nil
			}
			tableInfo = &schema.Table{
				Name:    md.GetName(),
				ChildOf: nil,
			}
		}
		if _, found := s.TableRegistry[tableInfo.Name]; found {
			return nil
		}
		table, err := NewTable(md, tableInfo, ordinal)
		s.logger.Debug("created table message descriptor", zap.String("message_descriptor_name", md.GetName()), zap.Int("ordinal", ordinal), zap.String("table_name", table.Name))
		if err != nil {
			return fmt.Errorf("creating table message descriptor: %w", err)
		}
		s.TableRegistry[tableInfo.Name] = table
		return nil
	})

	if err != nil {
		return fmt.Errorf("walking and creating table message descriptors registry: %q: %w", rootMessageDescriptor.GetName(), err)
	}

	return nil
}

func (s *Schema) walkMessageDescriptor(md *desc.MessageDescriptor, ordinal int, task func(md *desc.MessageDescriptor, ordinal int) error) error {
	s.logger.Debug("walking message descriptor", zap.String("message_descriptor_name", md.GetName()), zap.Int("ordinal", ordinal))
	for _, field := range md.GetFields() {
		s.logger.Debug("walking field", zap.String("field_name", field.GetName()), zap.String("field_type", field.GetType().String()))
		if field.GetType() == descriptor.FieldDescriptorProto_TYPE_MESSAGE {
			err := s.walkMessageDescriptor(field.GetMessageType(), ordinal+1, task)
			if err != nil {
				return fmt.Errorf("walking field %q message descriptor: %w", field.GetName(), err)
			}
		}
	}

	err := task(md, ordinal)
	if err != nil {
		return fmt.Errorf("running task on message descriptor %q: %w", md.GetName(), err)
	}

	return nil
}

func (s *Schema) String() string {
	return fmt.Sprintf("%s", s.Name)
}
