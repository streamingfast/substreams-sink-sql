package schema

import (
	"fmt"

	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/streamingfast/substreams-sink-sql/proto"
	"go.uber.org/zap"
)

type Schema struct {
	Name                  string
	TableRegistry         map[string]*Table
	logger                *zap.Logger
	rootMessageDescriptor *desc.MessageDescriptor
}

func NewSchema(name string, rootMessageDescriptor *desc.MessageDescriptor, logger *zap.Logger) (*Schema, error) {
	s := &Schema{
		Name:                  name,
		TableRegistry:         make(map[string]*Table),
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
	s.TableRegistry = make(map[string]*Table)
	err := s.init(s.rootMessageDescriptor)
	if err != nil {
		return fmt.Errorf("changing schema name: %w", err)
	}

	return nil
}

func (s *Schema) init(rootMessageDescriptor *desc.MessageDescriptor) error {
	err := s.walkMessageDescriptor(rootMessageDescriptor, 0, func(md *desc.MessageDescriptor, ordinal int) error {
		tableInfo := proto.TableInfo(md)
		if tableInfo == nil {
			return nil
		}
		if _, found := s.TableRegistry[tableInfo.Name]; found {
			return nil
		}
		table, err := NewTable(md, ordinal)
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
	for _, field := range md.GetFields() {
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
