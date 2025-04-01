package sql

import (
	"fmt"
	"strings"

	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
)

func fieldName(f *desc.FieldDescriptor) string {
	fieldNameSuffix := ""
	if f.GetType() == descriptor.FieldDescriptorProto_TYPE_MESSAGE {
		fieldNameSuffix = "_id"
	}

	return fmt.Sprintf("%s%s", strings.ToLower(f.GetName()), fieldNameSuffix)
}

func fieldQuotedName(f *desc.FieldDescriptor) string {
	return Quoted(fieldName(f))
}

func Quoted(value string) string {
	return fmt.Sprintf("\"%s\"", value)
}
