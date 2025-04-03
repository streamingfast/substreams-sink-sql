package proto

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/jhump/protoreflect/desc"
	v1 "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	"google.golang.org/protobuf/types/descriptorpb"
)

func FileDescriptorForOutputType(spkg *v1.Package, err error, deps map[string]*desc.FileDescriptor, outputType string) (*desc.FileDescriptor, error) {
	var fd *desc.FileDescriptor
	for _, p := range spkg.ProtoFiles {
		fd, err = desc.CreateFileDescriptor(p, slices.Collect(maps.Values(deps))...)
		if err != nil {
			return nil, fmt.Errorf("creating file descriptor: %w", err)
		}

		for _, md := range fd.GetMessageTypes() {
			if md.GetName() == outputType {
				break
			}
		}
	}

	if fd == nil {
		return nil, fmt.Errorf("could not find file descriptor")
	}
	return fd, nil
}

func ModuleOutputType(spkg *v1.Package, moduleName string) string {
	outputType := ""
	for _, m := range spkg.Modules.Modules {
		if m.Name == moduleName {
			outputType = strings.TrimPrefix(m.Output.Type, "proto:")
			break
		}
	}
	return outputType
}
func ResolveDependencies(protoFiles map[string]*descriptorpb.FileDescriptorProto) (map[string]*desc.FileDescriptor, error) {
	out := map[string]*desc.FileDescriptor{}
	for _, protoFile := range protoFiles {
		err := resolveDependencies(protoFile, protoFiles, out)
		if err != nil {
			return nil, fmt.Errorf("error resolving dependencies: %w", err)
		}
	}

	return out, nil
}

func resolveDependencies(protoFile *descriptorpb.FileDescriptorProto, protoFiles map[string]*descriptorpb.FileDescriptorProto, deps map[string]*desc.FileDescriptor) error {
	if deps[protoFile.GetName()] != nil {
		return nil
	}
	if len(protoFile.Dependency) != 0 {
		for _, dep := range protoFile.Dependency {
			depProtoFile, found := protoFiles[dep]
			if !found {
				return fmt.Errorf("could not find proto file for dependency %q", dep)
			}
			err := resolveDependencies(depProtoFile, protoFiles, deps)
			if err != nil {
				return fmt.Errorf("error resolving dependencies: %w", err)
			}
		}
	}

	d, err := desc.CreateFileDescriptor(protoFile, slices.Collect(maps.Values(deps))...)
	if err != nil {
		return fmt.Errorf("creating file descriptor: %w", err)
	}

	deps[protoFile.GetName()] = d
	return nil
}

//func ResolveDependencies(fds []*descriptorpb.FileDescriptorProto, fileName string, stack []string, deps map[string]*desc.FileDescriptor) error {
//	if deps[fileName] != nil {
//		return nil
//	}
//
//	tabs := ""
//	for range len(stack) {
//		tabs += "\t"
//	}
//
//	p := func(format string, a ...any) {
//		fmt.Print(tabs)
//		fmt.Printf(format, a...)
//		fmt.Println()
//	}
//
//	p("resolving dependencies for %s", fileName)
//	p("stack:")
//	for _, s := range stack {
//		p("\t%s", s)
//	}
//	for _, fd := range fds {
//		stack = append(stack, fd.GetName())
//		if len(fd.Dependency) != 0 {
//			p("dependencies:")
//			for _, dep := range fd.Dependency {
//				p("\t%s", dep)
//				for _, s := range stack {
//
//					if s == dep {
//						return fmt.Errorf("circular dependency detected: %s is already in the stack", dep)
//					}
//				}
//				err := ResolveDependencies([]*descriptorpb.FileDescriptorProto{fd}, dep, stack, deps)
//				if err != nil {
//					return err
//				}
//			}
//		}
//
//		d, err := desc.CreateFileDescriptor(fd, slices.Collect(maps.Values(deps))...)
//		if err != nil {
//			return fmt.Errorf("creating file descriptor: %w", err)
//		}
//		//pop it
//		name := ""
//		name, stack = stack[len(stack)-1], stack[:len(stack)-1]
//		if name != fd.GetName() {
//			panic("stack is corrupted")
//		}
//		deps[fd.GetName()] = d
//	}
//	return nil
//}

//func ResolveDependencies2(fds []*descriptorpb.FileDescriptorProto) ([]*desc.FileDescriptor, error) {
//	out := []*desc.FieldDescriptor{}
//
//	for _, fd := range fds {
//		fileDesc, err := desc.CreateFileDescriptor(fd, nil)
//		if err != nil {
//			return nil, fmt.Errorf("error creating file descriptor: %w", err)
//		}
//		out = append(out, fileDesc)
//	}
//	return out, nil
//	//if deps[fileName] != nil {
//	//	return nil
//	//}
//	//
//	//tabs := ""
//	//for range len(stack) {
//	//	tabs += "\t"
//	//}
//	//
//	//p := func(format string, a ...any) {
//	//	fmt.Print(tabs)
//	//	fmt.Printf(format, a...)
//	//	fmt.Println()
//	//}
//	//
//	//p("resolving dependencies for %s", fileName)
//	//p("stack:")
//	//for _, s := range stack {
//	//	p("\t%s", s)
//	//}
//	//for _, fd := range fds {
//	//
//	//	stack = append(stack, fd.GetName())
//	//
//	//	if len(fd.Dependency) != 0 {
//	//		p("dependencies:")
//	//		for _, dep := range fd.Dependency {
//	//			p("\t%s", dep)
//	//			for _, s := range stack {
//	//
//	//				if s == dep {
//	//					return fmt.Errorf("circular dependency detected: %s is already in the stack", dep)
//	//				}
//	//			}
//	//			err := ResolveDependencies(fds, dep, stack, deps)
//	//			if err != nil {
//	//				return err
//	//			}
//	//		}
//	//	}
//	//
//	//	d, err := desc.CreateFileDescriptor(fd, slices.Collect(maps.Values(deps))...)
//	//	if err != nil {
//	//		return fmt.Errorf("creating file descriptor: %w", err)
//	//	}
//	//	//pop it
//	//	name := ""
//	//	name, stack = stack[len(stack)-1], stack[:len(stack)-1]
//	//	if name != fd.GetName() {
//	//		panic("stack is corrupted")
//	//	}
//	//	deps[fd.GetName()] = d
//	//}
//	//return nil
//}
