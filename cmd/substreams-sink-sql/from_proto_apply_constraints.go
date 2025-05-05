package main

import (
	"database/sql"
	"fmt"

	"github.com/jhump/protoreflect/desc"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	. "github.com/streamingfast/cli"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams-sink-sql/db_changes/db"
	"github.com/streamingfast/substreams-sink-sql/db_proto/proto"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/postgres"
	schema2 "github.com/streamingfast/substreams-sink-sql/db_proto/sql/schema"
	"google.golang.org/protobuf/types/descriptorpb"
)

var fromProtoApplyConstraintsCmd = Command(fromProtoApplyConstraintsCmdE,
	"from-proto-apply-constraints <dsn> [<manifest> [<module_name>]]",
	"",
	ExactArgs(3),
	Flags(func(flags *pflag.FlagSet) {
		sink.AddFlagsToSet(flags, ignoreUndoBufferSize{})
	}),
)

func fromProtoApplyConstraintsCmdE(cmd *cobra.Command, args []string) error {
	//app := NewApplication(cmd.Context())

	dsnString := args[0]
	manifestPath := args[1]
	outputModuleName := args[2]
	if outputModuleName == "" {
		outputModuleName = sink.InferOutputModuleFromPackage
	}

	spkg, _, _, _, err := sink.ReadManifestAndModuleAndBlockRange(manifestPath, "", nil, outputModuleName, "", false, "", zlog)
	if err != nil {
		return fmt.Errorf("reading manifest: %w", err)
	}

	outputType := proto.ModuleOutputType(spkg, outputModuleName)
	if outputType == "" {
		return fmt.Errorf("could not find output type for module %s", outputModuleName)
	}

	protoFiles := map[string]*descriptorpb.FileDescriptorProto{}
	for _, file := range spkg.ProtoFiles {
		protoFiles[file.GetName()] = file
	}

	deps, err := proto.ResolveDependencies(protoFiles)
	if err != nil {
		return fmt.Errorf("resolving dependencies: %w", err)
	}

	fileDescriptor, err := proto.FileDescriptorForOutputType(spkg, err, deps, outputType)
	if err != nil {
		return fmt.Errorf("finding file descriptor for output type %q: %w", outputType, err)
	}

	var rootMessageDescriptor *desc.MessageDescriptor
	for _, messageDescriptor := range fileDescriptor.GetMessageTypes() {
		name := messageDescriptor.GetFullyQualifiedName()
		if name == outputType {
			rootMessageDescriptor = messageDescriptor
			break
		}
	}
	dsn, err := db.ParseDSN(dsnString)
	if err != nil {
		return fmt.Errorf("parsing dsn: %w", err)
	}

	schemaName := dsn.Schema()
	schema, err := schema2.NewSchema(schemaName, rootMessageDescriptor, zlog)
	if err != nil {
		return fmt.Errorf("creating schema: %w", err)
	}
	fmt.Println(schema.String())

	connectionString := dsn.ConnString()
	fmt.Println(connectionString)
	sqlDB, err := sql.Open(dsn.Driver(), connectionString)
	if err != nil {
		return fmt.Errorf("open db connection: %w", err)
	}

	tx, err := sqlDB.BeginTx(cmd.Context(), nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	sqlDialect, err := postgres.NewDialectPostgres(schemaName, schema.TableRegistry, zlog)
	if err != nil {
		return fmt.Errorf("creating dialect: %w", err)
	}

	err = sqlDialect.ApplyConstraints(tx)
	if err != nil {
		err := tx.Rollback()
		if err != nil {
			return fmt.Errorf("rollback tx: %w", err)
		}
		return fmt.Errorf("apply constraints: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	fmt.Println("Goodbye")

	return nil
}
