package main

import (
	"database/sql"
	"fmt"

	"github.com/jhump/protoreflect/desc"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams-sink-sql/db_changes/db"
	"github.com/streamingfast/substreams-sink-sql/db_proto/data"
	"github.com/streamingfast/substreams-sink-sql/db_proto/proto"
	protosql "github.com/streamingfast/substreams-sink-sql/db_proto/sql"
	"github.com/streamingfast/substreams/manifest"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/descriptorpb"
)

var fromProtoCmd = Command(fromProtoE,
	"from-proto <dsn> [<manifest> [<module_name>]]",
	"",
	RangeArgs(2, 3),
	Flags(func(flags *pflag.FlagSet) {
		sink.AddFlagsToSet(flags, ignoreUndoBufferSize{})
		flags.StringP("substreams-endpoint", "e", "", "Substreams gRPC endpoint. If empty, will be replaced by the SUBSTREAMS_ENDPOINT_{network_name} environment variable, where `network_name` is determined from the substreams manifest. Some network names have default endpoints.")
		flags.StringP("start-block", "s", "", "Start block to stream from. If empty, will be replaced by initialBlock of the first module you are streaming. If negative, will be resolved by the server relative to the chain head")
		flags.StringP("stop-block", "t", "0", "Stop block to end stream at, exclusively. If the start-block is positive, a '+' prefix can indicate 'relative to start-block'")

	}),
)

func fromProtoE(cmd *cobra.Command, args []string) error {
	//app := NewApplication(cmd.Context())

	dsnString := args[0]
	manifestPath := args[1]
	outputModuleName := args[2]
	if outputModuleName == "" {
		outputModuleName = sink.InferOutputModuleFromPackage
	}

	endpoint := sflags.MustGetString(cmd, "substreams-endpoint")
	if endpoint == "" {
		network := sflags.MustGetString(cmd, "network")
		if network == "" {
			reader, err := manifest.NewReader(manifestPath)
			if err != nil {
				return fmt.Errorf("setup manifest reader: %w", err)
			}
			pkgBundle, err := reader.Read()
			if err != nil {
				return fmt.Errorf("read manifest: %w", err)
			}
			network = pkgBundle.Package.Network
		}
		var err error
		endpoint, err = manifest.ExtractNetworkEndpoint(network, sflags.MustGetString(cmd, "substreams-endpoint"), zlog)
		if err != nil {
			return err
		}
	}

	startBlock := sflags.MustGetString(cmd, "start-block")
	endBlock := sflags.MustGetString(cmd, "stop-block")
	blockRange := ""
	if startBlock != "" {
		blockRange = startBlock
	}
	blockRange += ":"
	if endBlock != "0" {
		blockRange += endBlock
	}

	dsn, err := db.ParseDSN(dsnString)
	if err != nil {
		return fmt.Errorf("parsing dsn: %w", err)
	}

	//todo: handle network
	//todo: handle params
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

	schemaName := dsn.Schema()
	schema, err := protosql.NewSchema(schemaName, rootMessageDescriptor, zlog)
	if err != nil {
		return fmt.Errorf("creating schema: %w", err)
	}
	fmt.Println(schema.String())

	baseSink, err := sink.NewFromViper(
		cmd,
		outputType,
		endpoint,
		manifestPath,
		outputModuleName,
		blockRange,
		zlog,
		tracer,
	)
	if err != nil {
		return fmt.Errorf("new base sinker: %w", err)
	}

	connectionString := dsn.ConnString()
	fmt.Println(connectionString)
	sqlDB, err := sql.Open(dsn.Driver(), connectionString)
	if err != nil {
		return fmt.Errorf("open db connection: %w", err)
	}

	database, err := protosql.NewDatabase(schema, sqlDB, outputModuleName, rootMessageDescriptor, zlog)
	if err != nil {
		return fmt.Errorf("creating database: %w", err)
	}

	sinker := data.NewSinker(zlog, baseSink, database)
	sinker.OnTerminating(func(err error) {
		zlog.Error("sinker terminating", zap.Error(err))
	})

	err = sinker.Run(cmd.Context())
	if err != nil {
		return fmt.Errorf("runnning sinker:%w", err)
	}

	return nil
}
