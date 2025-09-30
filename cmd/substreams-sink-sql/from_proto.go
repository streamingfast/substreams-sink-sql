package main

import (
	"fmt"

	"github.com/jhump/protoreflect/desc"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/streamingfast/cli"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
	sink "github.com/streamingfast/substreams-sink"
	sinksql "github.com/streamingfast/substreams-sink-sql"
	"github.com/streamingfast/substreams-sink-sql/bytes"
	"github.com/streamingfast/substreams-sink-sql/db_changes/db"
	"github.com/streamingfast/substreams-sink-sql/db_proto"
	"github.com/streamingfast/substreams-sink-sql/db_proto/proto"
	pbsql "github.com/streamingfast/substreams-sink-sql/pb/sf/substreams/sink/sql/services/v1"
	"github.com/streamingfast/substreams-sink-sql/services"
	"github.com/streamingfast/substreams/manifest"
	"google.golang.org/protobuf/types/descriptorpb"
)

var fromProtoCmd = Command(fromProtoE,
	"from-proto <dsn> <manifest> [output-module]",
	"",
	RangeArgs(2, 3),
	Flags(func(flags *pflag.FlagSet) {
		sink.AddFlagsToSet(flags, ignoreUndoBufferSize{})
		flags.StringP("substreams-endpoint", "e", "", "Substreams gRPC endpoint. If empty, will be replaced by the SUBSTREAMS_ENDPOINT_{network_name} environment variable, where `network_name` is determined from the substreams manifest. Some network names have default endpoints.")
		flags.StringP("start-block", "s", "", "Start block to stream from. If empty, will be replaced by initialBlock of the first module you are streaming. If negative, will be resolved by the server relative to the chain head")
		flags.StringP("stop-block", "t", "0", "Stop block to end stream at, exclusively. If the start-block is positive, a '+' prefix can indicate 'relative to start-block'")

		flags.Bool("no-constraints", false, "Do not add any constraints to the database. This is useful to speed up the initial import of a large dataset.")
		//flags.Bool("no-proto-option", false, "this tell the schema manager to not rely on proto option to generate the schema.")
		//flags.Bool("no-transactions", false, "Do not use transactions when inserting data. This is useful to speed up the initial import of a large dataset.")
		//flags.Bool("parallel", false, "Run the sinker in parallel mode. This is useful to speed up the initial import of a large dataset. This is will process blocks of a batch in parallel")
		flags.Int("block-batch-size", 25, "number of blocks to process at a time")
		flags.String("clickhouse-sink-info-folder", "", "folder where to store the clickhouse sink info")
		flags.String("clickhouse-cursor-file-path", "cursor.txt", "file name where to store the clickhouse cursor")
		flags.String("bytes-encoding", "raw", "Encoding for protobuf bytes fields (raw, hex, 0xhex, base64, base58)")
	}),
)

//now
//todo: add a validator on top of schema to validate all the relations

// Later
//todo: migration tool
//todo: add index support
//todo: post generate index
//todo: external process
//todo: handle network

func fromProtoE(cmd *cobra.Command, args []string) error {
	app := cli.NewApplication(cmd.Context())

	dsnString := args[0]
	manifestPath := args[1]

	outputModuleName := sink.InferOutputModuleFromPackage
	if len(args) == 3 {
		outputModuleName = args[2]
	}

	useConstraints := !sflags.MustGetBool(cmd, "no-constraints")
	blockBatchSize := sflags.MustGetInt(cmd, "block-batch-size")

	encodingStr := sflags.MustGetString(cmd, "bytes-encoding")
	encoding, err := bytes.ParseEncoding(encodingStr)
	if err != nil {
		return fmt.Errorf("invalid bytes encoding %q: %w", encodingStr, err)
	}

	useTransactions := true
	parallel := false

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

	//todo: handle params
	spkg, module, _, _, err := sink.ReadManifestAndModuleAndBlockRange(manifestPath, "", nil, outputModuleName, "", false, "", zlog)
	if err != nil {
		return fmt.Errorf("reading manifest: %w", err)
	}

	outputModuleName = module.Name
	outputType := proto.ModuleOutputType(spkg, outputModuleName)
	if outputType == "" {
		return fmt.Errorf("could not find output type for module %s", outputModuleName)
	}

	service, err := sinksql.ExtractSinkService(spkg)
	if err != nil {
		service = &pbsql.Service{}
	}

	err = services.Run(service, zlog)
	if err != nil {
		return fmt.Errorf("running service: %w", err)
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

	useProtoOption := false
	for _, descriptor := range fileDescriptor.GetDependencies() {
		if descriptor.GetName() == "sf/substreams/sink/sql/schema/v1/schema.proto" {
			useProtoOption = true
		}
	}
	if !useProtoOption {
		useConstraints = false
	}

	var rootMessageDescriptor *desc.MessageDescriptor
	for _, messageDescriptor := range fileDescriptor.GetMessageTypes() {
		name := messageDescriptor.GetFullyQualifiedName()
		if name == outputType {
			rootMessageDescriptor = messageDescriptor
			break
		}
	}
	if rootMessageDescriptor == nil {
		return fmt.Errorf("message descriptor not found for output type %q. Your substreams need to bundle its protobuf definitions", outputType)
	}

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

	factory := db_proto.SinkerFactory(baseSink, outputModuleName, rootMessageDescriptor.UnwrapMessage(), db_proto.SinkerFactoryOptions{
		UseProtoOption:  useProtoOption,
		UseConstraints:  useConstraints,
		UseTransactions: useTransactions,
		BlockBatchSize:  blockBatchSize,
		Parallel:        parallel,
		Encoding:        encoding,
		Clickhouse: db_proto.SinkerFactoryClickhouse{
			SinkInfoFolder: sflags.MustGetString(cmd, "clickhouse-sink-info-folder"),
			CursorFilePath: sflags.MustGetString(cmd, "clickhouse-cursor-file-path"),
		},
	})

	sinker, err := factory(cmd.Context(), dsnString, dsn.Schema(), zlog, tracer)
	if err != nil {
		return fmt.Errorf("creating sinker: %w", err)
	}

	app.SuperviseAndStartUsing(sinker, sinker.Run)

	if err := app.WaitForTermination(zlog, 0, 0); err != nil {
		cli.Quit("application terminated with error: %s", err)
	}

	return nil
}
