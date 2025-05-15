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
	"github.com/streamingfast/substreams-sink-sql/db_proto"
	"github.com/streamingfast/substreams-sink-sql/db_proto/proto"
	protosql "github.com/streamingfast/substreams-sink-sql/db_proto/sql"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/postgres"
	schema2 "github.com/streamingfast/substreams-sink-sql/db_proto/sql/schema"
	stats2 "github.com/streamingfast/substreams-sink-sql/db_proto/stats"
	"github.com/streamingfast/substreams-sink-sql/services"
	"github.com/streamingfast/substreams/manifest"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/descriptorpb"
)

var fromProtoCmd = Command(fromProtoE,
	"from-proto <dsn> <manifest>",
	"",
	ExactArgs(2),
	Flags(func(flags *pflag.FlagSet) {
		sink.AddFlagsToSet(flags, ignoreUndoBufferSize{})
		flags.StringP("substreams-endpoint", "e", "", "Substreams gRPC endpoint. If empty, will be replaced by the SUBSTREAMS_ENDPOINT_{network_name} environment variable, where `network_name` is determined from the substreams manifest. Some network names have default endpoints.")
		flags.StringP("start-block", "s", "", "Start block to stream from. If empty, will be replaced by initialBlock of the first module you are streaming. If negative, will be resolved by the server relative to the chain head")
		flags.StringP("stop-block", "t", "0", "Stop block to end stream at, exclusively. If the start-block is positive, a '+' prefix can indicate 'relative to start-block'")

		flags.Bool("no-constraints", false, "Do not add any constraints to the database. This is useful to speed up the initial import of a large dataset.")
		flags.Bool("no-proto-option", false, "this tell the schema manager to not rely on proto option to generate the schema.")
		//flags.Bool("no-transactions", false, "Do not use transactions when inserting data. This is useful to speed up the initial import of a large dataset.")
		//flags.Bool("parallel", false, "Run the sinker in parallel mode. This is useful to speed up the initial import of a large dataset. This is will process blocks of a batch in parallel")
		flags.Int("block-batch-size", 25, "number of blocks to process at a time")
		//flags.String("clickhouse-sink-info-folder", "", "folder where to store the clickhouse sink info")
		//flags.String("clickhouse-cursor-file-path", "cursor.txt", "file name where to store the clickhouse cursor")
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
//todo: fix DSN for clickhouse

func fromProtoE(cmd *cobra.Command, args []string) error {
	//app := NewApplication(cmd.Context())

	dsnString := args[0]
	manifestPath := args[1]

	useConstraints := !sflags.MustGetBool(cmd, "no-constraints")
	//useTransactions := !sflags.MustGetBool(cmd, "no-transactions")
	blockBatchSize := sflags.MustGetInt(cmd, "block-batch-size")
	useProtoOption := !sflags.MustGetBool(cmd, "no-proto-option")

	if !useProtoOption {
		useConstraints = false
	}

	useTransactions := true
	parallel := false

	//parallel := sflags.MustGetBool(cmd, "parallel")
	//if parallel {
	//	useConstraints = false
	//	useTransactions = false
	//}

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
	spkg, module, _, _, err := sink.ReadManifestAndModuleAndBlockRange(manifestPath, "", nil, sink.InferOutputModuleFromPackage, "", false, "", zlog)
	if err != nil {
		return fmt.Errorf("reading manifest: %w", err)
	}

	outputModuleName := module.Name
	outputType := proto.ModuleOutputType(spkg, outputModuleName)
	if outputType == "" {
		return fmt.Errorf("could not find output type for module %s", outputModuleName)
	}

	service, err := extractSinkService(spkg)
	if err != nil {
		return fmt.Errorf("extracting sink service: %w", err)
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

	var rootMessageDescriptor *desc.MessageDescriptor
	for _, messageDescriptor := range fileDescriptor.GetMessageTypes() {
		name := messageDescriptor.GetFullyQualifiedName()
		if name == outputType {
			rootMessageDescriptor = messageDescriptor
			break
		}
	}

	//todo: fix me
	//schemaName := "test"
	schemaName := dsn.Schema()

	schema, err := schema2.NewSchema(schemaName, rootMessageDescriptor, useProtoOption, zlog)
	if err != nil {
		return fmt.Errorf("creating schema: %w", err)
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

	connectionString := dsn.ConnString()
	//todo: fix me
	//connectionString = "http://localhost:8123?secure=false"

	fmt.Println("connection string", connectionString)
	sqlDB, err := sql.Open(dsn.Driver(), connectionString)
	if err != nil {
		return fmt.Errorf("open db connection: %w", err)
	}

	dialect, err := postgres.NewDialectPostgres(schema.Name, schema.TableRegistry, zlog)
	//dialect, err := clickhouse.NewDialectClickHouse(schema.Name, schema.TableRegistry, zlog)
	if err != nil {
		return fmt.Errorf("creating dialect: %w", err)
	}

	var database protosql.Database
	//implDatabase, err := clickhouse.NewDatabase(
	//	schemaName,
	//	dialect,
	//	sqlDB,
	//	outputModuleName,
	//	rootMessageDescriptor,
	//	sflags.MustGetString(cmd, "clickhouse-sink-info-folder"),
	//	sflags.MustGetString(cmd, "clickhouse-cursor-file-path"),
	//	true,
	//	zlog,
	//)
	implDatabase, err := postgres.NewDatabase(schemaName, dialect, sqlDB, outputModuleName, rootMessageDescriptor, useProtoOption, zlog)
	if err != nil {
		return fmt.Errorf("creating database: %w", err)
	}
	database = implDatabase

	sinkInfo, err := database.FetchSinkInfo(schema.Name)
	if err != nil {
		return fmt.Errorf("fetching sink info: %w", err)
	}

	if sinkInfo == nil {
		err := database.BeginTransaction()
		if err != nil {
			return fmt.Errorf("begin transaction: %w", err)
		}
		err = database.CreateDatabase(useConstraints, schemaName)
		if err != nil {
			database.RollbackTransaction()
			return fmt.Errorf("creating database: %w", err)
		}

		err = database.StoreSinkInfo(schemaName, dialect.SchemaHash())
		if err != nil {
			database.RollbackTransaction()
			return fmt.Errorf("storing sink info: %w", err)
		}

		err = database.CommitTransaction()

	} else {
		migrationNeeded := sinkInfo.SchemaHash != dialect.SchemaHash()
		if migrationNeeded {

			tempSchemaName := schema.Name + "_" + dialect.SchemaHash()
			tempSinkInfo, err := database.FetchSinkInfo(tempSchemaName)
			if err != nil {
				return fmt.Errorf("fetching temp schema sink info: %w", err)
			}
			if tempSinkInfo != nil {
				hash, err := database.DatabaseHash(schema.Name)
				if err != nil {
					return fmt.Errorf("fetching schema %q hash: %w", schema.Name, err)
				}
				dbTempHash, err := database.DatabaseHash(tempSchemaName)
				if err != nil {
					return fmt.Errorf("fetching temp schema %q hash: %w", tempSchemaName, err)
				}

				if hash != dbTempHash {
					return fmt.Errorf("schema %s and temp schema %s have different hash", schema.Name, tempSchemaName)
				}
				err = database.BeginTransaction()
				if err != nil {
					return fmt.Errorf("begin transaction: %w", err)
				}
				err = database.UpdateSinkInfoHash(schemaName, tempSinkInfo.SchemaHash)
				if err != nil {
					database.RollbackTransaction()
					return fmt.Errorf("updating sink info hash: %w", err)
				}

				err = database.CommitTransaction()
				if err != nil {
					return fmt.Errorf("commit transaction: %w", err)
				}

			} else {
				//todo: create the temp schema ... and exit

				//err = schema.ChangeName(tempSchemaName, dialect)
				//if err != nil {
				//	return nil, fmt.Errorf("changing schema name: %w", err)
				//}
				//generateTempSchema = true
			}
		}
	}

	//inserter, err := clickhouse.NewAccumulatorInserter(implDatabase, zlog)

	var inserter protosql.Inserter
	if useConstraints {
		inserter, err = postgres.NewRowInserter(implDatabase, zlog)
		if err != nil {
			return fmt.Errorf("creating row inserter: %w", err)
		}
	} else {
		inserter, err = postgres.NewAccumulatorInserter(implDatabase, zlog)
		if err != nil {
			return fmt.Errorf("creating accumulator inserter: %w", err)
		}
	}

	database.SetInserter(inserter)

	stats := stats2.NewStats(zlog)
	sinker := db_proto.NewSinker(rootMessageDescriptor, baseSink, database, useTransactions, useConstraints, blockBatchSize, parallel, stats, zlog)
	sinker.OnTerminating(func(err error) {
		zlog.Error("sinker terminating", zap.Error(err))
	})

	err = sinker.Run(cmd.Context())
	if err != nil {
		return fmt.Errorf("runnning sinker:%w", err)
	}

	stats.Log()
	fmt.Println("Goodbye")

	return nil
}
