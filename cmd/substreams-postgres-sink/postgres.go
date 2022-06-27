package main

import (
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
	"github.com/streamingfast/bstream"
	database "github.com/streamingfast/substreams-postgres-sink/db"
	"github.com/streamingfast/substreams/client"
	"github.com/streamingfast/substreams/manifest"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"

	_ "github.com/lib/pq"
	_ "github.com/streamingfast/sf-ethereum/types"
	"google.golang.org/protobuf/proto"
)

var loadPostgresCmd = &cobra.Command{
	Use:          "load [manifest]",
	RunE:         runLoadPostgresE,
	Args:         cobra.ExactArgs(2),
	SilenceUsage: true,
}

func init() {
	loadPostgresCmd.Flags().Int64P("start-block", "s", -1, "Start block for blockchain firehose")
	loadPostgresCmd.Flags().Uint64P("stop-block", "t", 0, "Stop block for blockchain firehose")

	loadPostgresCmd.Flags().StringP("endpoint", "e", "bsc-dev.streamingfast.io:443", "firehose GRPC endpoint")
	loadPostgresCmd.Flags().String("substreams-api-key-envvar", "FIREHOSE_API_TOKEN", "name of variable containing firehose authentication token (JWT)")
	loadPostgresCmd.Flags().BoolP("insecure", "k", false, "Skip certificate validation on GRPC connection")
	loadPostgresCmd.Flags().BoolP("plaintext", "p", false, "Establish GRPC connection in plaintext")

	loadPostgresCmd.Flags().String("postgres-host", "localhost", "Set database hostname")
	loadPostgresCmd.Flags().String("postgres-port", "5432", "Set database port")
	loadPostgresCmd.Flags().String("postgres-username", "admin", "Set database username")
	loadPostgresCmd.Flags().String("postgres-password", "admin", "Set database password")
	loadPostgresCmd.Flags().Bool("postgres-ssl-enabled", false, "Set database ssl mode")
	loadPostgresCmd.Flags().String("postgres-name", "substreams", "Set database name")
	loadPostgresCmd.Flags().String("postgres-schema", "pancakeswap", "Mongo database schema for unmarshalling data")

	rootCmd.AddCommand(loadPostgresCmd)
}

func runLoadPostgresE(cmd *cobra.Command, args []string) error {
	err := bstream.ValidateRegistry()
	if err != nil {
		return fmt.Errorf("bstream validate registry %w", err)
	}

	ctx := cmd.Context()

	manifestPath := args[0]
	manifestReader := manifest.NewReader(manifestPath)
	pkg, err := manifestReader.Read()
	if err != nil {
		return fmt.Errorf("read manifest %q: %w", manifestPath, err)
	}

	moduleName := args[1]
	databaseSchema := mustGetString(cmd, "postgres-schema")

	loader, err := NewPostgresLoader(
		mustGetString(cmd, "postgres-host"),
		mustGetString(cmd, "postgres-port"),
		mustGetString(cmd, "postgres-username"),
		mustGetString(cmd, "postgres-password"),
		mustGetString(cmd, "postgres-name"),
		mustGetString(cmd, "postgres-schema"),
		mustGetBool(cmd, "postgres-ssl-enabled"),
	)
	if err != nil {
		return fmt.Errorf("creating postgres loader: %w", err)
	}

	ssClient, callOpts, err := client.NewSubstreamsClient(
		mustGetString(cmd, "endpoint"),
		os.Getenv(mustGetString(cmd, "substreams-api-key-envvar")),
		mustGetBool(cmd, "insecure"),
		mustGetBool(cmd, "plaintext"),
	)

	if err != nil {
		return fmt.Errorf("substreams client setup: %w", err)
	}

	req := &pbsubstreams.Request{
		StartBlockNum: mustGetInt64(cmd, "start-block"),
		StopBlockNum:  mustGetUint64(cmd, "stop-block"),
		ForkSteps:     []pbsubstreams.ForkStep{pbsubstreams.ForkStep_STEP_IRREVERSIBLE},
		Modules:       pkg.Modules,
		OutputModules: []string{moduleName},
	}

	stream, err := ssClient.Blocks(ctx, req, callOpts...)
	if err != nil {
		return fmt.Errorf("call sf.substreams.v1.Stream/Blocks: %w", err)
	}

	for {
		resp, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("receiving from stream: %w", err)
		}

		switch r := resp.Message.(type) {
		case *pbsubstreams.Response_Progress:
			p := r.Progress
			for _, module := range p.Modules {
				fmt.Println("progress:", module.Name, module.GetProcessedRanges())
			}
		case *pbsubstreams.Response_SnapshotData:
			_ = r.SnapshotData
		case *pbsubstreams.Response_SnapshotComplete:
			_ = r.SnapshotComplete
		case *pbsubstreams.Response_Data:
			for _, output := range r.Data.Outputs {
				for _, log := range output.Logs {
					fmt.Println("LOG: ", log)
				}
				if output.Name == "db_out" {
					databaseChanges := &database.DatabaseChanges{}
					err := proto.Unmarshal(output.GetMapOutput().GetValue(), databaseChanges)
					if err != nil {
						return fmt.Errorf("unmarshalling database changes: %w", err)
					}
					err = applyDatabaseChanges(loader, databaseChanges, databaseSchema)
					if err != nil {
						return fmt.Errorf("applying database changes: %w", err)
					}
				}
			}
		}
	}
}

func applyDatabaseChanges(loader *PostgresLoader, databaseChanges *database.DatabaseChanges, schemaName string) error {
	for _, change := range databaseChanges.TableChanges {
		_, tableExists := loader.tableRegistry[[2]string{loader.schema, change.Table}]
		if !tableExists {
			continue
		}

		id := change.Pk
		changes := map[string]string{}
		for _, field := range change.Fields {
			changes[field.Name] = field.NewValue
		}

		switch change.Operation {
		case database.TableChange_CREATE: //insert
			err := loader.Insert(schemaName, change.Table, id, changes)
			if err != nil {
				return fmt.Errorf("loader insert: %w", err)
			}
		case database.TableChange_UPDATE: //update
			err := loader.Update(schemaName, change.Table, id, changes)
			if err != nil {
				return fmt.Errorf("loader update: %w", err)
			}
		case database.TableChange_UNSET: //ignore
		case database.TableChange_DELETE: //delete
			err := loader.Delete(schemaName, change.Table, id)
			if err != nil {
				return fmt.Errorf("loader delete: %w", err)
			}
		default: //ignore
		}

	}

	return nil
}
