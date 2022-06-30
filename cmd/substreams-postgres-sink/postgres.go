package main

import (
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	// FIXME: use `pb/substreams/databases/deltas/v1/models.pb.go` instead, and move that `database.go` helpers over there.
	database "github.com/streamingfast/substreams-postgres-sink/db"
	"github.com/streamingfast/substreams/client"
	"github.com/streamingfast/substreams/manifest"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"

	// FIXME: ideally, we wouldn't rely on the `streamingfast/substreams` package, if only to show
	// that all those sinks can be written in isolation from our stack.

	_ "github.com/lib/pq"
	"google.golang.org/protobuf/proto"
)

var rootCmd = &cobra.Command{
	Use:          "substreams-postgres-sink <substreams_package> <output_module>",
	RunE:         runLoadPostgresE,
	Args:         cobra.ExactArgs(2),
	SilenceUsage: true,
}

func init() {
	rootCmd.Flags().Int64P("start-block", "s", -1, "Start block to stream from.")
	rootCmd.Flags().Uint64P("stop-block", "t", 0, "Stop block to end stream at, inclusively.")

	rootCmd.Flags().StringP("endpoint", "e", "bsc-dev.streamingfast.io:443", "firehose GRPC endpoint")
	rootCmd.Flags().String("substreams-api-token-envvar", "SUBSTREAMS_API_TOKEN", "name of variable containing the Substreams authentication token")
	rootCmd.Flags().BoolP("insecure", "k", false, "Skip certificate validation on GRPC connection")
	rootCmd.Flags().BoolP("plaintext", "p", false, "Establish GRPC connection in plaintext")

	// DISCUSSION: why wouldn't we have people pass that connection
	// string directly, a single one, "host=abc port=234 user=abc
	// sslmode=disable" it's much more annoying to pass a dozen flags,
	// and overriding anything would simply mean overiding one param.
	// We could have var interpolation like ${} to pick it up from
	// environment, so it doesn't show the password.
	// Waddayathink?
	rootCmd.Flags().String("postgres-host", "localhost", "Set database hostname")
	rootCmd.Flags().String("postgres-port", "5432", "Set database port")
	rootCmd.Flags().String("postgres-username", "admin", "Set database username")
	rootCmd.Flags().String("postgres-password", "admin", "Set database password")
	rootCmd.Flags().Bool("postgres-ssl-enabled", false, "Set database ssl mode")
	rootCmd.Flags().String("postgres-name", "postgres", "Set database name")
	rootCmd.Flags().String("postgres-schema", "postgres", "PostgreSQL database schema")
}

func runLoadPostgresE(cmd *cobra.Command, args []string) error {
	// FIXME: why would we do that?! we're running a remote service, why validate a registry? why load
	// the whole `sf-ethereum` also?!
	// err := bstream.ValidateRegistry()
	// if err != nil {
	// 	return fmt.Errorf("bstream validate registry %w", err)
	// }

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
		os.Getenv(mustGetString(cmd, "substreams-api-token-envvar")),
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

				if output.Name != moduleName {
					continue
				}

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
		case database.TableChange_CREATE:
			err := loader.Insert(schemaName, change.Table, id, changes)
			if err != nil {
				return fmt.Errorf("loader insert: %w", err)
			}
		case database.TableChange_UPDATE:
			err := loader.Update(schemaName, change.Table, id, changes)
			if err != nil {
				return fmt.Errorf("loader update: %w", err)
			}
		case database.TableChange_DELETE:
			err := loader.Delete(schemaName, change.Table, id)
			if err != nil {
				return fmt.Errorf("loader delete: %w", err)
			}
		default:
			//case database.TableChange_UNSET:
		}
	}
	return nil
}
