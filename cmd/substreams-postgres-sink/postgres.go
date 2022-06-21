package main

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/jimsmart/schema"
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
	RunE:         runLoadPostgres,
	Args:         cobra.ExactArgs(2),
	SilenceUsage: true,
}

func init() {
	loadPostgresCmd.Flags().Int64P("start-block", "s", -1, "Start block for blockchain firehose")
	loadPostgresCmd.Flags().Uint64P("stop-block", "t", 0, "Stop block for blockchain firehose")

	loadPostgresCmd.Flags().StringP("firehose-endpoint", "e", "bsc-dev.streamingfast.io:443", "firehose GRPC endpoint")
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

func runLoadPostgres(cmd *cobra.Command, args []string) error {
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
		mustGetString(cmd, "firehose-endpoint"),
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
		_, tableExists := loader.tableRegistry[fmt.Sprintf("%s.%s", loader.schema, change.Table)]
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

type PostgresLoader struct {
	db *sql.DB

	schema string

	tableRegistry    map[string]map[string]reflect.Type
	tablePrimaryKeys map[string]string
}

func NewPostgresLoader(host, port, username, password, dbname, schemaName string, sslEnabled bool) (*PostgresLoader, error) {
	var sslmode string
	if sslEnabled {
		sslmode = "enable"
	} else {
		sslmode = "disable"
	}

	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s", host, port, username, password, dbname, sslmode))
	if err != nil {
		return nil, err
	}

	tables, err := schema.Tables(db)
	if err != nil {
		return nil, err
	}

	tableRegistry := map[string]map[string]reflect.Type{}
	primaryKeys := map[string]string{}
	for k, t := range tables {
		if k[0] != schemaName {
			continue
		}

		schemaTable := fmt.Sprintf("%s.%s", k[0], k[1])

		m := map[string]reflect.Type{}
		for _, f := range t {
			m[f.Name()] = f.ScanType()
		}
		tableRegistry[schemaTable] = m

		key, err := schema.PrimaryKey(db, k[0], k[1])
		if err != nil {
			return nil, err
		}
		if len(key) > 0 {
			primaryKeys[schemaTable] = key[0]
		} else {
			primaryKeys[schemaTable] = "id"
		}
	}

	return &PostgresLoader{
		db:               db,
		tableRegistry:    tableRegistry,
		tablePrimaryKeys: primaryKeys,
		schema:           schemaName,
	}, err
}

func (pgm *PostgresLoader) Insert(schemaName, table string, key string, data map[string]string) error {
	var keys []string
	var values []string
	for k, v := range data {
		keys = append(keys, k)
		value, err := pgm.value(schemaName, table, k, v)
		if err != nil {
			return fmt.Errorf("getting sql value for column %s raw value %s: %w", k, v, err)
		}
		values = append(values, value)
	}

	keysString := strings.Join(keys, ",")
	valuesString := strings.Join(values, ",")

	query := fmt.Sprintf("insert into %s.%s (%s) values (%s)", schemaName, table, keysString, valuesString)
	return pgm.exec(query)
}

func (pgm *PostgresLoader) Update(schemaName, table string, key string, data map[string]string) error {
	pk, ok := pgm.tablePrimaryKeys[fmt.Sprintf("%s.%s", schemaName, table)]
	if !ok {
		pk = "id"
	}

	var keys []string
	var values []string
	for k, v := range data {
		keys = append(keys, k)
		value, err := pgm.value(schemaName, table, k, v)
		if err != nil {
			return fmt.Errorf("getting sql value for column %s raw value %s: %w", k, v, err)
		}
		values = append(values, value)
	}

	var updates []string
	for i := 0; i < len(keys); i++ {
		update := fmt.Sprintf("%s=%s", keys[i], values[i])
		updates = append(updates, update)
	}

	updatesString := strings.Join(updates, ", ")

	query := fmt.Sprintf("update %s.%s set %s where %s = %s", schemaName, table, updatesString, pk, key)
	return pgm.exec(query)
}

func (pgm *PostgresLoader) Delete(schemaName, table string, key string) error {
	pk, ok := pgm.tablePrimaryKeys[fmt.Sprintf("%s.%s", schemaName, table)]
	if !ok {
		pk = "id"
	}

	query := fmt.Sprintf("delete from %s.%s where %s = %s", schemaName, table, pk, key)
	return pgm.exec(query)
}

func (pgm *PostgresLoader) exec(query string) error {
	_, err := pgm.db.Exec(query)
	if err != nil {
		return fmt.Errorf("executing query:\n`%s`\n err: %w", query, err)
	}

	return nil
}

func (pgm *PostgresLoader) value(schema, table, column, value string) (string, error) {
	valType, ok := pgm.tableRegistry[fmt.Sprintf("%s.%s", schema, table)][column]
	if !ok {
		return "", fmt.Errorf("could not find column %s in table %s.%s", column, schema, table)
	}

	switch valType.Kind() {
	case reflect.String:
		return fmt.Sprintf("'%s'", value), nil
	case reflect.Struct: //time?
		i, err := strconv.Atoi(value)
		if err != nil {
			return value, nil
		}

		v := time.Unix(int64(i), 0).Format("2006-01-02 15:04:05")
		return fmt.Sprintf("'%s'", v), nil
	default:
		return value, nil
	}
}
