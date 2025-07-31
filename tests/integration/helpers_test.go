package tests

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/logging"
	db2 "github.com/streamingfast/substreams-sink-sql/db_changes/db"
	"github.com/streamingfast/substreams/client"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/clickhouse"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var logger *zap.Logger
var tracer logging.Tracer

func init() {
	logger, tracer = logging.ApplicationLogger("test", "test")
}

type PostgresSeeder = func(ctx context.Context, user, password, database, schema, dsn string, container *postgres.PostgresContainer) error

// setupRawPostgresContainer spins up a Postgres Docker container and let a seeder function seed the database.
func setupRawPostgresContainer(t *testing.T, schema string, seedDb PostgresSeeder) (dbConnectionString string, container *postgres.PostgresContainer) {
	t.Helper()
	ctx := context.Background()

	dbName := "users"
	dbUser := "user"
	dbPassword := "password"

	postgresContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase(dbName),
		postgres.WithUsername(dbUser),
		postgres.WithPassword(dbPassword),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second)),
	)
	testcontainers.CleanupContainer(t, postgresContainer)
	require.NoError(t, err)

	dbConnectionString, err = postgresContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	_, _, err = postgresContainer.Exec(ctx, []string{"psql", "-U", dbUser, "-d", dbName, "-c", fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schema)})
	require.NoError(t, err)

	fmt.Println("Postgres container started with connection string:", dbConnectionString)
	require.NoError(t, seedDb(ctx, dbUser, dbPassword, dbName, schema, dbConnectionString, postgresContainer))

	err = postgresContainer.Snapshot(ctx)
	require.NoError(t, err)

	return dbConnectionString, postgresContainer
}

const dbChangesSchemaName = "testschema"

// setupDbChangesPostgresContainer spins up a Postgres Docker container and initialize the database with the corresponding
// testTables. If the testTablesSQL is `nil`, it will generate the SQL from the testTables directly, otherwise
// it will use the provided SQL to set up the tables.
func setupDbChangesPostgresContainer(t *testing.T, testTables map[string]*db2.TableInfo, testTablesSQL *string) (dbConnectionString string, container *postgres.PostgresContainer) {
	t.Helper()

	dbConnectionString, container = setupRawPostgresContainer(t, dbChangesSchemaName, func(ctx context.Context, user, password, database, schema, dsn string, container *postgres.PostgresContainer) error {
		l := db2.NewTestLoader(
			t,
			dsn+"&schemaName="+schema,
			nil,
			testTables,
			logger,
			tracer,
		)

		if testTablesSQL == nil {
			testTablesSQL = ptr(db2.GenerateCreateTableSQL(testTables))
		}

		require.NoError(t, l.Setup(context.Background(), schema, *testTablesSQL, false))
		require.NoError(t, l.Close())

		return nil
	})

	return dbConnectionString + "&schemaName=" + dbChangesSchemaName, container
}

type ClickhouseSeeder = func(ctx context.Context, user, password, database, dsn string, container *clickhouse.ClickHouseContainer) error

// setupClickhouseContainer spins up a ClickHouse Docker container and let a seeder function seed the database.
func setupClickhouseContainer(t *testing.T, seedDb ClickhouseSeeder) (dbConnectionString string, container *clickhouse.ClickHouseContainer) {
	t.Helper()

	start := time.Now()
	defer func() { logger.Debug("setupClickhouseContainer duration", zap.Duration("duration", time.Since(start))) }()

	ctx := context.Background()

	dbName := "test_schema"
	dbUser := "default"
	dbPassword := "clickhouse"

	clickhouseContainer, err := clickhouse.Run(ctx,
		"clickhouse/clickhouse-server:24.3-alpine",
		clickhouse.WithDatabase(dbName),
		clickhouse.WithUsername(dbUser),
		clickhouse.WithPassword(dbPassword),
	)
	require.NoError(t, err)

	dbConnectionString, err = clickhouseContainer.ConnectionString(ctx)
	require.NoError(t, err)

	t.Cleanup(func() {
		if os.Getenv("DEBUG_CLICKHOUSE_TEST") != "" {
			containerName, err := clickhouseContainer.Name(ctx)
			require.NoError(t, err)

			fmt.Println()
			fmt.Println("ClickHouse container started with connection string:", dbConnectionString)
			fmt.Println("You can connect to it using:")
			fmt.Printf("  docker exec -it %s clickhouse-client -u %s\n", containerName, dbUser)

			timeout, err := time.ParseDuration(os.Getenv("DEBUG_CLICKHOUSE_TEST"))
			require.NoError(t, err)
			fmt.Println("Waiting for", timeout, "before cleaning up container...")

			time.Sleep(timeout)
		}

		testcontainers.TerminateContainer(clickhouseContainer, testcontainers.StopTimeout(0*time.Second))
	})

	require.NoError(t, seedDb(ctx, dbUser, dbPassword, dbName, dbConnectionString, clickhouseContainer))

	return dbConnectionString, clickhouseContainer
}

// setupFakeSubstreamsServer creates a fake gRPC server with custom message buckets.
// If messages is nil, uses default messages.
func setupFakeSubstreamsServer(t *testing.T, messages ...*pbsubstreamsrpc.Response) *client.SubstreamsClientConfig {
	t.Helper()

	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	server := grpc.NewServer()
	pbsubstreamsrpc.RegisterStreamServer(server, NewFakeStreamServer(messages))

	go func() {
		if err := server.Serve(listener); err != nil {
			t.Logf("gRPC server error: %v", err)
		}
	}()

	t.Cleanup(server.Stop)

	return client.NewSubstreamsClientConfig(listener.Addr().String(), "", client.None, false, true, "sink-test")
}

// substreamsTestPackage creates a test package with the given output module name and file descriptor.
//
// File descriptor can usually be obtained from a generate Golang proto file and the field to look for
// look like:
//
//	pbrelations.File_test_relations_relations_proto
func substreamsTestPackage(fileDescriptor protoreflect.FileDescriptor, outputDesc protoreflect.MessageDescriptor) *pbsubstreams.Package {
	// Create dummy base sink (we won't actually use it for streaming)
	fileDescriptorPb := protodesc.ToFileDescriptorProto(fileDescriptor)
	outputType := string(outputDesc.FullName())

	return &pbsubstreams.Package{
		ProtoFiles: []*descriptorpb.FileDescriptorProto{
			fileDescriptorPb,
		},
		Modules: &pbsubstreams.Modules{
			Modules: []*pbsubstreams.Module{
				{
					Name: defaultOutputModuleName,
					Output: &pbsubstreams.Module_Output{
						Type: outputType,
					},
					Kind: &pbsubstreams.Module_KindMap_{
						KindMap: &pbsubstreams.Module_KindMap{
							OutputType: outputType,
						},
					}},
			},
		},
	}
}

func blockScopedData(t *testing.T, blockIdentifier string, output proto.Message, extraArgs ...any) *pbsubstreamsrpc.Response {
	t.Helper()

	blockNum, blockId := expandBlockIdentifier(blockIdentifier)
	blockTime := timestamppb.New(fixedBaseTime.Add(time.Duration(blockNum * uint64(time.Minute))))

	outputData, err := anypb.New(output)
	require.NoError(t, err)

	currentRef := bstream.NewBlockRef(blockId, blockNum)
	finalRef := bstream.BlockRefEmpty

	finalBlockHeight := uint64(0)

	for _, arg := range extraArgs {
		switch v := arg.(type) {
		case finalBlock:
			finalBlockNum, finalBlockId := expandBlockIdentifier(string(v))
			finalRef = bstream.NewBlockRef(finalBlockId, finalBlockNum)

		case *timestamppb.Timestamp:
			blockTime = v
		}
	}

	cursor := bstream.Cursor{Step: bstream.StepNew, Block: currentRef, HeadBlock: currentRef, LIB: finalRef}
	if currentRef.ID() == finalRef.ID() {
		cursor.Step = bstream.StepNewIrreversible
	}

	return &pbsubstreamsrpc.Response{
		Message: &pbsubstreamsrpc.Response_BlockScopedData{
			BlockScopedData: &pbsubstreamsrpc.BlockScopedData{
				Cursor: cursor.ToOpaque(),
				Clock: &pbsubstreams.Clock{
					Id:        blockId,
					Number:    blockNum,
					Timestamp: blockTime,
				},
				Output: &pbsubstreamsrpc.MapModuleOutput{
					Name:      defaultOutputModuleName,
					MapOutput: outputData,
				},
				FinalBlockHeight: finalBlockHeight,
			},
		},
	}
}

// finalBlock can be used in [blockScopedData] to pass final block for the response.
type finalBlock string

var fixedBaseTime = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

// blockTime can be used in [blockScopedData] to specify the block time for the response.
func blockTime(t *testing.T, in string) *timestamppb.Timestamp {
	t.Helper()

	if in == "now" {
		return timestamppb.Now()
	}

	if parsedTime, err := time.Parse(time.RFC3339, in); err == nil {
		return timestamppb.New(parsedTime)
	}

	if parsedTime, err := time.Parse("2006-01-02 15:04:05", in); err == nil {
		return timestamppb.New(parsedTime)
	}

	if parsedTime, err := time.Parse("2006-01-02", in); err == nil {
		return timestamppb.New(parsedTime)
	}

	require.Fail(t, "invalid block time format", "expected RFC3339, <2006-01-02 15:04:05> or <2006-01-02> formats, got %q", in)
	return nil // This line will never be reached due to require.Fail
}

func expandBlockIdentifier(in string) (blockNum uint64, blockId string) {
	blockId = in
	if in == "" {
		return 0, "0"
	}

	i := 0
	for i < len(in) && in[i] >= '0' && in[i] <= '9' {
		i++
	}

	if i > 0 {
		// Parse the numeric part
		if num, err := strconv.ParseUint(in[:i], 10, 64); err == nil {
			blockNum = num
		}
	}

	return
}
