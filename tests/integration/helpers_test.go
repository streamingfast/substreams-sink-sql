package tests

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/logging"
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

const defaultOutputModuleName = "map_output"

func init() {
	logger, tracer = logging.ApplicationLogger("test", "test")
}

type PostgresSeeder = func(ctx context.Context, user, password, database, schema, dsn string, container *postgres.PostgresContainer) error

type PostgresContainerConfig struct {
	Image string
}

// setupRawPostgresContainer spins up a Postgres Docker container and let a seeder function seed the database.
func setupRawPostgresContainer(config PostgresContainerConfig) (*PostgresContainerExt, func()) {
	ctx := context.Background()

	dbName := "users"
	dbUser := "user"
	dbPassword := "password"

	postgresContainer, err := postgres.Run(ctx,
		config.Image,
		postgres.WithDatabase(dbName),
		postgres.WithUsername(dbUser),
		postgres.WithPassword(dbPassword),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second)),
	)
	if err != nil {
		panic(fmt.Errorf("setting up postgres container: %w", err))
	}

	return &PostgresContainerExt{
			PostgresContainer: postgresContainer,
			Configuration:     &config,
			ConnectionString:  postgresContainer.MustConnectionString(ctx, "sslmode=disable"),
		}, func() {
			_ = testcontainers.TerminateContainer(postgresContainer)
		}
}

type PostgresContainerExt struct {
	*postgres.PostgresContainer
	Configuration *PostgresContainerConfig
	// ConnectionString should be used instead of calling ConnectionString() on the embedded PostgresContainer
	// because this one is properly configured with sslmode=disable.
	ConnectionString string
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

// setupFakeSubstreamsServer creates a new fake stream server using bucket-based iterator pattern.
//
// The pattern is a slice of [any] where:
//   - [*pbsubstreamsrpc.Response]: Send this message to the stream
//   - [error]: Close the stream with this error
//   - nil: End of bucket boundary (start new bucket for next Blocks() call)
//
// For example, if you have a pattern like:
//
//	pattern := []any{block1, block2, errors.New("stream error"), block3, nil}
//
// This will create two buckets:
// - First bucket: sends block1, block2, then closes with error
// - Second bucket: sends block3, then ends normally
func setupFakeSubstreamsServer(t *testing.T, pattern ...any) *client.SubstreamsClientConfig {
	t.Helper()

	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	server := grpc.NewServer()
	pbsubstreamsrpc.RegisterStreamServer(server, newFakeStreamServer(pattern))

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
				FinalBlockHeight: finalRef.Num(),
			},
		},
	}
}

// blockUndo creates a BlockUndoSignal response for tests
func blockUndo(t *testing.T, lastValidBlockIdentifier string, extraArgs ...any) *pbsubstreamsrpc.Response {
	t.Helper()

	blockNum, blockId := expandBlockIdentifier(lastValidBlockIdentifier)
	finalRef := bstream.BlockRefEmpty

	for _, arg := range extraArgs {
		switch v := arg.(type) {
		case finalBlock:
			finalBlockNum, finalBlockId := expandBlockIdentifier(string(v))
			finalRef = bstream.NewBlockRef(finalBlockId, finalBlockNum)
		}
	}

	cursor := bstream.Cursor{Step: bstream.StepUndo, Block: bstream.NewBlockRef(blockId, blockNum), HeadBlock: bstream.NewBlockRef(blockId, blockNum), LIB: finalRef}

	return &pbsubstreamsrpc.Response{
		Message: &pbsubstreamsrpc.Response_BlockUndoSignal{
			BlockUndoSignal: &pbsubstreamsrpc.BlockUndoSignal{
				LastValidBlock:  &pbsubstreams.BlockRef{Id: blockId, Number: blockNum},
				LastValidCursor: cursor.ToOpaque(),
			},
		},
	}
}

// finalBlock can be used in [blockScopedData] to pass final block for the response.
type finalBlock string

var fixedBaseTime = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

// blockTime can be used in [blockScopedData] to specify the block time for the response.
func blockTime(t *testing.T, in string) time.Time {
	return blockTimepb(t, in).AsTime()
}

// blockTimepb can be used in [blockScopedData] to specify the block time for the response.
func blockTimepb(t *testing.T, in string) *timestamppb.Timestamp {
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

type isAlwaysLiveChecker struct{}

func (c *isAlwaysLiveChecker) IsLive(block *pbsubstreams.Clock) bool {
	return true
}

// streamMock is a helper function that creates a stream of responses for testing
func streamMock(responses ...any) []any {
	return responses
}

func readRowsBy[T any](t *testing.T, db *sqlx.DB, tableAndOrSchema, orderBy string) []*T {
	t.Helper()

	tableIdentifier := strings.TrimSpace(tableAndOrSchema)
	if !strings.HasPrefix(tableIdentifier, `"`) {
		tableIdentifier = `"` + tableIdentifier
	}

	if !strings.HasSuffix(tableIdentifier, `"`) {
		tableIdentifier += `"`
	}

	var rows []*T
	err := db.SelectContext(context.Background(), &rows, fmt.Sprintf(`SELECT * FROM %s ORDER BY %s;`, tableIdentifier, orderBy))
	require.NoError(t, err)

	return rows
}

func readDbChangesRows[T any](t *testing.T, db *sqlx.DB, schema string, table string) []*T {
	t.Helper()

	return readRowsBy[T](t, db, fmt.Sprintf(`"%s"."%s"`, schema, table), "id")
}
