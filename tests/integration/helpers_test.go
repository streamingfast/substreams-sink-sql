package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/streamingfast/logging"
	db2 "github.com/streamingfast/substreams-sink-sql/db_changes/db"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap"
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
