package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/dstore"
	sink "github.com/streamingfast/substreams-sink"

	"github.com/streamingfast/substreams-sink-postgres/db"
	"go.uber.org/zap"
)

var injectCSVCmd = Command(injectCSVE,
	"inject-csv <schema> <input-path> <table> <psql-dsn> <start-block> <stop-block>",
	"Injects generated CSV rows for <table> into the database pointed by <psql-dsn> argument.",
	Description(`
			Can be run in parallel for multiple rows up to the same stop-block. 

			Watch out, the start-block must be aligned with the range size of the csv files or the module inital block
	`),
	ExactArgs(6),
	Flags(func(flags *pflag.FlagSet) {
		sink.AddFlagsToSet(flags)
	}),
)

func injectCSVE(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	pgSchema := args[0]

	inputPath := args[1]
	tableName := args[2]

	psqlDSN := args[3]
	startBlock, err := strconv.ParseUint(args[4], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid start block %q: %w", args[5], err)
	}
	stopBlock, err := strconv.ParseUint(args[5], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid stop block %q: %w", args[6], err)
	}

	postgresDSN, err := db.ParseDSN(psqlDSN)
	if err != nil {
		return fmt.Errorf("invalid postgres DSN %q: %w", psqlDSN, err)
	}

	zlog.Info("connecting to input store")
	inputStore, err := dstore.NewStore(inputPath, "", "", false)
	if err != nil {
		return fmt.Errorf("unable to create input store: %w", err)
	}

	pool, err := pgxpool.Connect(ctx, fmt.Sprintf("%s pool_min_conns=%d pool_max_conns=%d", postgresDSN.DSN(), 2, 3))
	if err != nil {
		return fmt.Errorf("connecting to postgres: %w", err)
	}

	t0 := time.Now()

	zlog.Debug("table filler", zap.String("pg_schema", pgSchema), zap.String("table_name", tableName), zap.Uint64("start_block", startBlock), zap.Uint64("stop_block", stopBlock))
	filler := NewTableFiller(pool, pgSchema, tableName, startBlock, stopBlock, inputStore)
	theTableName := tableName
	if err := filler.Run(ctx); err != nil {
		return fmt.Errorf("table filler %q: %w", theTableName, err)
	}
	zlog.Info("table done", zap.Duration("total", time.Since(t0)))
	return nil
}

type TableFiller struct {
	pqSchema string
	tblName  string

	in            dstore.Store
	startBlockNum uint64
	stopBlockNum  uint64
	pool          *pgxpool.Pool
}

func NewTableFiller(pool *pgxpool.Pool, pqSchema, tblName string, startBlockNum, stopBlockNum uint64, inStore dstore.Store) *TableFiller {
	return &TableFiller{
		tblName:       tblName,
		pqSchema:      pqSchema,
		pool:          pool,
		startBlockNum: startBlockNum,
		stopBlockNum:  stopBlockNum,
		in:            inStore,
	}
}

func extractFieldsFromFirstLine(ctx context.Context, filename string, store dstore.Store) ([]string, error) {
	fl, err := store.OpenObject(ctx, filename)
	if err != nil {
		return nil, fmt.Errorf("opening csv: %w", err)
	}
	defer fl.Close()

	r := csv.NewReader(fl)
	out, err := r.Read()
	if err != nil {
		return nil, err
	}

	return out, nil
}

func (t *TableFiller) Run(ctx context.Context) error {
	zlog.Info("table filler", zap.String("table", t.tblName))

	loadFiles, err := findFilesToLoad(ctx, t.in, t.tblName, t.stopBlockNum, t.startBlockNum)
	if err != nil {
		return fmt.Errorf("listing files: %w", err)
	}

	if len(loadFiles) == 0 {
		return fmt.Errorf("no file to process")
	}
	dbFields, err := extractFieldsFromFirstLine(ctx, loadFiles[0], t.in)
	if err != nil {
		return fmt.Errorf("extracting fields from first csv line: %w", err)
	}

	zlog.Info("files to load",
		zap.String("table", t.tblName),
		zap.Int("file_count", len(loadFiles)),
	)

	for _, filename := range loadFiles {
		zlog.Info("opening file", zap.String("file", filename))

		if err := t.injectFile(ctx, filename, dbFields); err != nil {
			return fmt.Errorf("failed to inject file %q: %w", filename, err)
		}
	}

	return nil
}

func (t *TableFiller) injectFile(ctx context.Context, filename string, dbFields []string) error {
	fl, err := t.in.OpenObject(ctx, filename)
	if err != nil {
		return fmt.Errorf("opening csv: %w", err)
	}
	defer fl.Close()

	query := fmt.Sprintf(`COPY %s.%s ("%s") FROM STDIN WITH (FORMAT CSV, HEADER)`,
		db.EscapeIdentifier(t.pqSchema),
		db.EscapeIdentifier(t.tblName),
		strings.Join(dbFields, `","`))
	zlog.Info("loading file into sql", zap.String("filename", filename), zap.String("table_name", t.tblName), zap.Strings("db_fields", dbFields))

	t0 := time.Now()

	conn, err := t.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("pool acquire: %w", err)
	}
	defer conn.Release()

	tag, err := conn.Conn().PgConn().CopyFrom(ctx, fl, query)
	if err != nil {
		return fmt.Errorf("failed COPY FROM for %q: %w", t.tblName, err)
	}
	count := tag.RowsAffected()
	elapsed := time.Since(t0)
	zlog.Info("loaded file into sql",
		zap.String("filename", filename),
		zap.String("table_name", t.tblName),
		zap.Int64("rows_affected", count),
		zap.Duration("elapsed", elapsed),
	)

	return nil
}

func findFilesToLoad(ctx context.Context, inputStore dstore.Store, tableName string, stopBlockNum, desiredStartBlockNum uint64) (out []string, err error) {
	err = inputStore.Walk(ctx, tableName+"/", func(filename string) (err error) {
		startBlockNum, endBlockNum, err := getBlockRange(filename)
		if err != nil {
			return fmt.Errorf("fail reading block range in %q: %w", filename, err)
		}

		if stopBlockNum != 0 && startBlockNum >= stopBlockNum {
			return dstore.StopIteration
		}

		if endBlockNum < desiredStartBlockNum {
			return nil
		}

		if strings.HasSuffix(filename, ".csv") {
			out = append(out, filename)
		}

		return nil
	})
	return
}

var blockRangeRegex = regexp.MustCompile(`(\d{10})-(\d{10})`)

func getBlockRange(filename string) (uint64, uint64, error) {
	match := blockRangeRegex.FindStringSubmatch(filename)
	if match == nil {
		return 0, 0, fmt.Errorf("no block range in filename: %s", filename)
	}

	startBlock, _ := strconv.ParseUint(match[1], 10, 64)
	stopBlock, _ := strconv.ParseUint(match[2], 10, 64)
	return startBlock, stopBlock, nil
}
