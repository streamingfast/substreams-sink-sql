package db

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/streamingfast/cli"
	sink "github.com/streamingfast/substreams-sink"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
)

// arrayExprToTextLiteral converts a SQL array expression like ARRAY[1,2]::bigint[] or '{1,2}'
// to a Postgres text array literal suitable for casting from text[] via ::type[] later.
// This is a best-effort transformation for builder use only.
func arrayExprToTextLiteral(expr string) string {
	s := strings.TrimSpace(expr)
	if strings.HasPrefix(s, "ARRAY[") {
		// Extract inside ARRAY[...]
		inner := strings.TrimPrefix(s, "ARRAY[")
		// Drop everything after the closing ']' then wrap in braces
		idx := strings.Index(inner, "]")
		if idx >= 0 {
			inner = inner[:idx]
		}
		return "{" + inner + "}"
	}
	// Already a brace literal or quoted brace literal
	if strings.HasPrefix(s, "'{") || strings.HasPrefix(s, "{") {
		// Strip trailing casts like '::type[]'
		if i := strings.Index(s, "::"); i >= 0 {
			s = s[:i]
		}
		// Remove leading quote if present
		if strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'") {
			s = strings.TrimPrefix(s, "'")
			s = strings.TrimSuffix(s, "'")
		}
		return s
	}
	// Fallback: wrap as single element
	return "{" + s + "}"
}

type PostgresDialect struct {
	cursorTableName  string
	historyTableName string
	schemaName       string
}

func NewPostgresDialect(schemaName string, cursorTableName string, historyTableName string) *PostgresDialect {
	return &PostgresDialect{
		cursorTableName:  cursorTableName,
		historyTableName: historyTableName,
		schemaName:       schemaName,
	}
}

func (d PostgresDialect) Revert(tx Tx, ctx context.Context, l *Loader, lastValidFinalBlock uint64) error {
	query := fmt.Sprintf(`SELECT op,table_name,pk,prev_value,block_num FROM %s WHERE "block_num" > %d ORDER BY "block_num" DESC`,
		d.historyTable(d.schemaName),
		lastValidFinalBlock,
	)

	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return err
	}

	var reversions []func() error
	l.logger.Info("reverting forked block block(s)", zap.Uint64("last_valid_final_block", lastValidFinalBlock))
	if rows != nil { // rows will be nil with no error only in testing scenarios
		defer rows.Close()
		for rows.Next() {
			var op string
			var table_name string
			var pk string
			var prev_value_nullable sql.NullString
			var block_num uint64
			if err := rows.Scan(&op, &table_name, &pk, &prev_value_nullable, &block_num); err != nil {
				return fmt.Errorf("scanning row: %w", err)
			}
			l.logger.Debug("reverting", zap.String("operation", op), zap.String("table_name", table_name), zap.String("pk", pk), zap.Uint64("block_num", block_num))
			prev_value := prev_value_nullable.String

			// we can't call revertOp inside this loop, because it calls tx.ExecContext,
			// which can't run while this query is "active" or it will silently discard the remaining rows!
			reversions = append(reversions, func() error {
				if err := d.revertOp(tx, ctx, op, table_name, pk, prev_value, block_num); err != nil {
					return fmt.Errorf("revertOp: %w", err)
				}
				return nil
			})
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterating on rows from query %q: %w", query, err)
		}
		for _, reversion := range reversions {
			if err := reversion(); err != nil {
				return fmt.Errorf("execution revert operation: %w", err)
			}
		}
	}
	pruneHistory := fmt.Sprintf(`DELETE FROM %s WHERE "block_num" > %d;`,
		d.historyTable(d.schemaName),
		lastValidFinalBlock,
	)

	_, err = tx.ExecContext(ctx, pruneHistory)
	if err != nil {
		return fmt.Errorf("executing pruneHistory: %w", err)
	}
	return nil
}

func (d PostgresDialect) Flush(tx Tx, ctx context.Context, l *Loader, outputModuleHash string, lastFinalBlock uint64) (int, error) {
	var totalRows int
	for entriesPair := l.entries.Oldest(); entriesPair != nil; entriesPair = entriesPair.Next() {
		entries := entriesPair.Value
		totalRows += entries.Len()

		if l.tracer.Enabled() {
			l.logger.Debug("flushing table rows", zap.String("table_name", entriesPair.Key), zap.Int("row_count", entries.Len()))
		}
	}

	allOperations := make([]*Operation, 0, totalRows)
	for entriesPair := l.entries.Oldest(); entriesPair != nil; entriesPair = entriesPair.Next() {
		entries := entriesPair.Value
		for entryPair := entries.Oldest(); entryPair != nil; entryPair = entryPair.Next() {
			allOperations = append(allOperations, entryPair.Value)
		}
	}

	slices.SortFunc(allOperations, func(a, b *Operation) int {
		return cmp.Compare(a.ordinal, b.ordinal)
	})

	// Build execution segments (single ops or VALUES batches) preserving global order
	type execSegment struct {
		kind  string // "single" | "values-batch"
		start int
		end   int
		sql   string      // prebuilt SQL for batch
		mode  PgBatchMode // values | unnest (only relevant for batch kind)
	}

	segments := make([]execSegment, 0, len(allOperations))
	mode := d.effectivePgBatchMode(l)
	batchSize := d.effectivePgBatchSize(l)
	if batchSize < 2 {
		batchSize = 2
	}

	for i := 0; i < len(allOperations); {
		op := allOperations[i]
		if (mode == PgBatchModeValues || mode == PgBatchModeUnnest) && op.opType == OperationTypeInsert {
			// Attempt to form a VALUES batch
			tbl := op.table
			start := i
			end := i
			count := 1
			for j := i + 1; j < len(allOperations) && count < batchSize; j++ {
				next := allOperations[j]
				if next.opType != OperationTypeInsert || next.table != tbl {
					break
				}
				end = j
				count++
			}

			if count >= 2 {
				cols, vals, planErr := d.computeInsertBatchPlan(allOperations[start : end+1])
				cte, needsHistory := d.buildInsertHistoryCTE(d.schemaName, allOperations[start:end+1])
				if planErr == nil && (needsHistory || true) { // allow both irreversible and reversible (with CTE)
					if l.tracer.Enabled() {
						l.logger.Debug("detected insert-only batch",
							zap.String("table_name", tbl.identifier),
							zap.Int("rows", count),
							zap.Int("columns", len(cols)),
							zap.Uint64("start_ordinal", allOperations[start].ordinal),
							zap.Uint64("end_ordinal", allOperations[end].ordinal),
							zap.Bool("with_history_cte", needsHistory),
						)
					}
					if mode == PgBatchModeValues {
						batchSQL := d.buildValuesInsertSQL(tbl, cols, vals)
						if needsHistory {
							batchSQL = cte + " " + strings.TrimSuffix(batchSQL, ";") + ";"
						}
						segments = append(segments, execSegment{kind: "values-batch", start: start, end: end, sql: batchSQL, mode: PgBatchModeValues})
					} else if mode == PgBatchModeUnnest {
						if batchSQL, err := d.buildUnnestInsertSQL(tbl, cols, vals); err == nil {
							if needsHistory {
								batchSQL = cte + " " + strings.TrimSuffix(batchSQL, ";") + ";"
							}
							segments = append(segments, execSegment{kind: "values-batch", start: start, end: end, sql: batchSQL, mode: PgBatchModeUnnest})
						} else {
							// Fallback to VALUES batch when UNNEST cannot be built (e.g., ragged arrays)
							batchSQL := d.buildValuesInsertSQL(tbl, cols, vals)
							if needsHistory {
								batchSQL = cte + " " + strings.TrimSuffix(batchSQL, ";") + ";"
							}
							segments = append(segments, execSegment{kind: "values-batch", start: start, end: end, sql: batchSQL, mode: PgBatchModeValues})
							if l.tracer.Enabled() {
								l.logger.Debug("fell back to VALUES batch after UNNEST build error",
									zap.String("table_name", tbl.identifier),
									zap.Error(err),
								)
							}
						}
					}
					i = end + 1
					continue
				}

				if l.tracer.Enabled() {
					l.logger.Debug("skipping batch candidate",
						zap.String("table_name", tbl.identifier),
						zap.Error(planErr),
					)
				}
			}
		}

		// Attempt UPSERT batching using UNNEST when enabled and not insert-only mode
		if mode == PgBatchModeUnnest && !d.isPgInsertOnly(l) && op.opType == OperationTypeUpsert {
			tbl := op.table
			start := i
			end := i
			count := 1
			for j := i + 1; j < len(allOperations) && count < batchSize; j++ {
				next := allOperations[j]
				if next.opType != OperationTypeUpsert || next.table != tbl {
					break
				}
				end = j
				count++
			}

			if count >= 2 {
				cols, vals, planErr := d.computeUpsertBatchPlan(allOperations[start : end+1])
				cte, needsHistory := d.buildUpsertHistoryCTE(d.schemaName, tbl, allOperations[start:end+1])
				if planErr == nil {
					if l.tracer.Enabled() {
						l.logger.Debug("detected upsert-only UNNEST batch",
							zap.String("table_name", tbl.identifier),
							zap.Int("rows", count),
							zap.Int("columns", len(cols)),
							zap.Uint64("start_ordinal", allOperations[start].ordinal),
							zap.Uint64("end_ordinal", allOperations[end].ordinal),
							zap.Bool("with_history_cte", needsHistory),
						)
					}
					if batchSQL, err := d.buildUnnestUpsertSQL(tbl, cols, vals); err == nil {
						if needsHistory {
							batchSQL = cte + " " + strings.TrimSuffix(batchSQL, ";") + ";"
						}
						segments = append(segments, execSegment{kind: "values-batch", start: start, end: end, sql: batchSQL, mode: PgBatchModeUnnest})
						i = end + 1
						continue
					} else {
						// Fallback to VALUES upsert when UNNEST cannot be built
						batchSQL := d.buildValuesUpsertSQL(tbl, cols, vals)
						if needsHistory {
							batchSQL = cte + " " + strings.TrimSuffix(batchSQL, ";") + ";"
						}
						segments = append(segments, execSegment{kind: "values-batch", start: start, end: end, sql: batchSQL, mode: PgBatchModeValues})
						i = end + 1
						continue
					}
				}

				if l.tracer.Enabled() {
					l.logger.Debug("skipping upsert UNNEST batch candidate",
						zap.String("table_name", tbl.identifier),
						zap.Error(planErr),
					)
				}
			}
		}

		// Attempt UPSERT batching using VALUES when enabled and not insert-only mode
		if mode == PgBatchModeValues && !d.isPgInsertOnly(l) && op.opType == OperationTypeUpsert {
			tbl := op.table
			start := i
			end := i
			count := 1
			for j := i + 1; j < len(allOperations) && count < batchSize; j++ {
				next := allOperations[j]
				if next.opType != OperationTypeUpsert || next.table != tbl {
					break
				}
				end = j
				count++
			}

			if count >= 2 {
				cols, vals, planErr := d.computeUpsertBatchPlan(allOperations[start : end+1])
				cte, needsHistory := d.buildUpsertHistoryCTE(d.schemaName, tbl, allOperations[start:end+1])
				if planErr == nil {
					if l.tracer.Enabled() {
						l.logger.Debug("detected upsert-only batch",
							zap.String("table_name", tbl.identifier),
							zap.Int("rows", count),
							zap.Int("columns", len(cols)),
							zap.Uint64("start_ordinal", allOperations[start].ordinal),
							zap.Uint64("end_ordinal", allOperations[end].ordinal),
							zap.Bool("with_history_cte", needsHistory),
						)
					}
					batchSQL := d.buildValuesUpsertSQL(tbl, cols, vals)
					if needsHistory {
						batchSQL = cte + " " + strings.TrimSuffix(batchSQL, ";") + ";"
					}
					segments = append(segments, execSegment{kind: "values-batch", start: start, end: end, sql: batchSQL, mode: PgBatchModeValues})
					i = end + 1
					continue
				}

				if l.tracer.Enabled() {
					l.logger.Debug("skipping upsert batch candidate",
						zap.String("table_name", tbl.identifier),
						zap.Error(planErr),
					)
				}
			}
		}

		// Fallback: single operation
		segments = append(segments, execSegment{kind: "single", start: i, end: i})
		i++
	}

	var rowCount int
	for _, seg := range segments {
		switch seg.kind {
		case "values-batch":
			if l.tracer.Enabled() {
				l.logger.Debug("executing VALUES batch insert", zap.Int("rows", seg.end-seg.start+1), zap.String("sql", seg.sql))
			}
			// For UNNEST mode, fail fast without per-row fallback to ease testing
			if seg.mode == PgBatchModeUnnest {
				if _, err := tx.ExecContext(ctx, seg.sql); err != nil {
					return 0, fmt.Errorf("executing UNNEST batch query %q: %w", seg.sql, err)
				}
				rowCount += (seg.end - seg.start + 1)
				break
			}

			// VALUES mode keeps safe fallback path under SAVEPOINT
			if _, spErr := tx.ExecContext(ctx, "SAVEPOINT sp_batch"); spErr == nil {
				if _, err := tx.ExecContext(ctx, seg.sql); err != nil {
					_, _ = tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT sp_batch")
					if l.tracer.Enabled() {
						l.logger.Debug("batch failed, falling back to per-row", zap.Error(err))
					}
					for i := seg.start; i <= seg.end; i++ {
						entry := allOperations[i]
						query, qerr := d.prepareStatement(d.schemaName, entry)
						if qerr != nil {
							return 0, fmt.Errorf("prepare fallback statement: %w", qerr)
						}
						if _, exErr := tx.ExecContext(ctx, query); exErr != nil {
							return 0, fmt.Errorf("executing fallback flush query %q: %w", query, exErr)
						}
						rowCount++
					}
					continue
				}
				_, _ = tx.ExecContext(ctx, "RELEASE SAVEPOINT sp_batch")
				rowCount += (seg.end - seg.start + 1)
			} else {
				// If SAVEPOINT unsupported, execute batch directly; on error, surface it
				if _, err := tx.ExecContext(ctx, seg.sql); err != nil {
					return 0, fmt.Errorf("executing batch insert query %q: %w", seg.sql, err)
				}
				rowCount += (seg.end - seg.start + 1)
			}
		case "single":
			entry := allOperations[seg.start]
			query, err := d.prepareStatement(d.schemaName, entry)
			if err != nil {
				return 0, fmt.Errorf("failed to prepare statement: %w", err)
			}
			if l.tracer.Enabled() {
				l.logger.Debug("adding query from operation to transaction", zap.Stringer("op", entry), zap.String("query", query), zap.Uint64("ordinal", entry.ordinal))
			}
			if _, err := tx.ExecContext(ctx, query); err != nil {
				return 0, fmt.Errorf("executing flush query %q: %w", query, err)
			}
			rowCount++
		}
	}

	if err := d.pruneReversibleSegment(tx, ctx, d.schemaName, lastFinalBlock); err != nil {
		return 0, err
	}

	return rowCount, nil
}

func (d PostgresDialect) revertOp(tx Tx, ctx context.Context, op, escaped_table_name, pk, prev_value string, block_num uint64) error {

	pkmap := make(map[string]string)
	if err := json.Unmarshal([]byte(pk), &pkmap); err != nil {
		return fmt.Errorf("revertOp: unmarshalling %q: %w", pk, err)
	}
	switch op {
	case "I":
		query := fmt.Sprintf(`DELETE FROM %s WHERE %s;`,
			escaped_table_name,
			getPrimaryKeyWhereClause(pkmap, ""),
		)
		if _, err := tx.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("executing revert query %q: %w", query, err)
		}
	case "D":
		query := fmt.Sprintf(`INSERT INTO %s SELECT * FROM json_populate_record(null::%s,%s);`,
			escaped_table_name,
			escaped_table_name,
			escapeStringValue(prev_value),
		)
		if _, err := tx.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("executing revert query %q: %w", query, err)
		}

	case "U":
		columns, err := sqlColumnNamesFromJSON(prev_value)
		if err != nil {
			return err
		}

		query := fmt.Sprintf(`UPDATE %s SET(%s)=((SELECT %s FROM json_populate_record(null::%s,%s))) WHERE %s;`,
			escaped_table_name,
			columns,
			columns,
			escaped_table_name,
			escapeStringValue(prev_value),
			getPrimaryKeyWhereClause(pkmap, ""),
		)
		if _, err := tx.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("executing revert query %q: %w", query, err)
		}
	default:
		panic("invalid op in revert command")
	}
	return nil
}

func sqlColumnNamesFromJSON(in string) (string, error) {
	valueMap := make(map[string]interface{})
	if err := json.Unmarshal([]byte(in), &valueMap); err != nil {
		return "", fmt.Errorf("unmarshalling %q into valueMap: %w", in, err)
	}
	escapedNames := make([]string, len(valueMap))
	i := 0
	for k := range valueMap {
		escapedNames[i] = EscapeIdentifier(k)
		i++
	}
	sort.Strings(escapedNames)

	return strings.Join(escapedNames, ","), nil
}

func (d PostgresDialect) pruneReversibleSegment(tx Tx, ctx context.Context, schema string, highestFinalBlock uint64) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE block_num <= %d;`, d.historyTable(schema), highestFinalBlock)
	if _, err := tx.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("executing prune query %q: %w", query, err)
	}
	return nil
}

func (d PostgresDialect) GetCreateCursorQuery(schema string, withPostgraphile bool) string {
	out := fmt.Sprintf(cli.Dedent(`
		create table if not exists %s.%s
		(
			id         text not null constraint %s primary key,
			cursor     text,
			block_num  bigint,
			block_id   text
		);
		`), EscapeIdentifier(schema), EscapeIdentifier(d.cursorTableName), EscapeIdentifier(d.cursorTableName+"_pk"))
	if withPostgraphile {
		out += fmt.Sprintf("COMMENT ON TABLE %s.%s IS E'@omit';",
			EscapeIdentifier(schema), EscapeIdentifier(d.cursorTableName))
	}
	return out
}

func (d PostgresDialect) GetCreateHistoryQuery(schema string, withPostgraphile bool) string {
	out := fmt.Sprintf(cli.Dedent(`
		create table if not exists %s
		(
            id           SERIAL PRIMARY KEY,
            op           char,
            table_name   text,
			pk           text,
            prev_value   text,
			block_num    bigint
		);
		`),
		d.historyTable(schema),
	)
	if withPostgraphile {
		out += fmt.Sprintf("COMMENT ON TABLE %s.%s IS E'@omit';",
			EscapeIdentifier(schema), EscapeIdentifier(d.historyTableName))
	}
	return out
}

func (d PostgresDialect) ExecuteSetupScript(ctx context.Context, l *Loader, schemaSql string) error {
	if _, err := l.ExecContext(ctx, schemaSql); err != nil {
		return fmt.Errorf("exec schemaName: %w", err)
	}
	return nil
}

func (d PostgresDialect) GetUpdateCursorQuery(table, moduleHash string, cursor *sink.Cursor, block_num uint64, block_id string) string {
	return query(`
		UPDATE %s set cursor = '%s', block_num = %d, block_id = '%s' WHERE id = '%s';
	`, table, cursor, block_num, block_id, moduleHash)
}

func (d PostgresDialect) GetAllCursorsQuery(table string) string {
	return fmt.Sprintf("SELECT id, cursor, block_num, block_id FROM %s", table)
}

func (d PostgresDialect) ParseDatetimeNormalization(value string) string {
	return escapeStringValue(value)
}

func (d PostgresDialect) DriverSupportRowsAffected() bool {
	return true
}

func (d PostgresDialect) OnlyInserts() bool {
	return false
}

func (d PostgresDialect) AllowPkDuplicates() bool {
	return false
}

// Helper accessors for Postgres insert batching configuration carried on Loader.
// These are intentionally resolved at Flush-time to keep Dialect construction unchanged.
type PgBatchMode string

const (
	PgBatchModeOff    PgBatchMode = "off"
	PgBatchModeValues PgBatchMode = "values"
	PgBatchModeUnnest PgBatchMode = "unnest"
)

func (d PostgresDialect) effectivePgBatchMode(l *Loader) PgBatchMode {
	switch strings.ToLower(l.PgInsertBatchMode()) {
	case string(PgBatchModeValues):
		return PgBatchModeValues
	case string(PgBatchModeUnnest):
		return PgBatchModeUnnest
	default:
		return PgBatchModeOff
	}
}

func (d PostgresDialect) effectivePgBatchSize(l *Loader) int {
	size := l.PgInsertBatchSize()
	if size <= 0 {
		return 1000
	}
	return size
}

func (d PostgresDialect) isPgInsertOnly(l *Loader) bool {
	return l.PgInsertOnly()
}

func (d PostgresDialect) CreateUser(tx Tx, ctx context.Context, l *Loader, username string, password string, database string, readOnly bool) error {
	user, pass, db := EscapeIdentifier(username), password, EscapeIdentifier(database)
	var q string
	if readOnly {
		q = fmt.Sprintf(`
            CREATE ROLE %s LOGIN PASSWORD '%s';
            GRANT CONNECT ON DATABASE %s TO %s;
            GRANT USAGE ON SCHEMA public TO %s;
            ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO %s;
            GRANT SELECT ON ALL TABLES IN SCHEMA public TO %s;
        `, user, pass, db, user, user, user, user)
	} else {
		q = fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s'; GRANT ALL PRIVILEGES ON DATABASE %s TO %s;", user, pass, db, user)
	}

	_, err := tx.ExecContext(ctx, q)
	if err != nil {
		return fmt.Errorf("executing create user query %q: %w", q, err)
	}

	return nil
}

func (d PostgresDialect) historyTable(schema string) string {
	return fmt.Sprintf("%s.%s", EscapeIdentifier(schema), EscapeIdentifier(d.historyTableName))
}

func (d PostgresDialect) saveInsert(schema string, table string, primaryKey map[string]string, blockNum uint64) string {
	return fmt.Sprintf(`INSERT INTO %s (op,table_name,pk,block_num) values (%s,%s,%s,%d);`,
		d.historyTable(schema),
		escapeStringValue("I"),
		escapeStringValue(table),
		escapeStringValue(primaryKeyToJSON(primaryKey)),
		blockNum,
	)
}

/*
with t as (select 'default' id)
select CASE WHEN block_meta.id is null THEN 'I' ELSE 'U' END AS op, '"public"."block_meta"', 'allo', row_to_json(block_meta),10  from t left join block_meta on block_meta.id='default';
*/
func (d PostgresDialect) saveUpsert(schema string, escapedTableName string, primaryKey map[string]string, blockNum uint64) string {
	schemaAndTable := fmt.Sprintf("%s.%s", EscapeIdentifier(schema), escapedTableName)

	return fmt.Sprintf(`
		WITH t as (select %s)
		INSERT INTO %s (op,table_name,pk,prev_value,block_num)
		SELECT CASE WHEN %s THEN 'I' ELSE 'U' END AS op, %s, %s, row_to_json(%s),%d from t left join %s.%s on %s;`,

		getPrimaryKeyFakeEmptyValues(primaryKey),
		d.historyTable(schema),

		getPrimaryKeyFakeEmptyValuesAssertion(primaryKey, escapedTableName),

		escapeStringValue(schemaAndTable), escapeStringValue(primaryKeyToJSON(primaryKey)), escapedTableName, blockNum,
		EscapeIdentifier(schema), escapedTableName,
		getPrimaryKeyWhereClause(primaryKey, escapedTableName),
	)

}

func (d PostgresDialect) saveUpdate(schema string, escapedTableName string, primaryKey map[string]string, blockNum uint64) string {
	return d.saveRow("U", schema, escapedTableName, primaryKey, blockNum)
}

func (d PostgresDialect) saveDelete(schema string, escapedTableName string, primaryKey map[string]string, blockNum uint64) string {
	return d.saveRow("D", schema, escapedTableName, primaryKey, blockNum)
}

func (d PostgresDialect) saveRow(op, schema, escapedTableName string, primaryKey map[string]string, blockNum uint64) string {
	schemaAndTable := fmt.Sprintf("%s.%s", EscapeIdentifier(schema), escapedTableName)
	return fmt.Sprintf(`INSERT INTO %s (op,table_name,pk,prev_value,block_num) SELECT %s,%s,%s,row_to_json(%s),%d FROM %s.%s WHERE %s;`,
		d.historyTable(schema),
		escapeStringValue(op), escapeStringValue(schemaAndTable), escapeStringValue(primaryKeyToJSON(primaryKey)), escapedTableName, blockNum,
		EscapeIdentifier(schema), escapedTableName,
		getPrimaryKeyWhereClause(primaryKey, ""),
	)

}

func (d *PostgresDialect) prepareStatement(schema string, o *Operation) (string, error) {
	var columns, values []string
	if o.opType == OperationTypeInsert || o.opType == OperationTypeUpsert || o.opType == OperationTypeUpdate {
		var err error
		columns, values, err = d.prepareColValues(o.table, o.data)
		if err != nil {
			return "", fmt.Errorf("preparing column & values: %w", err)
		}
	}

	if o.opType == OperationTypeUpsert || o.opType == OperationTypeUpdate || o.opType == OperationTypeDelete {
		// A table without a primary key set yield a `primaryKey` map with a single entry where the key is an empty string
		if _, found := o.primaryKey[""]; found {
			return "", fmt.Errorf("trying to perform %s operation but table %q don't have a primary key set, this is not accepted", o.opType, o.table.name)
		}
	}

	switch o.opType {
	case OperationTypeInsert:
		insertQuery := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);",
			o.table.identifier,
			strings.Join(columns, ","),
			strings.Join(values, ","),
		)

		if o.reversibleBlockNum != nil {
			return d.saveInsert(schema, o.table.identifier, o.primaryKey, *o.reversibleBlockNum) + insertQuery, nil
		}
		return insertQuery, nil

	case OperationTypeUpsert:
		updates := make([]string, len(columns))
		for i := range columns {
			updates[i] = fmt.Sprintf("%s=EXCLUDED.%s", columns[i], columns[i])
		}

		insertQuery := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s;",
			o.table.identifier,
			strings.Join(columns, ","),
			strings.Join(values, ","),
			strings.Join(maps.Keys(o.primaryKey), ","),
			strings.Join(updates, ", "),
		)

		if o.reversibleBlockNum != nil {
			return d.saveUpsert(schema, o.table.nameEscaped, o.primaryKey, *o.reversibleBlockNum) + insertQuery, nil
		}
		return insertQuery, nil

	case OperationTypeUpdate:
		updates := make([]string, len(columns))
		for i := 0; i < len(columns); i++ {
			updates[i] = fmt.Sprintf("%s=%s", columns[i], values[i])
		}

		primaryKeySelector := getPrimaryKeyWhereClause(o.primaryKey, "")

		updateQuery := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
			o.table.identifier,
			strings.Join(updates, ", "),
			primaryKeySelector,
		)

		if o.reversibleBlockNum != nil {
			return d.saveUpdate(schema, o.table.nameEscaped, o.primaryKey, *o.reversibleBlockNum) + updateQuery, nil
		}
		return updateQuery, nil

	case OperationTypeDelete:
		primaryKeyWhereClause := getPrimaryKeyWhereClause(o.primaryKey, "")
		deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE %s",
			o.table.identifier,
			primaryKeyWhereClause,
		)
		if o.reversibleBlockNum != nil {
			return d.saveDelete(schema, o.table.nameEscaped, o.primaryKey, *o.reversibleBlockNum) + deleteQuery, nil
		}
		return deleteQuery, nil

	default:
		panic(fmt.Errorf("unknown operation type %q", o.opType))
	}
}

func (d *PostgresDialect) prepareColValues(table *TableInfo, colValues map[string]string) (columns []string, values []string, err error) {
	if len(colValues) == 0 {
		return
	}

	columns = make([]string, len(colValues))
	values = make([]string, len(colValues))

	i := 0
	for colName := range colValues {
		columns[i] = colName
		i++
	}
	sort.Strings(columns) // sorted for determinism in tests

	for i, columnName := range columns {
		value := colValues[columnName]
		columnInfo, found := table.columnsByName[columnName]
		if !found {
			return nil, nil, fmt.Errorf("cannot find column %q for table %q (valid columns are %q)", columnName, table.identifier, strings.Join(maps.Keys(table.columnsByName), ", "))
		}

		normalizedValue, err := d.normalizeValueType(value, columnInfo.scanType)
		if err != nil {
			return nil, nil, fmt.Errorf("getting sql value from table %s for column %q raw value %q: %w", table.identifier, columnName, value, err)
		}

		values[i] = normalizedValue
		columns[i] = columnInfo.escapedName // escape the column name
	}
	return
}

// buildValuesInsertSQL constructs a multi-row VALUES insert for a single table using the
// provided escaped column names and per-row SQL-literal values (already normalized).
func (d *PostgresDialect) buildValuesInsertSQL(table *TableInfo, columnsEscaped []string, perRowValues [][]string) string {
	valuesParts := make([]string, len(perRowValues))
	for i := range perRowValues {
		valuesParts[i] = fmt.Sprintf("(%s)", strings.Join(perRowValues[i], ","))
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES %s;",
		table.identifier,
		strings.Join(columnsEscaped, ","),
		strings.Join(valuesParts, ","),
	)
}

// buildUnnestInsertSQL constructs an UNNEST-based insert for a single table.
// It takes escaped column names and per-row SQL-literal values, and produces a
// statement like:
//
//	INSERT INTO schema.table (col1, col2)
//	SELECT * FROM unnest(ARRAY[...], ARRAY[...]);
//
// We use text arrays and rely on Postgres implicit cast when possible; for
// robustness, we cast each array to the column's database type name when known.
func (d *PostgresDialect) buildUnnestInsertSQL(table *TableInfo, columnsEscaped []string, perRowValues [][]string) (string, error) {
	if len(columnsEscaped) == 0 || len(perRowValues) == 0 {
		return "", fmt.Errorf("empty columns or rows for UNNEST")
	}

	// Build arrays per column from perRowValues
	numCols := len(columnsEscaped)

	// Invert rows to columns
	colsToValues := make([][]string, numCols)
	for i := 0; i < numCols; i++ {
		colsToValues[i] = make([]string, len(perRowValues))
	}
	for r, row := range perRowValues {
		if len(row) != numCols {
			return "", fmt.Errorf("row %d has %d values, expected %d", r, len(row), numCols)
		}
		for c := 0; c < numCols; c++ {
			colsToValues[c][r] = row[c]
		}
	}

	// Determine type casts for each column from table metadata
	escapedToInfo := make(map[string]*ColumnInfo, len(table.columnsByName))
	for _, ci := range table.columnsByName {
		escapedToInfo[ci.escapedName] = ci
	}

	// Partition into scalar and array-typed columns
	scalarIdx := make([]int, 0, numCols)
	arrayIdx := make([]int, 0, numCols)
	baseTypes := make([]string, numCols)
	isArrayCol := make([]bool, numCols)
	for i, esc := range columnsEscaped {
		ci := escapedToInfo[esc]
		bt, isArr := canonicalizePostgresType(ci.databaseTypeName)
		baseTypes[i] = bt
		isArrayCol[i] = isArr
		if isArr {
			arrayIdx = append(arrayIdx, i)
		} else {
			scalarIdx = append(scalarIdx, i)
		}
	}

	// If no scalar columns, bail out (let caller fallback)
	if len(scalarIdx) == 0 {
		return "", fmt.Errorf("no scalar columns for UNNEST WITH ORDINALITY")
	}

	// Build unnest args for scalar columns and select projection
	sAliases := make([]string, len(scalarIdx)+1) // +1 for ord
	for i := 0; i < len(scalarIdx); i++ {
		sAliases[i] = fmt.Sprintf("c%d", scalarIdx[i])
	}
	sAliases[len(sAliases)-1] = "ord"

	sUnnestArgs := make([]string, len(scalarIdx))
	for k, colIdx := range scalarIdx {
		bt := baseTypes[colIdx]
		arr := fmt.Sprintf("ARRAY[%s]", strings.Join(colsToValues[colIdx], ","))
		if bt != "" {
			arr = fmt.Sprintf("%s::%s[]", arr, bt)
		}
		sUnnestArgs[k] = arr
	}

	// Build projection list for all columns in original order
	projections := make([]string, numCols)
	for i := 0; i < numCols; i++ {
		if !isArrayCol[i] {
			bt := baseTypes[i]
			alias := fmt.Sprintf("s.c%d", i)
			if bt != "" {
				projections[i] = fmt.Sprintf("(%s)::%s", alias, bt)
			} else {
				projections[i] = alias
			}
			continue
		}
		// Array-typed: avoid 2D arrays; select the per-row array using CASE on ord
		bt := baseTypes[i]
		cases := make([]string, 0, len(perRowValues)+2)
		cases = append(cases, "CASE ((s.ord)::int)")
		for r := 0; r < len(perRowValues); r++ {
			v := colsToValues[i][r]
			var rowExpr string
			if v == "NULL" {
				rowExpr = fmt.Sprintf("NULL::%s[]", bt)
			} else {
				textLit := arrayExprToTextLiteral(strings.Trim(v, "'"))
				rowExpr = fmt.Sprintf("(%s)::%s[]", escapeStringValue(textLit), bt)
			}
			cases = append(cases, fmt.Sprintf("WHEN %d THEN %s", r+1, rowExpr))
		}
		cases = append(cases, fmt.Sprintf("ELSE '{}'::%s[] END", bt))
		projections[i] = strings.Join(cases, " ")
	}

	selectList := strings.Join(projections, ",")
	unnestArgs := strings.Join(sUnnestArgs, ", ")
	aliasList := strings.Join(sAliases, ",")
	return fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM unnest(%s) WITH ORDINALITY AS s(%s);",
		table.identifier,
		strings.Join(columnsEscaped, ","),
		selectList,
		unnestArgs,
		aliasList,
	), nil
}

// buildUnnestUpsertSQL constructs an UNNEST-based upsert for a single table using escaped
// column names and per-row SQL-literal values. It builds per-column ARRAY[...] with type casts
// and emits ON CONFLICT over the table primary key columns, updating all provided columns.
func (d *PostgresDialect) buildUnnestUpsertSQL(table *TableInfo, columnsEscaped []string, perRowValues [][]string) (string, error) {
	if len(columnsEscaped) == 0 || len(perRowValues) == 0 {
		return "", fmt.Errorf("empty columns or rows for UNNEST upsert")
	}

	// Build arrays per column from perRowValues
	numCols := len(columnsEscaped)

	// Invert rows to columns
	colsToValues := make([][]string, numCols)
	for i := 0; i < numCols; i++ {
		colsToValues[i] = make([]string, len(perRowValues))
	}
	for r, row := range perRowValues {
		if len(row) != numCols {
			return "", fmt.Errorf("row %d has %d values, expected %d", r, len(row), numCols)
		}
		for c := 0; c < numCols; c++ {
			colsToValues[c][r] = row[c]
		}
	}

	// Reverse-map escaped to ColumnInfo
	escapedToInfo := make(map[string]*ColumnInfo, len(table.columnsByName))
	for _, ci := range table.columnsByName {
		escapedToInfo[ci.escapedName] = ci
	}

	// Partition columns
	scalarIdx := make([]int, 0, numCols)
	arrayIdx := make([]int, 0, numCols)
	baseTypes := make([]string, numCols)
	isArrayCol := make([]bool, numCols)
	for i, esc := range columnsEscaped {
		ci := escapedToInfo[esc]
		bt, isArr := canonicalizePostgresType(ci.databaseTypeName)
		baseTypes[i] = bt
		isArrayCol[i] = isArr
		if isArr {
			arrayIdx = append(arrayIdx, i)
		} else {
			scalarIdx = append(scalarIdx, i)
		}
	}

	if len(scalarIdx) == 0 {
		return "", fmt.Errorf("no scalar columns for UNNEST WITH ORDINALITY")
	}

	// Unnest args and aliases for scalars
	sAliases := make([]string, len(scalarIdx)+1)
	for i := 0; i < len(scalarIdx); i++ {
		sAliases[i] = fmt.Sprintf("c%d", scalarIdx[i])
	}
	sAliases[len(sAliases)-1] = "ord"

	sUnnestArgs := make([]string, len(scalarIdx))
	for k, colIdx := range scalarIdx {
		bt := baseTypes[colIdx]
		arr := fmt.Sprintf("ARRAY[%s]", strings.Join(colsToValues[colIdx], ","))
		if bt != "" {
			arr = fmt.Sprintf("%s::%s[]", arr, bt)
		}
		sUnnestArgs[k] = arr
	}

	// Build projection list
	projections := make([]string, numCols)
	for i := 0; i < numCols; i++ {
		if !isArrayCol[i] {
			bt := baseTypes[i]
			alias := fmt.Sprintf("s.c%d", i)
			if bt != "" {
				projections[i] = fmt.Sprintf("(%s)::%s", alias, bt)
			} else {
				projections[i] = alias
			}
			continue
		}
		// Array-typed: avoid 2D arrays; select the per-row array using CASE on ord
		bt := baseTypes[i]
		cases := make([]string, 0, len(perRowValues)+2)
		cases = append(cases, "CASE ((s.ord)::int)")
		for r := 0; r < len(perRowValues); r++ {
			v := colsToValues[i][r]
			var rowExpr string
			if v == "NULL" {
				rowExpr = fmt.Sprintf("NULL::%s[]", bt)
			} else {
				textLit := arrayExprToTextLiteral(strings.Trim(v, "'"))
				rowExpr = fmt.Sprintf("(%s)::%s[]", escapeStringValue(textLit), bt)
			}
			cases = append(cases, fmt.Sprintf("WHEN %d THEN %s", r+1, rowExpr))
		}
		cases = append(cases, fmt.Sprintf("ELSE '{}'::%s[] END", bt))
		projections[i] = strings.Join(cases, " ")
	}

	// conflict target from table primary key columns
	conflictCols := make([]string, len(table.primaryColumns))
	for i, pk := range table.primaryColumns {
		conflictCols[i] = pk.escapedName
	}
	updates := make([]string, len(columnsEscaped))
	for i := range columnsEscaped {
		updates[i] = fmt.Sprintf("%s=EXCLUDED.%s", columnsEscaped[i], columnsEscaped[i])
	}

	unnestArgs := strings.Join(sUnnestArgs, ", ")
	selectList := strings.Join(projections, ",")
	aliasList := strings.Join(sAliases, ",")
	return fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM unnest(%s) WITH ORDINALITY AS s(%s) ON CONFLICT (%s) DO UPDATE SET %s;",
		table.identifier,
		strings.Join(columnsEscaped, ","),
		selectList,
		unnestArgs,
		aliasList,
		strings.Join(conflictCols, ","),
		strings.Join(updates, ", "),
	), nil
}

// canonicalizePostgresType maps driver DatabaseTypeName (e.g., INT8, _INT8)
// to canonical Postgres base type (e.g., bigint) and whether the column itself
// is an array type.
func canonicalizePostgresType(databaseTypeName string) (baseType string, isArray bool) {
	if databaseTypeName == "" {
		return "", false
	}
	// Detect array-typed column from leading underscore (pq convention)
	isArray = strings.HasPrefix(databaseTypeName, "_")
	name := databaseTypeName
	if isArray {
		name = name[1:]
	}
	switch strings.ToUpper(name) {
	case "INT8", "BIGINT":
		baseType = "bigint"
	case "INT4", "INTEGER", "INT":
		baseType = "integer"
	case "INT2", "SMALLINT":
		baseType = "smallint"
	case "BOOL", "BOOLEAN":
		baseType = "boolean"
	case "VARCHAR", "TEXT":
		baseType = "varchar"
	case "TIMESTAMP":
		baseType = "timestamp"
	case "TIMESTAMPTZ":
		baseType = "timestamptz"
	case "NUMERIC", "DECIMAL":
		baseType = "numeric"
	case "BYTEA":
		baseType = "bytea"
	default:
		// Fallback to lowercase of provided name
		baseType = strings.ToLower(name)
	}
	return baseType, isArray
}

// buildInsertHistoryCTE builds a CTE that inserts one history row per INSERT operation in ops
// when those operations require reversible tracking (i.e., have a non-nil reversibleBlockNum).
// Returns the CTE SQL (without trailing semicolon) and a boolean indicating whether any history
// rows are needed. If none are needed, returns "", false.
func (d PostgresDialect) buildInsertHistoryCTE(schema string, ops []*Operation) (string, bool) {
	if len(ops) == 0 {
		return "", false
	}
	values := make([]string, 0, len(ops))
	// All ops in the batch target the same table
	tableName := escapeStringValue(ops[0].table.identifier)
	for _, op := range ops {
		if op.reversibleBlockNum == nil {
			continue
		}
		// op must be INSERT for this helper
		pkJSON := escapeStringValue(primaryKeyToJSON(op.primaryKey))
		values = append(values, fmt.Sprintf("(%s,%s,%s,%d)",
			escapeStringValue("I"),
			tableName,
			pkJSON,
			*op.reversibleBlockNum,
		))
	}
	if len(values) == 0 {
		return "", false
	}
	cte := fmt.Sprintf("WITH history_cte AS (INSERT INTO %s (op,table_name,pk,block_num) VALUES %s RETURNING 1)",
		d.historyTable(schema),
		strings.Join(values, ","),
	)
	return cte, true
}

// buildUpsertHistoryCTE builds a CTE that inserts one history row per UPSERT operation in ops
// for rows that require reversible tracking (have a non-nil reversibleBlockNum). It determines
// for each such row whether it would be an insert (no existing target row) or an update, and
// records prev_value via row_to_json(target) for updates. Returns the CTE SQL (without trailing
// semicolon) and a boolean indicating whether any history rows are needed.
func (d PostgresDialect) buildUpsertHistoryCTE(schema string, table *TableInfo, ops []*Operation) (string, bool) {
	if len(ops) == 0 {
		return "", false
	}

	// Build VALUES rows for reversible ops only: (pk1, pk2, ..., pk_json, block_num)
	pkCols := table.primaryColumns
	if len(pkCols) == 0 {
		return "", false
	}

	// Build column alias list for src, quoting actual pk column names
	srcCols := make([]string, 0, len(pkCols)+2)
	for _, pk := range pkCols {
		srcCols = append(srcCols, pk.escapedName)
	}
	srcCols = append(srcCols, "pk_json", "block_num")

	values := make([]string, 0, len(ops))

	for _, op := range ops {
		if op.reversibleBlockNum == nil {
			continue
		}
		rowVals := make([]string, 0, len(pkCols)+2)
		// pk values in declared order
		for _, pk := range pkCols {
			raw, ok := op.primaryKey[pk.name]
			if !ok {
				// if primary key missing, skip this row (safer than failing the whole batch)
				rowVals = nil
				break
			}
			norm, err := d.normalizeValueType(raw, pk.scanType)
			if err != nil {
				rowVals = nil
				break
			}
			rowVals = append(rowVals, norm)
		}
		if rowVals == nil {
			continue
		}
		// pk_json literal
		rowVals = append(rowVals, escapeStringValue(primaryKeyToJSON(op.primaryKey)))
		// block number literal
		rowVals = append(rowVals, fmt.Sprintf("%d", *op.reversibleBlockNum))
		values = append(values, fmt.Sprintf("(%s)", strings.Join(rowVals, ",")))
	}

	if len(values) == 0 {
		return "", false
	}

	// Build ON clause using pk columns
	var onParts []string
	for _, pk := range pkCols {
		onParts = append(onParts, fmt.Sprintf("%s = src.%s", pk.escapedName, pk.escapedName))
	}
	sort.Strings(onParts)
	joinOn := strings.Join(onParts, " AND ")

	cte := fmt.Sprintf("WITH history_cte AS (WITH src(%s) AS (VALUES %s) INSERT INTO %s (op,table_name,pk,prev_value,block_num) SELECT CASE WHEN target.%s IS NULL THEN 'I' ELSE 'U' END AS op, %s, src.pk_json, CASE WHEN target.%s IS NULL THEN NULL ELSE row_to_json(target) END AS prev_value, src.block_num FROM %s AS src LEFT JOIN %s AS target ON %s RETURNING 1)",
		strings.Join(srcCols, ","),
		strings.Join(values, ","),
		d.historyTable(schema),
		pkCols[0].escapedName,
		escapeStringValue(fmt.Sprintf("%s.%s", EscapeIdentifier(schema), table.nameEscaped)),
		pkCols[0].escapedName,
		"src",
		table.identifier,
		joinOn,
	)
	return cte, true
}

// computeInsertBatchPlan computes a stable, sorted superset of columns across the provided
// INSERT operations (which must all target the same table) and returns the escaped
// column names along with per-row values aligned to that column order. Missing
// fields are represented as SQL NULL.
func (d *PostgresDialect) computeInsertBatchPlan(ops []*Operation) (columnsEscaped []string, perRowValues [][]string, err error) {
	if len(ops) == 0 {
		return nil, nil, fmt.Errorf("empty operation slice for batch plan")
	}

	table := ops[0].table
	// Validate homogeneity: same table and INSERT type
	for _, op := range ops {
		if op.opType != OperationTypeInsert {
			return nil, nil, fmt.Errorf("non-insert operation encountered in insert batch candidate")
		}
		if op.table != table {
			return nil, nil, fmt.Errorf("mixed tables in insert batch candidate")
		}
	}

	// Build superset of raw column names across rows and ensure primary keys are included
	rawNamesSet := make(map[string]struct{})
	for _, op := range ops {
		for k := range op.data {
			rawNamesSet[k] = struct{}{}
		}
	}
	for _, pk := range table.primaryColumns {
		rawNamesSet[pk.name] = struct{}{}
	}

	rawNames := make([]string, 0, len(rawNamesSet))
	for name := range rawNamesSet {
		// Validate column exists on table schema
		if _, found := table.columnsByName[name]; !found {
			return nil, nil, fmt.Errorf("unknown column %q for table %s", name, table.identifier)
		}
		rawNames = append(rawNames, name)
	}
	sort.Strings(rawNames)

	// Produce escaped column names and a lookup for ColumnInfo
	columnsEscaped = make([]string, len(rawNames))
	colInfos := make([]*ColumnInfo, len(rawNames))
	for i, name := range rawNames {
		info := table.columnsByName[name]
		colInfos[i] = info
		columnsEscaped[i] = info.escapedName
	}

	// Build per-row values aligned with the stable column order
	perRowValues = make([][]string, len(ops))
	for rowIdx, op := range ops {
		row := make([]string, len(rawNames))
		for colIdx, name := range rawNames {
			if v, ok := op.data[name]; ok {
				normalized, nerr := d.normalizeValueType(v, colInfos[colIdx].scanType)
				if nerr != nil {
					return nil, nil, fmt.Errorf("normalize value for column %q: %w", name, nerr)
				}
				row[colIdx] = normalized
			} else {
				row[colIdx] = "NULL"
			}
		}
		perRowValues[rowIdx] = row
	}

	return columnsEscaped, perRowValues, nil
}

// computeUpsertBatchPlan computes a stable column list for UPSERT batching. To preserve semantics
// of single-row upserts (only updating explicitly provided columns), this requires that all rows
// in the batch share the exact same set of data columns and that primary key columns are present
// in every row. Returns escaped column names and per-row normalized values aligned to that order.
func (d *PostgresDialect) computeUpsertBatchPlan(ops []*Operation) (columnsEscaped []string, perRowValues [][]string, err error) {
	if len(ops) == 0 {
		return nil, nil, fmt.Errorf("empty operation slice for upsert batch plan")
	}

	table := ops[0].table
	for _, op := range ops {
		if op.opType != OperationTypeUpsert {
			return nil, nil, fmt.Errorf("non-upsert operation encountered in upsert batch candidate")
		}
		if op.table != table {
			return nil, nil, fmt.Errorf("mixed tables in upsert batch candidate")
		}
	}

	// Column set from first row (raw names)
	firstSet := make(map[string]struct{})
	for k := range ops[0].data {
		firstSet[k] = struct{}{}
	}
	// Ensure all primary key columns are present in data
	for _, pk := range table.primaryColumns {
		if _, ok := firstSet[pk.name]; !ok {
			return nil, nil, fmt.Errorf("primary key column %q missing from upsert data", pk.name)
		}
	}

	// Validate all rows have identical column set
	for _, op := range ops[1:] {
		if len(op.data) != len(firstSet) {
			return nil, nil, fmt.Errorf("heterogeneous columns across upsert batch")
		}
		for k := range op.data {
			if _, ok := firstSet[k]; !ok {
				return nil, nil, fmt.Errorf("heterogeneous columns across upsert batch")
			}
		}
	}

	// Build stable ordered list
	rawNames := make([]string, 0, len(firstSet))
	for name := range firstSet {
		// Validate column exists on table schema
		if _, found := table.columnsByName[name]; !found {
			return nil, nil, fmt.Errorf("unknown column %q for table %s", name, table.identifier)
		}
		rawNames = append(rawNames, name)
	}
	sort.Strings(rawNames)

	// Produce escaped names and normalize values per row
	columnsEscaped = make([]string, len(rawNames))
	colInfos := make([]*ColumnInfo, len(rawNames))
	for i, name := range rawNames {
		info := table.columnsByName[name]
		colInfos[i] = info
		columnsEscaped[i] = info.escapedName
	}

	perRowValues = make([][]string, len(ops))
	for rowIdx, op := range ops {
		row := make([]string, len(rawNames))
		for colIdx, name := range rawNames {
			v, ok := op.data[name]
			if !ok {
				return nil, nil, fmt.Errorf("unexpected missing column %q in upsert row", name)
			}
			normalized, nerr := d.normalizeValueType(v, colInfos[colIdx].scanType)
			if nerr != nil {
				return nil, nil, fmt.Errorf("normalize value for column %q: %w", name, nerr)
			}
			row[colIdx] = normalized
		}
		perRowValues[rowIdx] = row
	}

	return columnsEscaped, perRowValues, nil
}

// buildValuesUpsertSQL constructs a multi-row VALUES upsert for a single table using the provided
// escaped column names and per-row values. It appends an ON CONFLICT clause using the table's
// primary key columns and updates all provided columns to EXCLUDED values.
func (d *PostgresDialect) buildValuesUpsertSQL(table *TableInfo, columnsEscaped []string, perRowValues [][]string) string {
	valuesParts := make([]string, len(perRowValues))
	for i := range perRowValues {
		valuesParts[i] = fmt.Sprintf("(%s)", strings.Join(perRowValues[i], ","))
	}
	// conflict target from table primary key columns
	conflictCols := make([]string, len(table.primaryColumns))
	for i, pk := range table.primaryColumns {
		conflictCols[i] = pk.escapedName
	}
	updates := make([]string, len(columnsEscaped))
	for i := range columnsEscaped {
		updates[i] = fmt.Sprintf("%s=EXCLUDED.%s", columnsEscaped[i], columnsEscaped[i])
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON CONFLICT (%s) DO UPDATE SET %s;",
		table.identifier,
		strings.Join(columnsEscaped, ","),
		strings.Join(valuesParts, ","),
		strings.Join(conflictCols, ","),
		strings.Join(updates, ", "),
	)
}

func getPrimaryKeyFakeEmptyValues(primaryKey map[string]string) string {
	if len(primaryKey) == 1 {
		for key := range primaryKey {
			return "'' " + EscapeIdentifier(key)
		}
	}

	reg := make([]string, 0, len(primaryKey))
	for key := range primaryKey {
		reg = append(reg, "'' "+EscapeIdentifier(key))
	}
	sort.Strings(reg)

	return strings.Join(reg, ",")
}

func getPrimaryKeyFakeEmptyValuesAssertion(primaryKey map[string]string, escapedTableName string) string {
	if len(primaryKey) == 1 {
		for key := range primaryKey {
			return escapedTableName + "." + EscapeIdentifier(key) + " IS NULL"
		}
	}

	reg := make([]string, 0, len(primaryKey))
	for key := range primaryKey {
		reg = append(reg, escapedTableName+"."+EscapeIdentifier(key)+" IS NULL")
	}
	sort.Strings(reg)

	return strings.Join(reg, " AND ")
}

func getPrimaryKeyWhereClause(primaryKey map[string]string, escapedTableName string) string {
	// Avoid any allocation if there is a single primary key
	if len(primaryKey) == 1 {
		for key, value := range primaryKey {
			if escapedTableName == "" {
				return EscapeIdentifier(key) + " = " + escapeStringValue(value)
			}

			return escapedTableName + "." + EscapeIdentifier(key) + " = " + escapeStringValue(value)
		}
	}

	reg := make([]string, 0, len(primaryKey))
	for key, value := range primaryKey {

		if escapedTableName == "" {
			reg = append(reg, EscapeIdentifier(key)+" = "+escapeStringValue(value))
		} else {
			reg = append(reg, escapedTableName+"."+EscapeIdentifier(key)+" = "+escapeStringValue(value))
		}
	}
	sort.Strings(reg)

	return strings.Join(reg[:], " AND ")
}

// Format based on type, value returned unescaped
func (d *PostgresDialect) normalizeValueType(value string, valueType reflect.Type) (string, error) {
	switch valueType.Kind() {
	case reflect.String:
		// replace unicode null character with empty string
		value = strings.ReplaceAll(value, "\u0000", "")
		return escapeStringValue(value), nil

	// BYTES in Postgres must be escaped, we receive a Vec<u8> from substreams
	case reflect.Slice:
		return escapeStringValue(value), nil

	case reflect.Bool:
		return fmt.Sprintf("'%s'", value), nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return value, nil

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return value, nil

	case reflect.Float32, reflect.Float64:
		return value, nil

	case reflect.Struct:
		if valueType == reflectTypeTime {
			if integerRegex.MatchString(value) {
				i, err := strconv.Atoi(value)
				if err != nil {
					return "", fmt.Errorf("could not convert %s to int: %w", value, err)
				}

				return escapeStringValue(time.Unix(int64(i), 0).Format(time.RFC3339)), nil
			}

			// It's a plain string, parse by dialect it and pass it to the database
			return d.ParseDatetimeNormalization(value), nil
		}

		return "", fmt.Errorf("unsupported struct type %s", valueType)
	default:
		// It's a column's type the schemaName parsing don't know how to represents as
		// a Go type. In that case, we pass it unmodified to the database engine. It
		// will be the responsibility of the one sending the data to correctly represent
		// it in the way accepted by the database.
		//
		// In most cases, it going to just work.
		return value, nil
	}
}
