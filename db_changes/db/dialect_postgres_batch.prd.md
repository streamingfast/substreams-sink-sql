---
description: Enable optional INSERT-ONLY batched ingestion for Postgres (VALUES/UNNEST), preserving safety (ordering, history) and offering measurable ingestion speedups.
globs: *.prd.md
alwaysApply: false
---

### Context & Goal

The current Postgres dialect applies one operation at a time in strict `ordinal` order, logging a 1:1 history row per mutation (with `prev_value` JSON) and supporting mixed operation types (INSERT/UPSERT/UPDATE/DELETE) and arbitrary per-row column sets. This prevents using multi-row INSERT batching by default.

Goal: introduce an optional INSERT-ONLY batching mode for Postgres that safely batches inserts when conditions are met, with a choice of implementations (multi-row `VALUES` or `SELECT FROM unnest(...)`) and a guard-rail fallback to the existing single-row path. We will measure performance impact and document trade-offs.

Reference: TigerData article on UNNEST-based batching performance improvements in Postgres [Boosting Postgres INSERT Performance by 2x With UNNEST](https://www.tigerdata.com/blog/boosting-postgres-insert-performance).

### Non-Goals

- Changing ClickHouse batching behavior (serves as prior art only).
- Changing history/reorg semantics globally; batching applies only when explicitly enabled and safe.

### Constraints & Rationale (why batching isn’t default today)

- Mixed operation types require global ordering across INSERT/UPSERT/UPDATE/DELETE.
- Rows can carry arbitrary column subsets; single batched statement needs a shared column list.
- History table stores per-op `prev_value` JSON; we must log per-row history before mutation.
- Reorg safety depends on 1:1 mapping between mutation and history entry in correct order.

### Rollout & Config

- Add flags (subject to review):
  - `--pg-insert-batch-mode=off|values|unnest` (default `off`).
  - `--pg-insert-batch-size=<int>` (default `1000`).
  - `--pg-insert-only=true|false` (default `false`). When `true`, sink will reject non-insert ops or auto-fallback to single-row for those ops.
- Batching activates only if:
  - Current flush window contains only INSERT operations for a table segment chosen for batching.
  - All rows in the batch can share a stable column set (either identical or we can safely expand to a superset with defaults/nulls).
  - History logging can be emitted per row (e.g., via CTE) prior to insert.
- Fallback: if any check fails, use existing single-row path transparently.

---

## Tasks

- [x] 1. Add CLI flags for Postgres batching
  - Definition:
    - Add `--pg-insert-batch-mode`, `--pg-insert-batch-size`, `--pg-insert-only` flags and plumb them into config/dialect wiring.
    - Relevant files: `cmd/substreams-sink-sql/common_flags.go`, `cmd/substreams-sink-sql/run.go`, `db_changes/db/db.go` (Loader wiring). Confidence: high these are sufficient.
  - Progress:
    - Expected work: Added runtime-only flags in `cmd/substreams-sink-sql/run.go` — `--pg-insert-batch-mode` (off|values|unnest), `--pg-insert-batch-size` (default 1000), `--pg-insert-only` (bool). Intentional: keep flags local to run command.
    - Unexpected skipped work: Did not add flags to `common_flags.go` or other commands (e.g., `from-proto`, `tools`) by design; plumbing into Loader/dialect deferred to Task 2.
    - Unexpected extra work: Ran linter checks; no issues reported for modified file.
    - Learnings: Runtime flags belong in `run.go`; wiring typically flows `run.go` → `db_changes/sinker/factory.go` → `db.NewLoader(...)` → dialect. Task 2 should include `db_changes/sinker/factory.go` in relevant files.
    - Deliberate tech debt: Flags not yet propagated to Loader/dialect; no tests/docs for new flags yet; DSN-based config (optional) postponed.

- [x] 2. Surface batching settings to Postgres dialect
  - Definition:
    - Extend `PostgresDialect`/`Loader` to expose batching settings to the dialect. Prefer resolving at flush-time from `Loader` to avoid constructor churn; alternatively, pass via `newDialect`.
    - Relevant files: `db_changes/db/db.go`, `db_changes/db/dialect_postgres.go`, `db_changes/sinker/factory.go` (for wiring in a later step). Confidence: high.
  - Progress:
    - Expected work: Added `Loader` fields/getters `PgInsertBatchMode()`, `PgInsertBatchSize()`, `PgInsertOnly()` in `db.go`; added `PgBatchMode` enum and helpers `effectivePgBatchMode`, `effectivePgBatchSize`, `isPgInsertOnly` in `dialect_postgres.go` to read settings at flush-time.
    - Unexpected skipped work: Did not modify `NewPostgresDialect`/`newDialect` signatures; decided to resolve config from `Loader` during `Flush` instead of passing via constructor.
    - Unexpected extra work: None.
    - Learnings: Keeping dialect construction stable reduces blast radius; `Loader` already flows into `Flush`, so accessing runtime flags via `Loader` is cleaner.
    - Deliberate tech debt: Flags are not yet assigned to `Loader` fields (will be wired via `db_changes/sinker/factory.go` and `run.go` in the next step). No tests yet for helpers.

- [x] 2a. Wire runtime flags into Loader via sinker factory
  - Definition:
    - Read flags in `run.go`, extend `db_changes/sinker/factory.go` options to carry `pg-insert-*` settings, and assign `Loader.pgInsertBatchMode`, `Loader.pgInsertBatchSize`, `Loader.pgInsertOnly` when constructing the loader.
    - Relevant files: `cmd/substreams-sink-sql/run.go`, `db_changes/sinker/factory.go`, `db_changes/db/db.go`. Confidence: high.
  - Progress:
    - Expected work: Added fields to factory options; read flags in `run.go`; pass settings to factory; call `ConfigurePgInsertBatching` on `Loader` in factory.
    - Unexpected skipped work: None.
    - Unexpected extra work: None.
    - Learnings: Centralizing config assignment in the factory keeps `NewLoader` stable and minimizes CLI coupling in db layer.
    - Deliberate tech debt: No unit tests yet validating option flow; DSN fallback still deferred.

- [x] 3. Detect INSERT-ONLY batches during flush
  - Definition:
    - In `Flush`, detect spans of operations that are all INSERTs for the same table and within batch size; otherwise use single-row path.
    - Relevant files: `db_changes/db/dialect_postgres.go`. Confidence: high.
  - Progress:
    - Expected work: Added scan of sorted operations in `Flush` to find contiguous INSERT-only spans per table up to configured batch size; emits debug logs when tracer is enabled, leaving execution on single-row path for now.
    - Unexpected skipped work: None.
    - Unexpected extra work: Guarded pathological batch size (<2) and ensured spans don't overlap in logs.
    - Learnings: Candidate detection is cheap and safe to run under tracer; sets the stage for VALUES/UNNEST builders.
    - Deliberate tech debt: No SQL generation yet; no metrics counters beyond debug logs.

- [x] 4. Compute stable column set (or superset) for a batch
  - Definition:
    - Determine shared column list across rows; if rows differ, either (a) fill missing columns with safe defaults/nulls or (b) split into compatible sub-batches.
    - Relevant files: `db_changes/db/dialect_postgres.go`. Confidence: medium (edge cases on types/defaults).
  - Progress:
    - Expected work: Added `computeInsertBatchPlan` to build a sorted column superset (ensuring PKs included), return escaped columns and per-row aligned values, filling missing fields with `NULL`.
    - Unexpected skipped work: None.
    - Unexpected extra work: Integrated helper in batch detection to log plan readiness under tracer; validated columns exist on table schema.
    - Learnings: Using table `scanType` with existing `normalizeValueType` ensures consistent SQL literal formatting.
    - Deliberate tech debt: Does not yet split into sub-batches on type incompatibilities; relies on upcoming builders to enforce stricter rules.

- [x] 5. Implement batched INSERT using VALUES (...), (...)
  - Definition:
    - Build a single `INSERT INTO tbl(cols) VALUES (...), (...), ...)` statement for the batch using normalized values; preserve per-row order within batch.
    - Relevant files: `db_changes/db/dialect_postgres.go`. Confidence: high.
  - Progress:
    - Expected work: Added `buildValuesInsertSQL` and refactored `Flush` to emit ordered execution segments, forming VALUES batches when mode=values, rows are contiguous INSERTs for the same table, within batch size, and all irreversible.
    - Unexpected skipped work: Skipped reversible rows for now to preserve per-row history guarantees; they fall back to single-row path.
    - Unexpected extra work: Added tracer logs for batch execution and candidate skips.
    - Learnings: Segment builder keeps global ordering intact while enabling batched execution opportunistically.
    - Deliberate tech debt: No CTE-based per-row history emission yet; UNNEST mode not implemented.

- [x] 6. Implement batched INSERT using UNNEST arrays (optional mode)
  - Definition:
    - Build `INSERT INTO tbl(cols) SELECT * FROM unnest($1::type[], ...)` with arrays per column, matching row order; param binding or literal arrays as appropriate.
    - Relevant files: `db_changes/db/dialect_postgres.go`. Confidence: medium (types/arrays casting).
  - Progress:
    - Expected work: Added `buildUnnestInsertSQL` to generate per-column ARRAY[...] with type casts from `ColumnInfo.databaseTypeName`, and extended `Flush` to form UNNEST batches when mode=unnest with irreversible rows.
    - Unexpected skipped work: Skipped parameterized arrays; using literal arrays for simplicity; relies on proper escaping already in normalized values.
    - Unexpected extra work: Reverse-mapped escaped column names to `ColumnInfo` to obtain types.
    - Learnings: Casting arrays to `<type>[]` avoids implicit cast pitfalls across columns.
    - Deliberate tech debt: No CTE history yet; no param binding; minimal validation on type name compatibility.

- [x] 7. Emit per-row history entries for batched insert (CTE)
  - Definition:
    - Use a CTE to insert history rows per row with `op='I'`, `pk`, `prev_value=NULL`, `block_num`, then perform the batched insert; maintain correct ordering.
    - Relevant files: `db_changes/db/dialect_postgres.go`. Confidence: medium.
  - Progress:
    - Expected work: Added `buildInsertHistoryCTE` that constructs a `WITH` CTE inserting one history row per reversible INSERT in the batch, and prepends it to VALUES/UNNEST insert when needed.
    - Unexpected skipped work: `prev_value` is omitted (NULL) for inserts, consistent with single-row path; ordering preserved by executing CTE before batched insert.
    - Unexpected extra work: Ensured table name and PK JSON are escaped safely; only includes rows with non-nil reversible block numbers.
    - Learnings: Using a single CTE keeps atomicity and ordering with minimal SQL overhead.
    - Deliberate tech debt: Metrics not added yet; complex interleaving scenarios handled by prior segmentation.

- [x] 8. Global ordering guarantees across batches
  - Definition:
    - Ensure batching never crosses ordinals where other ops (UPSERT/UPDATE/DELETE) interleave; preserve original `ordinal` sequence semantically.
    - Relevant files: `db_changes/db/dialect_postgres.go`. Confidence: high.
  - Progress:
    - Expected work: Segment builder scans sorted ops and only batches contiguous INSERTs for the same table; other ops force segment boundaries, preserving order.
    - Unexpected skipped work: None.
    - Unexpected extra work: Added tracer logs to make segment boundaries observable.
    - Learnings: Building segments first simplifies order guarantees and later SQL generation.
    - Deliberate tech debt: None.

- [x] 9. Safe fallback to single-row path
  - Definition:
    - If any precondition fails (mixed ops, incompatible columns, type cast issues), fall back to existing per-row queries without failing the flush.
    - Relevant files: `db_changes/db/dialect_postgres.go`. Confidence: high.
  - Progress:
    - Expected work: Wrapped batch execution with SAVEPOINT; on error, rollback to savepoint and execute per-row statements for that segment.
    - Unexpected skipped work: If SAVEPOINT unsupported, we surface the error (transaction aborts) instead of manual per-row replay.
    - Unexpected extra work: Added debug logs on fallback path for diagnosability.
    - Learnings: SAVEPOINT provides a clean rollback boundary without affecting prior segments.
    - Deliberate tech debt: No metrics yet for fallback occurrences.

- [x] 10. Batched UPSERT using VALUES (...)
  - Definition:
    - Extend detection to form contiguous UPSERT-only spans per table. Build `INSERT INTO tbl(cols) VALUES (...), (...) ON CONFLICT (pk...) DO UPDATE SET col=EXCLUDED.col`.
    - Implement per-row history via a CTE: derive op ('I' | 'U') and `prev_value` by left-joining existing rows using the PK over a `src` CTE of the batch rows.
    - Preserve global ordering; fallback to single-row path on any failure.
    - Relevant files: `db_changes/db/dialect_postgres.go`. Confidence: medium-high.
  - Progress:
    - Expected work: Implemented detection in `Flush` for contiguous UPSERT-only spans per table when `mode=values` and not insert-only; added `computeUpsertBatchPlan` enforcing identical column sets and PK presence; added `buildValuesUpsertSQL` generating `INSERT ... VALUES ... ON CONFLICT (pk...) DO UPDATE SET col=EXCLUDED.col`; added `buildUpsertHistoryCTE` to derive per-row op and `prev_value` via a `src` CTE and LEFT JOIN to target; prepends CTE when reversible rows are present; executes batches under SAVEPOINT with per-row fallback on error.
    - Unexpected skipped work: UNNEST-based UPSERT (handled in Task 11) and parameterized arrays not implemented; heterogeneous column sets for UPSERT are not supported (by design, identical sets required).
    - Unexpected extra work: Aligned `saveUpsert` to accept an escaped table name and updated call sites to match; enhanced tracer logs for UPSERT batch detection and execution.
    - Learnings: Enforcing identical column sets preserves single-row UPSERT semantics (only explicitly provided columns are updated) and simplifies deterministic SQL; a single CTE keeps history emission atomic and ordered with minimal overhead.
    - Deliberate tech debt: Metrics/counters (Task 15), unit/integration tests (Tasks 12–13), documentation (Task 16), and UNNEST UPSERT (Task 11) remain pending.

- [x] 11. Batched UPSERT using UNNEST arrays
  - Definition:
    - Build `INSERT INTO tbl(cols) SELECT * FROM unnest(col1_arr::type[], ...) ON CONFLICT (pk...) DO UPDATE SET col=EXCLUDED.col` using per-column arrays.
    - Create a `src` CTE from UNNEST to compute history entries (op and `prev_value`) with a left join to existing table rows; prepend a history CTE before the insert.
    - Ensure correct type casts and row order; fallback safely.
    - Relevant files: `db_changes/db/dialect_postgres.go`. Confidence: medium.
  - Progress:
    - Expected work: Implemented `buildUnnestUpsertSQL` to construct per-column `ARRAY[...]` with `<type>[]` casts via `ColumnInfo.databaseTypeName`, generating `INSERT INTO ... SELECT * FROM unnest(...) ON CONFLICT (pk...) DO UPDATE SET col=EXCLUDED.col`; extended `Flush` to detect contiguous UPSERT-only spans for a table when `mode=unnest`, reuse `computeUpsertBatchPlan` to validate identical column sets and normalize values, and prepend `buildUpsertHistoryCTE` when reversible rows are present; execute under SAVEPOINT with per-row fallback on failure.
    - Unexpected skipped work: Parameterized arrays are not implemented (literal arrays used); heterogeneous column sets for UPSERT remain unsupported by design.
    - Unexpected extra work: Added UNNEST UPSERT detection logs and error diagnostics when SQL build fails; ensured conflict target uses table PKs.
    - Learnings: Casting arrays to concrete `<type>[]` avoids implicit cast ambiguity and keeps UNNEST robust across column types; sharing the UPSERT plan logic across VALUES and UNNEST keeps semantics consistent.
    - Deliberate tech debt: Metrics (Task 15), tests (Tasks 12–13), and documentation (Task 16) still pending; parameterization of arrays left for a later task (Task 17).

- [x] 11a. UNNEST arrays via WITH ORDINALITY + projection indexing
  - Definition:
    - For tables with array-typed columns, avoid flattening these columns. Unnest only scalar columns WITH ORDINALITY to produce an `ord` per row, keep array-typed columns as 2D arrays, and project a per-row 1D array using `arr_col[ord]::type[]` in the SELECT list.
    - Ensure explicit casts use resolved Postgres types (including enums/domains) from table metadata; maintain row order and lengths across all arrays; guard on length mismatches.
    - Provide a safety fallback: when typed projection cannot be constructed safely, skip UNNEST for that batch and use VALUES.
    - Consider an alternative interim path: pass array-typed columns as `text[]` of brace literals and cast in projection to `type[]` when WITH ORDINALITY is not viable.
    - Relevant files: `db_changes/db/dialect_postgres.go`. Confidence: medium-high.
  - Progress:
    - Expected work: Finalized UNNEST handling for array-typed columns by selecting per-row arrays via CASE over `((s.ord)::int)` instead of 2D arrays + indexing. Updated both builders: `buildUnnestInsertSQL` and `buildUnnestUpsertSQL` in `db_changes/db/dialect_postgres.go`.
    - Unexpected skipped work: Abandoned the 2D array projection path (`arr2d[s.ord]`) due to persistent cast issues; rectangular-length enforcement no longer required.
    - Unexpected extra work: Added temporary rectangularity guards and an all-empty special-case during investigation; retained VALUES fallback in `Flush` for any UNNEST build errors. Removed unused `textArrayLiteralLength` helper.
    - Learnings: Even with explicit parentheses and `(s.ord)::int`, Postgres could treat the result of `arr2d[s.ord]` as a scalar in some plans, causing bigint→bigint[] cast errors. CASE-by-ordinal eliminates 2D typing/indexing pitfalls while keeping UNNEST performance for scalar columns. Reference: [PostgreSQL docs: Table Expressions, WITH ORDINALITY](https://www.postgresql.org/docs/current/queries-table-expressions.html).
    - Deliberate tech debt: CASE projections grow with batch size; consider a VALUES CTE keyed by `ord` to reduce SQL size. Observability (Task 15) and tests/benchmarks (Tasks 12–14) remain.

- [x] 11b. UPSERT UNNEST with heterogeneous columns via superset + presence flags
  - Definition:
    - For a contiguous UPSERT span, compute a sorted superset of columns across rows. For each column, produce two arrays: values (normalized SQL literals, using NULL when absent) and presence flags (boolean).
    - Build UNNEST over scalar columns WITH ORDINALITY as today. In projection, include each column value and its presence flag; for array-typed columns, keep the CASE-by-ordinal approach and add a parallel presence flag.
    - In `ON CONFLICT DO UPDATE`, preserve single-row semantics by setting each column as: `col = CASE WHEN col_present THEN EXCLUDED.col ELSE target.col END` so that absent columns do not modify existing values, while explicit NULLs still update to NULL.
    - Derive presence from row membership (whether the field was provided) rather than from value NULL-ness.
    - History CTE via `buildUpsertHistoryCTE` remains unchanged.
    - Relevant files: `db_changes/db/dialect_postgres.go`. Confidence: medium-high.
  - Progress:
    - Expected work: Implemented `computeUpsertSupersetPlanWithPresence` and `buildUnnestUpsertSQLWithPresence`; integrated in `Flush` for `mode=unnest` UPSERT spans. Presence flags control DO UPDATE to preserve single-row semantics. Array-typed columns use CASE-by-ordinal projections.
    - Unexpected skipped work: INSERT path currently uses the superset directly; absent fields become SQL NULL on brand-new rows, bypassing table defaults and potentially violating NOT NULL constraints (e.g., `status`). No catalog-aware default inlining or NOT NULL guard yet.
    - Unexpected extra work: Added presence matrix plumbing and CASE-by-ordinal logic for arrays; expanded logs for detection/execution.
    - Learnings: Presence is sufficient for DO UPDATE, but INSERT requires either default inlining or partitioning to preserve omission semantics. See Tasks 11c and 11d.
    - Deliberate tech debt: Missing default inlining/NOT NULL safeguards; no fallback to VALUES when unsafe; metrics/tests pending.

- [x] 11c. Harden superset INSERT semantics by inlining defaults for absent values
  - Definition:
    - Read column nullability and default expressions (e.g., from `pg_catalog`). In UNNEST/VALUES projection for INSERT, use `CASE WHEN present THEN value ELSE default_expr END` per column so that omitted fields receive table defaults. Preserve explicit NULL updates when presence=true and value=NULL. For NOT NULL columns without defaults, do not UNNEST the batch (fallback to VALUES with identical column sets or per-row).
    - Apply to both scalar and array-typed columns: keep CASE-by-ordinal for arrays and emit default_expr in the ELSE branch per row. Consider a feature flag to enable default inlining due to volatile defaults (e.g., `now()`, `nextval`).
    - Relevant files: `db_changes/db/dialect_postgres.go` (UNNEST/VALUES builders), table metadata loading (extend `TableInfo` with nullability/defaults or fetch lazily/cached).
    - Confidence: medium-high.
  - Progress:
    - Expected work: Implemented. Added `fetchColumnNullabilityDefaults` in `db_changes/db/db.go` to read nullability and `pg_get_expr` defaults; extended `ColumnInfo` with `nullable`, `hasDefault`, `defaultExpr`. Updated `buildUnnestUpsertSQLWithPresence` to inline defaults on INSERT: scalars use `CASE WHEN present THEN typed(value) ELSE default END`; arrays use CASE-by-ordinal wrapped with presence and default to `default_expr::type[]` or empty array.
    - Unexpected skipped work: Did not add a VALUES-mode default inlining (not strictly required); parameterized arrays still deferred (Task 17).
    - Unexpected extra work: Added a safety guard: if any NOT NULL column has no default and is absent in any row, the builder refuses UNNEST superset (error) so `Flush` falls back to VALUES or per-row.
    - Learnings: Guarding before SQL assembly avoids NOT NULL violations and preserves default semantics for brand-new rows; presence remains correct for DO UPDATE.
    - Deliberate tech debt: VALUES-mode default inlining and metrics/tests remain pending.

- [ ] 11d. Optional: Partition UNNEST batches by identical column sets
  - Definition:
    - Partition a heterogeneous UPSERT span into sub-batches where all rows share the exact same present column set. For each sub-batch, omit absent columns from the INSERT target so table defaults apply naturally; keep UNNEST for the sub-batch’s columns only. Maintain global ordering by emitting multiple segments. Fallback to VALUES/single when partitions would be too small to justify UNNEST.
    - Relevant files: `db_changes/db/dialect_postgres.go` (segment builder and UNNEST planner).
    - Confidence: high.
  - Progress:
    - Expected work:
    - Unexpected skipped work:
    - Unexpected extra work:
    - Learnings:
    - Deliberate tech debt:

- [ ] 12. Unit tests for SQL generation and column normalization
  - Definition:
    - Cover VALUES and UNNEST builders, column superset logic, and value normalization; verify stable, deterministic SQL.
    - Relevant files: `db_changes/db/dialect_postgres.go` (tests alongside), new `_test.go` files. Confidence: medium-high.
  - Progress:
    - Expected work:
    - Unexpected skipped work:
    - Unexpected extra work:
    - Learnings:
    - Deliberate tech debt:

- [ ] 13. Integration tests against Postgres
  - Definition:
    - Ingest sample batches with/without mixed columns; verify data, history entries, and ordering; include fallback scenarios.
    - Relevant files: test harness setup under `db_changes/db/` tests. Confidence: medium.
  - Progress:
    - Expected work:
    - Unexpected skipped work:
    - Unexpected extra work:
    - Learnings:
    - Deliberate tech debt:

- [ ] 14. Benchmarks: single-row vs VALUES vs UNNEST
  - Definition:
    - Measure planning/execution times and throughput for various batch sizes; compare with TigerData findings on UNNEST advantages.
    - Relevant files: benchmark scripts (new), optional Go benchmarks. Confidence: medium.
  - Progress:
    - Expected work:
    - Unexpected skipped work:
    - Unexpected extra work:
    - Learnings:
    - Deliberate tech debt:

- [ ] 15. Observability: counters and logs
  - Definition:
    - Expose metrics: batches formed, rows per batch, fallback occurrences, mode used (values/unnest), errors; add debug tracing of generated SQL when tracer enabled.
    - Relevant files: `db_changes/db/dialect_postgres.go`, logging facilities. Confidence: high.
  - Progress:
    - Expected work:
    - Unexpected skipped work:
    - Unexpected extra work:
    - Learnings:
    - Deliberate tech debt:

- [ ] 16. Documentation
  - Definition:
    - Document flags, safety constraints, trade-offs, and when to choose VALUES vs UNNEST; reference TigerData article and ClickHouse prior art.
    - Relevant files: README/docs (new or existing), CLI help in `common_flags.go`. Confidence: high.
  - Progress:
    - Expected work:
    - Unexpected skipped work:
    - Unexpected extra work:
    - Learnings:
    - Deliberate tech debt:

- [ ] 17. Optional: Parameterize UNNEST arrays (avoid literal arrays)
  - Definition:
    - Switch UNNEST mode to use bound parameters/driver array types instead of literal `ARRAY[...]` to reduce SQL size and improve safety. Ensure proper per-column casts and ordering are preserved.
    - Relevant files: `db_changes/db/dialect_postgres.go` (UNNEST builder), potential driver integration. Confidence: medium.
  - Progress:
    - Expected work:
    - Unexpected skipped work:
    - Unexpected extra work:
    - Learnings:
    - Deliberate tech debt:



