# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v2.3.3

### Fixed

* Batches written to the database now respects the insertion ordering has received from your Substreams. This fixes for example auto-increment to be as defined on the chain.

## v2.3.2

### Fixed

* Fixed problem where string had unicode character and caused `pq: invalid message format`.

## v2.3.1

### Fixed

* The `substreams-sink-postgres setup` command has been fixed to use the correct schema defined by the DSN.

* The `cursors` table suggestion when the table is not found has been updated to be in-sync with table used in `substreams-sink-postgres setup`.

### Changed

* Now using Go Protobuf generate bindings from https://github.com/streamingfast/substreams-sink-database-changes.

## v2.3.0

### Added

* Added `Composite keys` support following the update in `substreams-database-change`

  The code was updated to use `oneOf` primary keys (pk and composite) to keep backward compatibility. Therefore, Substreams using older versions of `DatabaseChange` can still use newer versions of `postgres-sink` without problems. To use composite key, define your schema to use Postgres composite keys, update to latest version of `substreams-database-changes` and update your code to send a `CompositePrimaryKey` key object for the `primary_key` field of the `TableChange` message.

* Added escape to value in case the postgres data type is `BYTES`. We now escape the byte array.

### Fixed

* Added back support for old Substreams Database Change Protobuf package id `sf.substreams.database.v1.DatabaseChanges`.

## v2.2.1

### Changed

* Reduced the amount of allocations and escaping performed which should increase ingestion speed, this will be more visible for Substreams where a lot of entities and columns are processed.

### Fixed

* The `schema` is correctly respected now for the the `cursors` table.

## v2.2.0

### Highlights

#### Cursor Bug Fix

It appeared that the cursor was not saved properly until the first graceful shutdown of `substreams-sink-postgres`. Furthermore, the on exit save was actually wrong because it was saving the cursor without flushing accumulated data which is wrong (e.g. that we had N blocks in memory unflushed and a cursor, and we were saving this cursor to the database without having flushed the in memory logic).

This bug has been introduced in v2.0.0 by mistake which means if we synced a new database with v2.0.0+, there is a good chance your are actually missing some data in your database. It's highly recommended that you re-synchronize your database from scratch.

> **Note** If your are using the same `.spkg` that you are using right now, database ingestion from scratch should go at very high speed because you will be reading from previously cached output, so the bottleneck should be network and the database write performance.

#### Behavior on `.spkg` update

In the release, we change a big how cursor is associated to the `<module>`'s hash in the database and how it's stored.

Prior this version, when loading the cursor back from the database on restart, we were retrieving the cursor associated to the `<module>`'s hash received by `substreams-sink-postgres run`. The consequence of that is that if you change the `.spkg` version you were sinking with, on restart we would find no cursor since the module's hash of this new `.spkg` would have changed and which you mean a full sync back would be happening because we would start without a cursor.

This silent behavior is problematic because it could seen like the cursor was lost somehow while actually, we just picked up a new one from scratch because the `.spkg` changed.

This release brings in a new flag `substreams-sink-postgres run --on-module-hash-mistmatch=error` (default value shown) where it would control how we should react to a changes in the module's hash since last run.

- If `error` is used (default), it will exit with an error explaining the problem and how to fix it.
- If `warn` is used, it does the same as 'ignore' but it will log a warning message when it happens.
- If `ignore` is set, we pick the cursor at the highest block number and use it as the starting point. Subsequent updates to the cursor will overwrite the module hash in the database.

There is a possibility that multiple cursors exists in your database, hence why we pick the one with the highest block. If it's the case, you will be warned that multiple cursors exists. You can run `substreams-sink-postgres tools cursor cleanup <manifest> <module> --dsn=<dsn>` which will delete now useless cursors.

The `ignore` value can be used to change to a new `.spkg` while retaining the previous data in the database, the database schema will start to be different after a certain point where the new `.spkg` became active.

### Added

* Added `substreams-sink-postgres run --on-module-hash-mistmatch=error` to control how a change in module's hash should be handled.

### Changed

* Changed behavior of how cursor are retrieved on restart.

### Fixed

* Fixed `cursor` not being saved correctly until the binary exits.

* Fixed wrong handling of updating the cursor, we were not checking if a row was updated when doing the flush operation.

* Fixed a bug where it was possible if the sink was terminating to write a cursor for data not yet flushed. This was happening if the `substreams-sink-postgres run` was stopped before we ever written a cursor, which normally happens each 1000 blocks. We don't expect anybody to have been hit by this but if you are unsure, you should check data for the 1000 first blocks of you sink (for example from 11 000 000 to 11 001 000 if your module start block was 11 000 000).

## v2.1.0

### Changed

* Column's schema type that are not known by the `sql` library we know will now be transferred as-is to the database.

  There is a lot of column's for which the `sql` library we use have to Go representation for by default. This is the case for example for the `numeric` column's type. Previously, this would be reported directly as an error, Now, we pass the received value from your Substreams unmodified to the database engine. It will be your responsibility to send the data in the right format accepted by the database. We send the value as-is, without escaping and without sanitization, so this is a risk if you don't control the Substreams.

### Added

* When doing `substreams-sink-postgres run`, the `<manifest>` argument now accepts directory like `.`.

### Fixed

* Fixed timestamp received in RFC3339 format.

## v2.0.2

### Changed

- Diminish amount of allocations done to perform fields transformation.

### Fixed

- Fixed some places where escaping for either identifier or value was not done properly.

- Fixed double escaping of boolean values.

## v2.0.1

### Added

- Added proper escaping for table & column names to allow keyword column names to use keywords as column names such as `to` and `from` etc.

## v2.0.0

### Highlights

This release drops support for Substreams RPC protocol `sf.substreams.v1` and switch to Substreams RPC protocol `sf.substreams.rpc.v2`. As a end user, right now the transition is seamless. All StreamingFast endpoints have been updated to to support the legacy Substreams RPC protocol `sf.substreams.v1` as well as the newer Substreams RPC protocol `sf.substreams.rpc.v2`.

Support for legacy Substreams RPC protocol `sf.substreams.v1` is expected to end by June 6 2023. What this means is that you will need to update to at least this release if you are running `substreams-sink-postgres` in production. Otherwise, after this date, your current binary will stop working and will return errors that `sf.substreams.v1.Blocks` is not supported on the endpoint.

From a database and operator standpoint, this binary is **fully** backward compatible with your current schema. Updating to this binary will continue to sink just like if you used a prior release.

#### Retryable Errors

The errors coming from Postgres are **not** retried anymore and will stop the binary immediately.

#### Operators

If you were using environment variable to configure the binary, note that the environment prefix has changed from `SINK_` to `SINK_POSTGRES_`.

### Changed

- **Deprecated** The flag `--irreversible-only` is deprecated, use `--final-blocks-only` instead.

### Added


- Added command `substreams-sink-postgres tools --dsn <dsn> cursor read` to read the current cursors stored in your database.

- **Dangerous** Added command `substreams-sink-postgres tools --dsn <dsn> cursor write <module_hash> <cursor>` to update the cursor in your database for the given `<module_hash>`

    > **Warning** This is a destructive operation, be sure you understand the consequences of updating the cursor.

- **Dangerous** Added command `substreams-sink-postgres tools --dsn <dsn> cursor delete [<module_hash>|--all]` to delete the cursor associated with the given module's hash or all cursors if `--all` is used.

    > **Warning** This is a destructive operation, be sure you understand the consequences of updating the cursor.

## v1.0.0

### Highlights

This is the latest release before upgrading to Substreams RPC v2.

### Added

- Added `--infinite-retry` to never exit on error and retry indefinitely instead.

- Added `--development-mode` to run in development mode.

    > **Warning** You should use that flag for testing purposes, development mode drastically reduce performance you get from the server.

- Added `--irreversible-only` to only deal with final (irreversible) blocks.

