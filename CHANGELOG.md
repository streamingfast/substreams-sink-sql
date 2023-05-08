# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

