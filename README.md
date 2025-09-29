# Substreams:SQL Sink

The Substreams:SQL sink helps you quickly and easily sync Substreams modules to a PostgreSQL or Clickhouse database.

It supports two different Substreams output formats, each with distinct advantages:

### Relational Mappings (Recommended)

Tables and rows are extracted dynamically from Protobuf messages using annotations for table and relation mappings. This approach leverages Substreams' built-in relational mapping capabilities.

**Pros:**
- **Less development work** - No need to manually emit database changes in your Substreams code
- **Automatic schema inference** - Tables and relationships are derived from your Protobuf definitions
- **Type safety** - Protobuf annotations ensure data consistency
- **Easier maintenance** - Schema changes are managed through Protobuf definitions
- **Faster in most scenarios** - Due to possibility to perform bulk inserts more easily

**Cons:**
- **Less control** - Limited flexibility in how data is structured in the database
- **Insert Only** - Does not support update/delete of rows, it's an insert-only method of ingestion

### Database Changes

Tables and rows are extracted from Substreams modules that directly emit database changes using [`sf.substreams.sink.database.v1.DatabaseChanges`](https://github.com/streamingfast/substreams-sink-database-changes?tab=readme-ov-file#substreams-sink-database-changes).

**Pros:**
- **Full control** - Complete flexibility over database structure and operations
- **Custom logic** - Can implement complex business logic for data transformation

**Cons:**
- **More development work** - Requires manually implementing database change logic
- **Error-prone** - More opportunities for bugs in manual database operations
- **Maintenance overhead** - Schema changes require code updates

## Quickstart

### Prerequisites

1. Install `substreams-sink-sql` from Brew with `brew install streamingfast/tap/substreams-sink-sql` or by using the pre-built binary release [available in the releases page](https://github.com/streamingfast/substreams-sink-sql/releases) (extract `substreams-sink-sql` binary into a folder and ensure this folder is referenced globally via your `PATH` environment variable).

2. Start Docker Compose in the background:

   ```bash
   docker compose up -d
   ```

   > You can wipe the database and restart from scratch by doing `docker compose down` and `rm -rf ./devel/data/postgres`.

3. Set up environment variables for convenience:

   ```bash
   export PG_DSN="psql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node?sslmode=disable"
   export CLICKHOUSE_DSN="clickhouse://default:default@localhost:9000/default"
   ```

   > **Note** To connect to Substreams you will need an authentication token, follow this [guide](https://substreams.streamingfast.io/reference-and-specs/authentication) to obtain one.

## Quickstart Relational Mappings

### Postgres

```bash
substreams-sink-sql from-proto $PG_DSN solana-spl-token@v0.1.3
```

### Clickhouse

```bash
substreams-sink-sql from-proto $CLICKHOUSE_DSN solana-spl-token@v0.1.3
```

## Quickstart Database Changes

### Postgres

```bash
substreams-sink-sql setup $PG_DSN substreams-template@v0.3.1
substreams-sink-sql run $PG_DSN substreams-template@v0.3.1
```

### Clickhouse

```bash
substreams-sink-sql setup $CLICKHOUSE_DSN substreams-template@v0.3.1
substreams-sink-sql run $CLICKHOUSE_DSN substreams-template@v0.3.1
```

### Sink Config

The `substreams-sink-sql` uses the "Sink Config" section of your Substreams manifest to configure the sink behavior:

```yaml
sink:
   module: db_out
   type: sf.substreams.sink.sql.v1.Service
   config:
      schema: "./schema.sql"
```

This configuration tells `substreams-sink-sql`:
- **module**: Which output module to consume (typically `db_out`)
- **type**: The sink service type (`sf.substreams.sink.sql.v1.Service`)
- **config.schema**: Path to the SQL schema file used during the `setup` step

The schema file should contain `CREATE TABLE IF NOT EXISTS` statements to ensure idempotent database setup.

### Network

Your Substreams manifest defines which network to connect to by default. For example, a manifest configured for `mainnet` will connect to the `mainnet.eth.streamingfast.io:443` endpoint automatically. 

You can override the default endpoint in two ways:
- **Command line flag**: Use `-e another.endpoint:443` when running the sink
- **Environment variable**: Set `SUBSTREAMS_ENDPOINTS_CONFIG_<NETWORK>` where `<NETWORK>` is the network name from your manifest in uppercase

For example, to override the mainnet endpoint: `export SUBSTREAMS_ENDPOINTS_CONFIG_MAINNET=custom.endpoint:443`

### DSN

DSN stands for Data Source Name (or Database Source Name) and `substreams-sink-sql` expects a URL input that defines how to connect to the right driver. An example input for Postgres is `psql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node?sslmode=disable` which lists hostname, user, password, port and database (with some options) in a single string input.

The URL's scheme is used to determine the driver to use, `psql`, `clickhouse`, etc. In the example case above, the picked driver will be Postgres. The generic format of a DSN is of the form:

```
<scheme>:://<username>:<password>@<hostname>:<port>/<database_name>?<options>
```

You will find below connection details for each currently supported driver.

#### Clickhouse

The DSN format for Clickhouse is:

```
clickhouse://<user>:<password>@<host>:<port>/<dbname>[?<options>]
```

> [!IMPORTANT]
> You are using Clickhouse Cloud? Add `?secure=true` option to your DSN otherwise you will receive weird error like `setup: exec schema: exec schema: read: EOF`. Here a DSN example for Clickhouse Cloud `clickhouse://default:<password>@<instance-id>.clickhouse.cloud:9440/default?secure=true`.
>
> Make sure also that you are using the _Native protocol SSL/TLS_ port which is usually set at 9440.

#### PostgreSQL

The DSN format for Postgres is:

```
psql://<user>:<password>@<host>:<port>/<dbname>[?<options>]
```

Where `<options>` is URL query parameters in `<key>=<value>` format, multiple options are separated by `&` signs. Supported options can be seen [on libpq official documentation](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS). The options `<user>`, `<password>`, `<host>` and `<dbname>` should **not** be passed in `<options>` as they are automatically extracted from the DSN URL.

##### Schema Isolation

The `schemaName` option key can be used to select a particular schema within the `<dbname>` database. This is **the recommended approach for running multiple substreams to different schemas on the same PostgreSQL database**.

> **Note**
> `schemaName` is a custom option handled by `substreams-sink-sql` and is not passed to PostgreSQL. It instructs the sink to operate within the specified schema and automatically sets the correct schema context for user SQL scripts.

**Example DSNs for multiple substreams:**
```bash
# Ethereum mainnet substreams using 'ethereum' schema
export DSN_ETHEREUM="psql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node?sslmode=disable&schemaName=ethereum"

# Polygon mainnet substreams using 'polygon' schema
export DSN_POLYGON="psql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node?sslmode=disable&schemaName=polygon"

# BSC mainnet substreams using 'bsc' schema
export DSN_BSC="psql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node?sslmode=disable&schemaName=bsc"
```

Each substreams instance will:
- Create its own isolated schema (namespace) within the same database
- Have its own cursor and history tables within that schema
- Execute user SQL scripts with the correct schema context automatically set

This allows you to efficiently manage multiple substreams data pipelines from different networks using a single PostgreSQL database while maintaining complete data isolation between networks.

#### Others

Only `psql` and `clickhouse` are supported today, adding support for a new _dialect_ is quite easy:

- Copy [db/dialect_clickhouse.go](db_changes/db/dialect_clickhouse.go) to a new file `db/dialect_<name>.go` implementing the right functionality.
- Update [`db.driverDialect` map](https://github.com/streamingfast/substreams-sink-sql/blob/develop/db/dialect.go#L27-L31) to add you dialect (key is the Golang type of your dialect implementation).
- Update [`dsn.driverMap` map](https://github.com/streamingfast/substreams-sink-sql/blob/develop/db/dsn.go#L27-L31) to add DSN -> `dialect name` mapping, edit the file to accommodate for your specific driver (might not be required)
- Update Docker Compose to have this dependency auto-started for development purposes
- Update README and CHANGELOG to add information about the new dialect
- Open a PR

### Output Module Requirements

The `substreams-sink-sql` accepts two types of Substreams output modules:

#### Database Changes Modules

For the **Database Changes** approach, your module output type must be [`sf.substreams.sink.database.v1.DatabaseChanges`](https://github.com/streamingfast/substreams-sink-database-changes/blob/develop/proto/sf/substreams/sink/database/v1/database.proto#L7). 

**Development Resources:**
- **Rust**: Use the [`substreams-database-change`](https://github.com/streamingfast/substreams-database-change) crate for bindings and helpers
- **Examples**: See [`substreams-eth-block-meta`](https://github.com/streamingfast/substreams-eth-block-meta/blob/master/src/lib.rs#L35) and its [db_out.rs helper](https://github.com/streamingfast/substreams-eth-block-meta/blob/master/src/db_out.rs#L6)

By convention, the module that emits `DatabaseChanges` is named `db_out`.

#### Relational Mappings Modules

For the **Relational Mappings** approach, your module can output any Protobuf message type. The sink automatically extracts table and row data from your Protobuf messages using annotations and field mappings.

**Examples:**
- **Solana SPL Token**: [`solana-spl-token@v0.1.3`](https://github.com/streamingfast/substreams-spl-token) - demonstrates relational mapping extraction from SPL token data

#### ClickHouse Table Options

When using the **Relational Mappings** approach with ClickHouse, you must configure `clickhouse_table_options` in your Protobuf message annotations. This is required because ClickHouse needs specific table engine parameters.

**Required Configuration:**

```proto
message TokenInteraction {
  option (schema.table) = {
    name: "token_interactions"
    clickhouse_table_options: {
      order_by_fields: [
        { name: "instruction_id" }
      ]
    }
  };

  string instruction_id = 1 [(schema.field) = { primary_key: true }];
  string token_address = 2;
  uint64 amount = 3;
  // ... other fields
}
```

**Available Options:**

- **`order_by_fields`** (required): Defines the ORDER BY clause for the ClickHouse table. At least one field is required.
- **`partition_fields`** (optional): Defines custom PARTITION BY fields. If not specified, defaults to partitioning by `_block_timestamp_` using `toYYYYMM()`.
- **`replacing_fields`** (optional): Additional fields for the ReplacingMergeTree engine beyond the default `_version` field.
- **`index_fields`** (optional): Defines secondary indexes for the table.

**Advanced Example:**

```proto
message Transfer {
  option (schema.table) = {
    name: "transfers"
    clickhouse_table_options: {
      order_by_fields: [
        { name: "block_number" },
        { name: "transaction_hash" }
      ]
      partition_fields: [
        { name: "_block_timestamp_", function: toYYYYMM }
      ]
      index_fields: [
        {
          name: "from_idx"
          field_name: "from_address"
          type: set
          granularity: 1
        }
      ]
    }
  };

  uint64 block_number = 1;
  string transaction_hash = 2;
  string from_address = 3;
  string to_address = 4;
  string amount = 5;
}
```

**Common Error:**
If you see an error like `clickhouse table options not set for table "your_table"`, it means you need to add the `clickhouse_table_options` configuration to your Protobuf message as shown above.

### Protobuf models

- protobuf bindings are generated using `buf generate` at the root of this repo. See https://buf.build/docs/installation to install buf.

### Advanced Topics

#### High Throughput Injection

> [!IMPORTANT]
> This method will be useful if you insert a lot of data into the database. If the standard ingestion speed satisfy your needs, continue to use it, the steps below are an advanced use case.

The `substreams-sink-sql` contains a fast injection mechanism for cases where big data needs to be dump into the database. In those cases, it may be preferable to dump every files to CSV and then use `COPYFROM` to transfer data super quick to Postgres.

The idea is to first dump the Substreams data to `CSV` files using `substreams-sink-sql generate-csv` command:

```bash
substreams-sink-sql generate-csv "psql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node?sslmode=disable" --output-dir ./data/tables :14490000
```

> [!NOTE]
> We are using 14490000 as our stop block, pick you stop block close to chain's HEAD or smaller like us to perform an experiment, adjust to your needs.

This will generate block segmented CSV files for each table in your schema inside the folder `./data/tables`. Next step is to actually inject those CSV files into your database. You can use `psql` and inject directly with it.

We offer `substreams-sink-sql inject-csv` command as a convenience. It's a per table invocation but feel free to run each table concurrently, your are bound by your database as this point, so it's up to you to decide you much concurrency you want to use. Here a small `Bash` command to loop through all tables and inject them all

```bash
for i in `ls ./data/tables | grep -v state.yaml`; do \
  substreams-sink-sql inject-csv "psql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node?sslmode=disable" ./data/tables "$i" :14490000; \
  if [[ $? != 0 ]]; then break; fi; \
done
```

Those files are then inserted in the database efficiently by doing a `COPY FROM` and reading the data from a network pipe directly.

The command above will also pick up the `cursors` table injection as it's a standard table to write. The table is a bit special as it contains a single file which is contains the `cursor` that will handoff between CSV injection and going back to "live" blocks. It's extremely important that you validate that this table has been properly populated. You can do this simply by doing:

```bash
substreams-sink-sql tools --dsn="psql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node?sslmode=disable" cursor read
Module eaf2fc2ea827d6aca3d5fee4ec9af202f3d1b725: Block #14490000 (61bd396f3776f26efc3f73c44e2b8be3b90cc5171facb1f9bdeef9cb5c4fd42a) [cqR8Jx...hxNg==]
```

This should emit a single line, the `Module <hash>` should fit the for `db_out` (check `substreams info <spkg>` to see your module's hashes) and the block number should fit your last block you written.

> [!WARNING]
> Failure to properly populate will 'cursors' table will make the injection starts from scratch when you will do `substreams-sink-sql run` to bridge with "live" blocks as no cursor will exist so we will start from scratch.

Once data has been injected and you validated the `cursors` table, you can then simply start streaming normally using:

```bash
substreams-sink-sql run "psql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node?sslmode=disable" <spkg>
```

This will start back at the latest block written and will start to handoff streaming to a "live" blocks.

##### Performance Knobs

When generating the CSV files, optimally choosing the `--buffer-max-size` configuration value can drastically increase your write throughput locally but even more if your target store is an Amazon S3, Google Cloud Storage or Azure bucket. The flag controls how many bytes of the files is to be held in memory. By having bigger amount of buffered bytes, data is transferred in big chunk to the storage layer leading to improve performance. In lots of cases, the full file can be held in memory leading to a single "upload" call being performed having even better performance.

When choosing this value you should consider 2 things:

- One buffer exist by table in your schema, so if there is 12 tables and you have a 128 MiB buffer, you could have up to 1.536 GiB (`128 MiB * 12`) of RAM allocated to those buffers.
- Amount of RAM you want to allocate.

Let's take a container that is going to have 8 GiB of RAM. We suggest leaving 512 MiB for other part of the `generate-csv` tasks, which mean we could dedicated 7.488 GiB to buffering. If your schema has 10 tables, you should use `--buffer-max-size=785173709` (`7.488 GiB / 10 = 748.8 MiB = 785173709`).
