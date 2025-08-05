# Substreams:SQL Sink

The Substreams:SQL sink helps you quickly and easily sync Substreams modules to a PostgreSQL, RisingWave, or ClickHouse database.

### Quickstart

1. Install `substreams-sink-sql` from Brew with `brew install streamingfast/tap/substreams-sink-sql` or by using the pre-built binary release [available in the releases page](https://github.com/streamingfast/substreams-sink-sql/releases) (extract `substreams-sink-sql` binary into a folder and ensure this folder is referenced globally via your `PATH` environment variable).

1. Compile the [Substreams](./docs/tutorial/substreams.yaml) tutorial project:

   ```bash
   cd docs/tutorial
   cargo build --target wasm32-unknown-unknown --release
   cd ../..
   ```

   This creates the following WASM file: `target/wasm32-unknown-unknown/release/substreams_postgresql_sink_tutorial.wasm`

1. Start Docker Compose in the background:

   ```bash
   docker compose up -d
   ```

   This will start PostgreSQL, ClickHouse, and RisingWave services. Individual services can be started with:
   ```bash
   # Start only PostgreSQL
   docker compose up -d postgres

   # Start only RisingWave  
   docker compose up -d risingwave

   # Start only ClickHouse
   docker compose up -d database
   ```

   > You can wipe the databases and restart from scratch by doing `docker compose down` and `rm -rf ./devel/data/`.

1. Run the setup command:

   **Postgres**

   ```bash
   export DSN="postgres://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node?sslmode=disable"
   substreams-sink-sql setup $DSN docs/tutorial/substreams.yaml
   ```

   **RisingWave**

   ```bash
   export DSN="risingwave://root:@localhost:4566/dev?schema=public"
   substreams-sink-sql setup $DSN docs/tutorial/substreams.risingwave.yaml
   ```

   > **Note** RisingWave's dashboard is available at http://localhost:5691 when using Docker Compose. The default user for the playground mode is `root` with no password.

   **ClickHouse**

   ```bash
   export DSN="clickhouse://default:@localhost:9000/default"
   substreams-sink-sql setup $DSN docs/tutorial/substreams.yaml
   ```

   This will connect to the database and create the schema, using the values from `sink.config.schema`

   > **Note** For the sake of idempotence, we recommend that the schema file only contain `create (...) if not exists` statements.

1. Run the sink

   Now that the code is compiled and the database is set up, let launch the `sink` process.

   > **Note** To connect to Substreams you will need an authentication token, follow this [guide](https://substreams.streamingfast.io/reference-and-specs/authentication) to obtain one.

   ```shell
   substreams-sink-sql run $DSN docs/tutorial/substreams.yaml
   ```

### Sink Config

Observe the "Sink Config" section of the [Substreams manifest in the tutorial](docs/tutorial/substreams.yaml#L39-L49):

```yaml
sink:
   module: db_out
   type: sf.substreams.sink.sql.v1.Service
   config:
      schema: "./schema.sql"
```

This is used by `substreams-sink-sql` to gather all required information about how to run and configure the sink, namely the output `module`, what service is desired, `sf.substreams.sink.sql.v1.Service` here and the config that in case of Substreams:SQL contains the schema file to populate the database on `substreams-sink-sql setup` step.

### Network

The [Substreams manifest in the tutorial](docs/tutorial/substreams.yaml#L37) defines on which network by default this is going to run.   This will connect to the `mainnet.eth.streamingfast.io:443` endpoint, because it is the default endpoint for the `mainnet` network. You can change this either by using the endpoint flag `-e another.endpoint:443` or by setting the environment variable `SUBSTREAMS_ENDPOINTS_CONFIG_MAINNET` to that endpoint. The last part of the environment variable is the name of the network in the manifest, in uppercase.

### DSN

DSN stands for Data Source Name (or Database Source Name) and `substreams-sink-sql` expects a URL input that defines how to connect to the right driver. An example input for Postgres is `psql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node?sslmode=disable` which lists hostname, user, password, port and database (with some options) in a single string input.

The URL's scheme is used to determine the driver to use, `psql`, `risingwave`, `clickhouse`, etc. In the example case above, the picked driver will be Postgres. The generic format of a DSN is of the form:

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

Moreover, the `schema` option key can be used to select a particular schema within the `<dbname>` database.

#### RisingWave

The DSN format for RisingWave is:

```
risingwave://<user>:<password>@<host>:<port>/<dbname>[?<options>]
```

RisingWave is a PostgreSQL-compatible streaming database, so it uses similar connection parameters as PostgreSQL. The default port for RisingWave is typically `4566`. Supported options are similar to PostgreSQL since RisingWave implements the PostgreSQL wire protocol.

**Example DSNs:**
```bash
# Local RisingWave instance
risingwave://root:@localhost:4566/dev?schema=public

# RisingWave with authentication
risingwave://username:password@risingwave-host:4566/database?schema=substreams

# RisingWave with SSL (if configured)
risingwave://user:pass@host:4566/db?schema=public&sslmode=require
```

> [!NOTE]
> RisingWave optimizes SQL for streaming workloads and provides real-time materialized views. While PostgreSQL-compatible, it uses RisingWave-specific data type mappings and SQL optimizations for better streaming performance.

#### Others

Currently supported drivers are `psql` (PostgreSQL), `risingwave` (RisingWave), and `clickhouse` (ClickHouse). Adding support for a new _dialect_ is quite easy:

- Copy [db/dialect_clickhouse.go](db_changes/db/dialect_clickhouse.go) to a new file `db/dialect_<name>.go` implementing the right functionality.
- Update [`db.driverDialect` map](https://github.com/streamingfast/substreams-sink-sql/blob/develop/db/dialect.go#L27-L31) to add you dialect (key is the Golang type of your dialect implementation).
- Update [`dsn.driverMap` map](https://github.com/streamingfast/substreams-sink-sql/blob/develop/db/dsn.go#L27-L31) to add DSN -> `dialect name` mapping, edit the file to accommodate for your specific driver (might not be required)
- Update Docker Compose to have this dependency auto-started for development purposes
- Update README and CHANGELOG to add information about the new dialect
- Open a PR

### Output Module

To be accepted by `substreams-sink-sql`, your module output's type must be a [sf.substreams.sink.database.v1.DatabaseChanges](https://github.com/streamingfast/substreams-sink-database-changes/blob/develop/proto/sf/substreams/sink/database/v1/database.proto#L7) message. The Rust crate [substreams-data-change](https://github.com/streamingfast/substreams-database-change) contains bindings and helpers to implement it easily. Some project implementing `db_out` module for reference:

- [substreams-eth-block-meta](https://github.com/streamingfast/substreams-eth-block-meta/blob/master/src/lib.rs#L35) (some helpers found in [db_out.rs](https://github.com/streamingfast/substreams-eth-block-meta/blob/master/src/db_out.rs#L6))

By convention, we name the `map` module that emits [sf.substreams.sink.database.v1.DatabaseChanges](https://github.com/streamingfast/substreams-sink-database-changes/blob/develop/proto/sf/substreams/sink/database/v1/database.proto#L7) output `db_out`.

> Note that using prior versions (0.2.0, 0.1.\*) of `substreams-database-change`, you have to use `substreams.database.v1.DatabaseChanges` in your `substreams.yaml` and put the respected version of the `spkg` in your `substreams.yaml`

### RisingWave Integration

RisingWave is a cloud-native streaming database designed for real-time analytics. It provides several advantages when used with Substreams:

#### Streaming-First Architecture
- **Real-time Materialized Views**: RisingWave automatically maintains materialized views as new data arrives from Substreams
- **Incremental Computation**: Efficiently processes only new/changed data rather than recomputing entire datasets
- **SQL-based Stream Processing**: Use standard SQL to define complex analytics on streaming blockchain data

#### Data Type Optimization
RisingWave uses optimized data types for blockchain data:
- `NUMERIC` for large unsigned integers (uint64)
- `TIMESTAMPTZ` for blockchain timestamps with timezone support
- `BYTEA` with hex encoding for blockchain addresses and hashes
- `VARCHAR` instead of `TEXT` for better performance on indexed string fields

#### Setup Example
```bash
# Start RisingWave (example with Docker)
docker run -d --name risingwave \
  -p 4566:4566 \
  -p 5691:5691 \
  risingwavelabs/risingwave:latest \
  playground

# Run substreams-sink-sql with RisingWave
export DSN="risingwave://root:@localhost:4566/dev?schema=public"
substreams-sink-sql setup $DSN your-substreams.yaml
substreams-sink-sql run $DSN your-substreams.yaml
```

#### Performance Considerations
- RisingWave excels at **append-heavy workloads** typical in blockchain data
- **Materialized views** can pre-aggregate data for fast analytical queries
- **Horizontal scaling** is built-in for handling high-throughput Substreams
- Use **streaming joins** to combine data from multiple Substreams modules in real-time

> [!TIP]
> For optimal performance with RisingWave, design your Substreams output to minimize updates and maximize inserts, leveraging RisingWave's streaming-first architecture.

### Protobuf models

- protobuf bindings are generated using `buf generate` at the root of this repo. See https://buf.build/docs/installation to install buf.

### Advanced Topics

#### High Throughput Injection

> [!IMPORTANT]
> This method will be useful if you insert a lot of data into the database. If the standard ingestion speed satisfy your needs, continue to use it, the steps below are an advanced use case.

The `substreams-sink-sql` contains a fast injection mechanism for cases where big data needs to be dump into the database. In those cases, it may be preferable to dump every files to CSV and then use `COPYFROM` to transfer data super quick to Postgres.

The idea is to first dump the Substreams data to `CSV` files using `substreams-sink-sql generate-csv` command:

**PostgreSQL:**
```bash
substreams-sink-sql generate-csv "psql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node?sslmode=disable" --output-dir ./data/tables :14490000
```

**RisingWave:**
```bash
substreams-sink-sql generate-csv "risingwave://root:@localhost:4566/dev?schema=public" --output-dir ./data/tables :14490000
```

> [!NOTE]
> RisingWave's streaming architecture makes it particularly well-suited for high-throughput injection scenarios. Its append-optimized design can handle large CSV imports efficiently while maintaining real-time query performance.

> [!NOTE]
> We are using 14490000 as our stop block, pick you stop block close to chain's HEAD or smaller like us to perform an experiment, adjust to your needs.

This will generate block segmented CSV files for each table in your schema inside the folder `./data/tables`. Next step is to actually inject those CSV files into your database. You can use `psql` and inject directly with it.

We offer `substreams-sink-sql inject-csv` command as a convenience. It's a per table invocation but feel free to run each table concurrently, your are bound by your database as this point, so it's up to you to decide you much concurrency you want to use. Here a small `Bash` command to loop through all tables and inject them all

**PostgreSQL:**
```bash
for i in `ls ./data/tables | grep -v state.yaml`; do \
  substreams-sink-sql inject-csv "psql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node?sslmode=disable" ./data/tables "$i" :14490000; \
  if [[ $? != 0 ]]; then break; fi; \
done
```

**RisingWave:**
```bash
for i in `ls ./data/tables | grep -v state.yaml`; do \
  substreams-sink-sql inject-csv "risingwave://root:@localhost:4566/dev?schema=public" ./data/tables "$i" :14490000; \
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
