# Substreams Sink PostgreSQL

This is a command line tool to quickly sync a Substreams with a PostgreSQL database.

### Quickstart

1. Install `substreams-sink-postgres` by using the pre-built binary release [available in the releases page](https://github.com/streamingfast/substreams-sink-postgres/releases). Extract `substreams-sink-postgres` binary into a folder and ensure this folder is referenced globally via your `PATH` environment variable.

    > **Note** Or install from source directly `go install github.com/streamingfast/substreams-sink-postgres/cmd/substreams-sink-postgres@latest`.

1. Start Docker Compose:

    ```bash
    docker compose up
    ```

    > **Note** Feel free to skip this step if you already have a running Postgres instance accessible, don't forget to update the connection string in the command below.

1. Run the setup command:

    ```bash
    substreams-sink-postgres setup "psql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node?sslmode=disable" docs/tutorial/schema.sql
    ```

    This will connect to the given database pointed by `psql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node?sslmode=disable`, create the tables and indexes specified in the given `<schema_file>`, and will create the required tables to run the sink (e.g. the `cursors` table).

    > **Note** For the sake of idempotency, we recommend that the schema file only contain `create table if not exists` statements.

1. Run the sink

    Compile the [Substreams](./docs/tutorial/substreams.yaml) tutorial project first:

    ```bash
    cd docs/tutorial
    cargo build --target wasm32-unknown-unknown --release
    cd ../..
    ```

    Once the compilation has completed, let launch the `sink` process.

    > **Note** To connect to Substreams you will need an authentication token, follow this [guide](https://substreams.streamingfast.io/reference-and-specs/authentication) to obtain one.

    ```shell
    substreams-sink-postgres run \
        "psql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node?sslmode=disable" \
        "mainnet.eth.streamingfast.io:443" \
        "./docs/tutorial/substreams.yaml" \
        db_out
    ```

### Output Module

To be accepted by `substreams-sink-postgres`, your module output's type must be a [sf.substreams.sink.database.v1.DatabaseChanges](https://github.com/streamingfast/substreams-database-change/blob/develop/proto/substreams/sink/database/v1/database.proto#L7) message. The Rust crate [substreams-data-change](https://github.com/streamingfast/substreams-database-change) contains bindings and helpers to implement it easily. Some project implementing `db_out` module for reference:
- [substreams-eth-block-meta](https://github.com/streamingfast/substreams-eth-block-meta/blob/master/src/lib.rs#L35) (some helpers found in [db_out.rs](https://github.com/streamingfast/substreams-eth-block-meta/blob/master/src/db_out.rs#L6))

By convention, we name the `map` module that emits [sf.substreams.sink.database.v1.DatabaseChanges](https://github.com/streamingfast/substreams-database-change/blob/develop/proto/substreams/sink/database/v1/database.proto#L7) output `db_out`.

> Note that using prior versions (0.2.0, 0.1.*) of `substreams-database-change`, you have to use `substreams.database.v1.DatabaseChanges` in your `substreams.yaml` and put the respected version of the `spkg` in your `substreams.yaml`

### PostgreSQL DSN

The connection string is provided using a simple string format respecting the URL specification. The DSN format is:

```
psql://<user>:<password>@<host>/<dbname>[?<options>]
```

Where `<options>` is URL query parameters in `<key>=<value>` format, multiple options are separated by `&` signs. Supported options can be seen [on libpq official documentation](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS). The options `<user>`, `<password>`, `<host>` and `<dbname>` should **not** be passed in `<options>` as they are automatically extracted from the DSN URL.

Moreover, the `schema` option key can be used to select a particular schema within the `<dbname>` database.

### Advanced Topics

#### High Throughput Injection

> [!IMPORTANT]
> This method will be useful if you insert a lot of data into the database. If the standard ingestion speed satisfy your needs, continue to use it, the steps below are an advanced use case.

The `substreams-sink-postgres` contains a fast injection mechanism for cases where big data needs to be dump into the database. In those cases, it may be preferable to dump every files to CSV and then use `COPYFROM` to transfer data super quick to Postgres.

The idea is to first dump the Substreams data to `CSV` files using `substreams-sink-postgres generate-csv` command:

```bash
substreams-sink-postgres generate-csv "psql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node?sslmode=disable" mainnet.eth.streamingfast.io:443 <spkg> db_out ./data/tables :14490000
```

> [!NOTE]
> We are using 14490000 as our stop block, pick you stop block close to chain's HEAD or smaller like us to perform an experiment, adjust to your needs.

This will generate block segmented CSV files for each table in your schema inside the folder `./data/tables`. Next step is to actually inject those CSV files into your database. You can use `psql` and inject directly with it.

We offer `substreams-sink-postgres inject-csv` command as a convenience. It's a per table invocation but feel free to run each table concurrently, your are bound by your database as this point, so it's up to you to decide you much concurrency you want to use. Here a small `Bash` command to loop through all tables and inject them all

```bash
for i in `ls ./data/tables | grep -v state.yaml`; do \
  substreams-sink-postgres inject-csv "psql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node?sslmode=disable" ./data/tables "$i" :14490000; \
  if [[ $? != 0 ]]; then break; fi; \
done
```

Those files are then inserted in the database efficiently by doing a `COPY FROM` and reading the data from a network pipe directly.

The command above will also pick up the `cursors` table injection as it's a standard table to write. The table is a bit special as it contains a single file which is contains the `cursor` that will handoff between CSV injection and going back to "live" blocks. It's extremely important that you validate that this table has been properly populated. You can do this simply by doing:

```bash
substreams-sink-postgres tools --dsn="psql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node?sslmode=disable" cursor read
Module eaf2fc2ea827d6aca3d5fee4ec9af202f3d1b725: Block #14490000 (61bd396f3776f26efc3f73c44e2b8be3b90cc5171facb1f9bdeef9cb5c4fd42a) [cqR8Jx...hxNg==]
```

This should emit a single line, the `Module <hash>` should fit the for `db_out` (check `substreams info <spkg>` to see your module's hashes) and the block number should fit your last block you written.

> [!WARNING]
> Failure to properly populate will 'cursors' table will make the injection starts from scratch when you will do `substreams-sink-postgres run` to bridge with "live" blocks as no cursor will exist so we will start from scratch.

Once data has been injected and you validated the `cursors` table, you can then simply start streaming normally using:

```bash
substreams-sink-postgres run "psql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node?sslmode=disable" mainnet.eth.streamingfast.io:443 <spkg> db_out
```

This will start back at the latest block written and will start to handoff streaming to a "live" blocks.

##### Performance Knobs

When generating the CSV files, optimally choosing the `--buffer-max-size` configuration value can drastically increase your write throughput locally but even more if your target store is an Amazon S3, Google Cloud Storage or Azure bucket. The flag controls how many bytes of the files is to be held in memory. By having bigger amount of buffered bytes, data is transferred in big chunk to the storage layer leading to improve performance. In lots of cases, the full file can be held in memory leading to a single "upload" call being performed having even better performance.

When choosing this value you should consider 2 things:
- One buffer exist by table in your schema, so if there is 12 tables and you have a 128 MiB buffer, you could have up to 1.536 GiB (`128 MiB * 12`) of RAM allocated to those buffers.
- Amount of RAM you want to allocate.

Let's take a container that is going to have 8 GiB of RAM. We suggest leaving 512 MiB for other part of the `generate-csv` tasks, which mean we could dedicated 7.488 GiB to buffering. If your schema has 10 tables, you should use `--buffer-max-size=785173709` (`7.488 GiB / 10 = 748.8 MiB = 785173709`).
