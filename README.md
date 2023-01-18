# Substreams Postgres Sink

This is a command line tool to quickly sync a substreams with a posgresql database.

### Setup

1. Install `substreams-sink-postgres` (Installation from source required for now):

 ```bash
 go install ./cmd/substreams-sink-postgres
 ```

2. Run setup command

```bash
substreams-sink-postgres setup <psql connection string> <path to schema file>
```

This will connect to the given database, create the tables and indexes specified in the given schema file, and will create the required tables to run the sink (ie: the `cursors` table).

For the sake of idempotency, we recommend that the schema file only contain `create table if not exists` statements.

### Running It

1. Your Substreams needs to implement a `map` that has an output type of `proto:sf.substreams.database.v1.DatabaseChanges`.
By convention, we name the `map` module `db_out`. The [substreams-data-change](https://github.com/streamingfast/substreams-database-change) crate contains Rust bindings and helpers to implement it. Some project implementing `db_out` module for reference:

    * [substreams-eth-block-meta](https://github.com/streamingfast/substreams-eth-block-meta/blob/master/src/lib.rs#L35) (some helpers found in [db_out.rs](https://github.com/streamingfast/substreams-eth-block-meta/blob/master/src/db_out.rs#L6))

1. A PostgresSQL database needs to be created with a schema applied, that matched the output `map` database changes. The
schema should contain a cursors table with columns `id`, `cursor`, `block_num`.

    Sample SQL schema

    ```sql
    create table tokens
    (
        id                text not null constraint token_pk primary key,
        contract_address  text,
        token_id          text,
        owner_address     text,
        created_block_num bigint,
        updated_block_num bigint
    );

    create table cursors
    (
        id         text not null constraint cursor_pk primary key,
        cursor     text,
        block_num  bigint,
        block_id   text
    );
    ```

    Once the SQL schema is created, you can Run the following commands from the folder that has your schema, to create
    a database `substreams_example` and load your schema into it.

    ```shell
    psql -h <host> -p <port> -U <user> -d <database>
    > CREATE DATABASE substreams_example
    > \c substreams_example
    > \i schema.sql
    ```

1. Install `substreams-sink-postgres` (Installation from source required for now):

    ```
    go install ./cmd/substreams-sink-postgres
    ```

    And check that your installation is correct:

    ```
    substreams-sink-postgres --help
    ```

    > If you see `command not found: substreams-sink-postgres` or something similar, your `PATH` environment is not configured correctly to find compiled Go binaries. Find where Go installs the binary with `go env | grep GOPATH` (on my machine it gives `GOPATH="/Users/name/go`) then append `/bin` to result and add it to your `PATH` with `export PATH=/Users/name/go/bin:$PATH` (make this change persistent if you plan to use it often).

1. Run the sink

    > To connect to substreams you will need an authentication token, follow this [guide](https://substreams.streamingfast.io/reference-and-specs/authentication) to obtain one,

    ```shell
    substreams-sink-postgres run \
        "psql://psql_user:psql_password@psql_host/psql_database?sslmode=disable" \
        "mainnet.eth.streamingfast.io:443" \
        "substreams.yaml" \
        db_out
    ```



