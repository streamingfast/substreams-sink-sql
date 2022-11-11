# Substreams Postgres Sink

This is a command line tool to quickly sync a substreams with a posgresql database.

### Running It

1) Your Substreams needs to implement a `map` that has an output type of `proto:substreams.database.v1.DatabaseChanges`.
By convention, we name the `map` module `db_out`. The [substreams-data-change](https://github.com/streamingfast/substreams-database-change) crate, contains the rust objects.


2) A postgresql database needs to be created with a schema applied, that matched the output `map` database changes. The 
schema should contain a cursors table with columnes `id`, `cursor`, `block_num`. 

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
    block_num  bigint
);
```

Once the SQL schema is created, you can run the following commands from the folder that has your schema, to create 
a database `substreams_example` and load your schema into it.

```shell
psql
> CREATE DATABASE substreams_example
> \c substreams_example
> \i schema.sql
```

3) Run the syncer

| Note: to connect to substreams you will nee an authentication token, follow this [guide](https://substreams.streamingfast.io/reference-and-specs/authentication) |
|------------------------------------------------------------------------------------------------------------------------------------------------------------------|


```shell
go install ./cmd/substreams-postgres-sink
substreams-postgres-sink run \
"psql://postgres:postgres@localhost/substreams_example?enable_incremental_sort=off&sslmode=disable" \
"api-dev.streamingfast.io:443" \
"substreams.yaml" \
db_out 
```



