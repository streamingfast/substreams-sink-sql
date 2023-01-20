# Substreams sink Postgres

## Description

`substreams-sink-postgres` is a tool that allows developers to pipe data extracted from a blockchain into a PostgreSQL database.

## Prerequisites

- A Substreams module prepared for PostgreSQL
- PostgreSQL installation

## Installation

Install `substreams-sink-postgres` by using the pre-built binary release [available in the official GitHub repository](https://github.com/streamingfast/substreams-sink-postgres/releases).

Extract `substreams-sink-postgres` into a folder and ensure this folder is referenced globally via your `PATH` environment variable.

## Using the `substreams-sink-postgres` tool

The `run` command is the primary way to work with the `substreams-sink-postgres` tool. The command for your project will resemble the following:

{% code overflow="wrap" %}

```bash
substreams-sink-postgres run \ "psql://<username>:<password>@<database_ip_address>/substreams_example?sslmode=disable" \ "mainnet.eth.streamingfast.io:443" \ "substreams.yaml" \ db_out
```

You'll need to use values for your Substreams module and desired output for each of the flags in the command.

{% endcode %}

Output resembling the following will be printed to the terminal window for properly issued commands and a properly set up and configured Substreams module.

```bash
2023-01-09T07:45:02.563-0800 INFO (substreams-sink-files) starting prometheus metrics server {"listen_addr": 2023-01-18T12:32:19.107-0800 INFO (sink-postgres) starting prometheus metrics server {"listen_addr": "localhost:9102"}
2023-01-18T12:32:19.107-0800 INFO (sink-postgres) sink from psql {"dsn": "psql://postgres:pass1@localhost/substreams_example?sslmode=disable", "endpoint": "mainnet.eth.streamingfast.io:443", "manifest_path": "substreams.yaml", "output_module_name": "db_out", "block_range": ""}
2023-01-18T12:32:19.107-0800 INFO (sink-postgres) starting pprof server {"listen_addr": "localhost:6060"}
2023-01-18T12:32:19.127-0800 INFO (sink-postgres) reading substreams manifest {"manifest_path": "substreams.yaml"}
2023-01-18T12:32:20.283-0800 INFO (pipeline) computed start block {"module_name": "store_block_meta_start", "start_block": 0}
2023-01-18T12:32:20.283-0800 INFO (pipeline) computed start block {"module_name": "db_out", "start_block": 0}
2023-01-18T12:32:20.283-0800 INFO (sink-postgres) validating output store {"output_store": "db_out"}
2023-01-18T12:32:20.285-0800 INFO (sink-postgres) resolved block range {"start_block": 0, "stop_block": 0}
2023-01-18T12:32:20.287-0800 INFO (sink-postgres) ready, waiting for signal to quit
2023-01-18T12:32:20.287-0800 INFO (sink-postgres) starting stats service {"runs_each": "2s"}
2023-01-18T12:32:20.288-0800 INFO (sink-postgres) no block data buffer provided. since undo steps are possible, using default buffer size {"size": 12}
2023-01-18T12:32:20.288-0800 INFO (sink-postgres) starting stats service {"runs_each": "2s"}
2023-01-18T12:32:20.730-0800 INFO (sink-postgres) session init {"trace_id": "4605d4adbab0831c7505265a0366744c"}
2023-01-18T12:32:21.041-0800 INFO (sink-postgres) flushing table entries {"table_name": "block_data", "entry_count": 2}
2023-01-18T12:32:21.206-0800 INFO (sink-postgres) flushing table entries {"table_name": "block_data", "entry_count": 2}
2023-01-18T12:32:21.319-0800 INFO (sink-postgres) flushing table entries {"table_name": "block_data", "entry_count": 0}
2023-01-18T12:32:21.418-0800 INFO (sink-postgres) flushing table entries {"table_name": "block_data", "entry_count": 0}
```

### Cursors

**TODO** Port appropriate cursors information from the tutorial in the Substreams docs to this section of the README.

## Contributing

For additional information, [refer to the general StreamingFast contribution guide](https://github.com/streamingfast/streamingfast/blob/master/CONTRIBUTING.md).

## License

The `substreams-sink-files` tool [uses the Apache 2.0 license](https://github.com/streamingfast/substreams/blob/develop/LICENSE/README.md).
