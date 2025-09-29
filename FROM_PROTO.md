# FROM_PROTO Command Guide

The `from-proto` command allows you to run a Substreams SQL sink directly from a protocol buffer definition without needing to set up separate schema files. This command automatically generates the SQL schema from your protobuf definitions and runs the sink in a single step.

## Overview

The `from-proto` command streamlines the process of running a SQL sink by:
1. Reading your Substreams manifest and protobuf definitions
2. Automatically generating the SQL schema from proto annotations
3. Creating database tables based on the proto message structure
4. Running the sink to stream data from Substreams to your database

## Command Syntax

```bash
substreams-sink-sql from-proto <dsn> <manifest> [output-module]
```

### Arguments

- `<dsn>`: Database connection string (Data Source Name)
- `<manifest>`: Path to your Substreams manifest file (substreams.yaml)
- `[output-module]`: Optional. Name of the output module to stream from (defaults to auto-detection)

### Common Flags

- `-e, --substreams-endpoint`: Substreams gRPC endpoint
- `-s, --start-block`: Start block number to stream from
- `-t, --stop-block`: Stop block to end stream at (default: 0, meaning no limit)
- `--no-constraints`: Skip adding database constraints (useful for fast initial imports)
- `--block-batch-size`: Number of blocks to process at a time (default: 25)

## Step-by-Step Workflow

### Step 1: Create Your Protocol Buffer Definition

Create a `.proto` file that defines your data structure with SQL schema annotations:

```proto
syntax = "proto3";
import "google/protobuf/timestamp.proto";
import "sf/substreams/sink/sql/schema/v1/schema.proto";

package myproject;

message Output {
  repeated Entity entities = 1;
}

message Entity {
  oneof entity {
    User user = 1;
    Transaction transaction = 2;
  }
}

message User {
  option (schema.table) = {
    name: "users"
    clickhouse_table_options: {
      order_by_fields: [{name: "id"}]
    }
  };

  string id = 1 [(schema.field) = { primary_key: true }];
  string name = 2;
  string email = 3;
  google.protobuf.Timestamp created_at = 4;
}

message Transaction {
  option (schema.table) = {
    name: "transactions"
    clickhouse_table_options: {
      order_by_fields: [{name: "tx_hash"}]
    }
  };

  string tx_hash = 1 [(schema.field) = { primary_key: true }];
  string user_id = 2 [(schema.field) = { foreign_key: "users on id"}];
  uint64 amount = 3;
  google.protobuf.Timestamp timestamp = 4;
}
```

### Step 2: Create Your Substreams Manifest

Create a `substreams.yaml` file that references your protobuf:

```yaml
specVersion: v0.1.0
package:
  name: 'myproject'
  version: v0.1.0
  doc: |
    My Substreams project with SQL sink

protobuf:
  files:
    - myproject.proto
  importPaths:
    - ./proto

binaries:
  default:
    type: wasm/rust-v1
    file: ./target/wasm32-unknown-unknown/release/myproject.wasm

modules:
  - name: map_events
    kind: map
    inputs:
      - source: sf.ethereum.type.v2.Block
    output:
      type: proto:myproject.Output

network: mainnet

sink:
  module: map_events
  type: sf.substreams.sink.sql.v1.Service
  config: {}
```

### Step 3: Implement Your Substreams Module

Create your Rust module that outputs data matching your proto definition:

```rust
use substreams::prelude::*;
use substreams_ethereum::pb::eth;

#[substreams::handlers::map]
fn map_events(block: eth::v2::Block) -> Result<myproject::Output, substreams::errors::Error> {
    let mut output = myproject::Output::default();
    
    // Process block data and populate entities
    // ... your business logic here ...
    
    Ok(output)
}
```

### Step 4: Compile Your Substreams

Build your WASM binary:

```bash
cargo build --target wasm32-unknown-unknown --release
```

### Step 5: Set Up Your Database

Start your database (PostgreSQL or ClickHouse):

**PostgreSQL:**
```bash
docker run --name postgres -e POSTGRES_PASSWORD=password -p 5432:5432 -d postgres:13
```

**ClickHouse:**
```bash
docker run --name clickhouse -p 9000:9000 -d clickhouse/clickhouse-server
```

### Step 6: Run the from-proto Command

Execute the `from-proto` command to automatically generate schema and start streaming:

**PostgreSQL:**
```bash
export DSN="postgres://postgres:password@localhost:5432/postgres?sslmode=disable"
substreams-sink-sql from-proto $DSN substreams.yaml
```

**ClickHouse:**
```bash
export DSN="clickhouse://default:@localhost:9000/default"
substreams-sink-sql from-proto $DSN substreams.yaml
```

## Proto Schema Annotations

The `from-proto` command relies on special protobuf annotations to generate the SQL schema. Here are the key annotations:

### Table Options

```proto
message MyTable {
  option (schema.table) = {
    name: "my_table"
    clickhouse_table_options: {
      order_by_fields: [{name: "id"}]
      partition_fields: [{name: "created_date", function: toYYYYMM}]
      index_fields: [{
        field_name: "status"
        name: "status_idx"
        type: bloom_filter
        granularity: 4
      }]
    }
  };
}
```

### Field Options

```proto
// Primary key
string id = 1 [(schema.field) = { primary_key: true }];

// Foreign key relationship
string user_id = 2 [(schema.field) = { foreign_key: "users on id"}];

// Unique constraint
string email = 3 [(schema.field) = { unique: true }];
```

### Child Tables

For nested messages, you can create child tables:

```proto
message OrderItem {
  option (schema.table) = {
    name: "order_items",
    child_of: "orders on order_id"
  };
  
  string item_id = 1;
  int64 quantity = 2;
}
```

## Supported Data Types

The `from-proto` command automatically maps protobuf types to SQL types:

| Protobuf Type | PostgreSQL Type | ClickHouse Type |
|---------------|-----------------|-----------------|
| `string` | `VARCHAR(255)` | `String` |
| `int32`, `sint32`, `sfixed32` | `INTEGER` | `Int32` |
| `int64`, `sint64`, `sfixed64` | `BIGINT` | `Int64` |
| `uint32`, `fixed32` | `NUMERIC` | `UInt32` |
| `uint64`, `fixed64` | `NUMERIC` | `UInt64` |
| `float` | `DECIMAL` | `Float32` |
| `double` | `DOUBLE PRECISION` | `Float64` |
| `bool` | `BOOLEAN` | `Bool` |
| `bytes` | `TEXT` | `String` |
| `google.protobuf.Timestamp` | `TIMESTAMP` | `DateTime` |
| `repeated <type>` | `<type>[]` | `Array(<type>)` |

## Advanced Usage

### Custom Block Range

Stream specific block ranges:

```bash
substreams-sink-sql from-proto $DSN substreams.yaml \
  --start-block 1000000 \
  --stop-block 1001000
```

### Performance Optimization

For high-throughput scenarios:

```bash
substreams-sink-sql from-proto $DSN substreams.yaml \
  --no-constraints \
  --block-batch-size 100
```

### ClickHouse-Specific Options

For ClickHouse with additional configuration:

```bash
substreams-sink-sql from-proto $DSN substreams.yaml \
  --clickhouse-sink-info-folder ./clickhouse-info \
  --clickhouse-cursor-file-path ./cursor.txt
```

## Troubleshooting

### Common Issues

1. **Proto import errors**: Ensure all required proto files are in your import paths
2. **Schema annotation errors**: Verify you're importing `sf/substreams/sink/sql/schema/v1/schema.proto`
3. **Database connection issues**: Check your DSN format and database accessibility
4. **Module output type errors**: Ensure your Substreams module outputs the expected proto message

### Debug Tips

1. Use `substreams info` to verify your manifest structure
2. Check database logs for schema creation issues  
3. Verify your protobuf definitions compile correctly
4. Test with a small block range first (`--start-block` and `--stop-block`)

## Examples

See the [test project](db_proto/test/substreams/order/) for a complete working example that demonstrates:
- Complex proto definitions with relationships
- ClickHouse-specific optimizations
- Various data types and constraints
- Child table relationships

## Migration from Traditional Setup

If you're migrating from the traditional `setup` + `run` workflow:

**Old way:**
```bash
substreams-sink-sql setup $DSN substreams.yaml
substreams-sink-sql run $DSN substreams.yaml
```

**New way with from-proto:**
```bash
substreams-sink-sql from-proto $DSN substreams.yaml
```

The `from-proto` command combines both steps and automatically generates the schema from your protobuf definitions instead of requiring a separate SQL schema file.