# Substreams Postgres Sink

### Create a schema for the database changes
For the moment, we only support these types:
todo! @colin

The schema has to be a sql file and has to be passed in with the flag `--postgres-schema` (default value is `./schema.sql`).

Here is an example of a schema:
```json
{
  "pair": {
    "timestamp" : "timestamp",
    "block": "integer"
  }
}
```

> Note: any other field which is of type string isn't necessary to be declared

### Running postgres cli loader
1. Deploy postgres with docker (you can also use any other tool or method to deploy a local postgres)
```bash
docker run --rm -d --publish 5432:5432 postgres
```

2. Run the below command:
```bash
# local deployment of firehose
substreams-postgres-sink load ./substreams.yaml db_out --endpoint localhost:9000 -p -s 6810706 -t 6810806 
# run remotely against bsc
sftoken # https://substreams.streamingfast.io/reference-and-specs/authentication
substreams-postgres-sink load ./substreams.yaml db_out --endpoint bsc-dev.streamingfast.io -k -s 6810706 -t 6810806
```


