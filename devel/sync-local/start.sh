#!/usr/bin/env bash

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

main() {
  cd "$ROOT"

  set -e

  pg_dsn="${PG_DSN:-"psql://postgres:postgres@localhost/substreams?enable_incremental_sort=off&sslmode=disable"}"
  sink="$ROOT/../substreams-sink-postgres"

  echo $pg_dsn
  exit 1

  $sink run \
    ${pg_dsn} \
    "mainnet.eth.streamingfast.io:443" \
    "./substreams-v0.0.1.spkg" \
    "db_out" \
    "12287507:12293007" \
    "$@"
}

main "$@"
