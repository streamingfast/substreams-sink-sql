#!/usr/bin/env bash

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )"  && pwd )"
sink="$ROOT/../substreams-postgres-sink"

finish() {
    kill -s TERM $active_pid &> /dev/null || true
}

main() {
  trap "finish" EXIT
  pushd "$ROOT" &> /dev/null

    set -e

    if [[ -z "${PG_DSN}" ]]; then
      pg_dsn="psql://postgres:postgres@localhost/substreams_lidar?enable_incremental_sort=off&sslmode=disable"
    else
      pg_dsn="${PG_DSN}"
    fi

    $sink run \
      ${pg_dsn} \
      "api-dev.streamingfast.io:443" \
      "./lidar-v0.0.2.spkg" \
      "db_out" \
      "12287507:12293007" \
      "$@"

  popd &> /dev/null
}

main "$@"
