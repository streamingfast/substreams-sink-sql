#!/usr/bin/env bash

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

main() {
  cd "$ROOT" &> /dev/null

  while getopts "hbc" opt; do
    case $opt in
      h) usage && exit 0;;
      b) bootstrap=true;;
      c) clean=true;;
      \?) usage_error "Invalid option: -$OPTARG";;
    esac
  done
  shift $((OPTIND-1))

  set -e

  sink="../substreams-sink-sql"
  pg_password=${PGPASSWORD:-"insecure-change-me-in-prod"}
  pg_dsn="psql://dev-node:${pg_password}@127.0.0.1:5432/substreams_example?sslmode=disable"

  if [[ "$clean" == "true" ]]; then
    echo "Cleaning up existing tables"
    PGPASSWORD=${pg_password} psql -h localhost -U dev-node -d dev-node -c 'drop database substreams_example;'
  fi

  if [[ "$clean" == "true" || "$bootstrap" == "true" ]]; then
    echo "Creating tables"
    set -e
    PGPASSWORD=${pg_password} psql -h localhost -U dev-node -d dev-node -c 'create database substreams_example;'
    set +e
    $sink setup "$pg_dsn" ../../docs/tutorial/sink/substreams.dev.yaml
  fi

  $sink run \
    "$pg_dsn" \
    ../../docs/tutorial/sink/substreams.dev.yaml \
    "$@"
}

main "$@"

