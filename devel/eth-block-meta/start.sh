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
  pg_dsn="psql://dev-node:${pg_password}@127.0.0.1:5432/dev-node?sslmode=disable"

  if [[ "$clean" == "true" ]]; then
    echo "Cleaning up existing tables"
    PGPASSWORD="${pg_password}" psql -h localhost -U dev-node -d dev-node -c '\i clean.sql'
  fi

  if [[ "$clean" == "true" || "$bootstrap" == "true" ]]; then
    echo "Creating tables"
    $sink setup "$pg_dsn" ./substreams.dev.yaml
  fi

  $sink run \
    "$pg_dsn" \
    ./substreams.dev.yaml \
    "$@"
}

main "$@"

