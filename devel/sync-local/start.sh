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

  if [[ "$clean" == "true" ]]; then
    echo "Cleaning up existing tables"
    PGPASSWORD="insecure-change-me-in-prod" psql -h localhost -U dev-node -d dev-node -c '\i clean.sql'
  fi

  if [[ "$clean" == "true" || "$bootstrap" == "true" ]]; then
    echo "Creating tables"
    PGPASSWORD="insecure-change-me-in-prod" psql -h localhost -U dev-node -d dev-node -c '\i schema.sql'
  fi

  pg_dsn="${PG_DSN:-"psql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node?sslmode=disable"}"
  sink="../substreams-sink-postgres"

  $sink run \
    ${pg_dsn} \
    "${SUBSTREAMS_ENDPOINT:-"mainnet.eth.streamingfast.io:443"}" \
    "${SUBSTREAMS_MANIFEST:-"substreams-eth-block-meta-v0.2.0.spkg"}" \
    "${SUBSTREAMS_MODULE:-"db_out"}" \
    "$@"
}

main "$@"
