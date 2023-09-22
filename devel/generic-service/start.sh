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

  if [[ "$clean" == "true" ]]; then
    echo "Cleaning up existing tables"
    PGPASSWORD=${PGPASSWORD:-"insecure-change-me-in-prod"} psql -h localhost -U dev-node -d dev-node -c '\i ../eth-block-meta/clean.sql'
  fi

  if [[ "$clean" == "true" || "$bootstrap" == "true" ]]; then
    echo "Creating tables"
    $sink setup "substreams.dev.yaml"
  fi

  $sink run \
    "${SUBSTREAMS_ENDPOINT:-"mainnet.eth.streamingfast.io:443"}" \
    "substreams.dev.yaml"
    "$@"
}

main "$@"

