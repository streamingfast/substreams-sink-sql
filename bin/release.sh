#!/usr/bin/env bash

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"

main() {
  if ! command -v sfreleaser &> /dev/null; then
    echo "The StreamingFast 'sfreleaser' CLI utility (https://github.com/streamingfast/tooling/cmd/sfreleaser/README.md) is "
    echo "required to perform the release."
    echo ""
    echo "Install via source with the command"
    echo ""
    echo "  go install github.com/streamingfast/tooling/cmd/sfreleaser@latest (must have \`go env GOPATH/bin\` in your PATH)"
    echo ""
    echo "And then re-run this script."
    exit 1
  fi

  # FIXME Check actual release date for knowing if "recent enough"

  exec sfreleaser --root "$ROOT" --language go --variant application release "$@"
}

main "$@"
