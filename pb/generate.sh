#!/bin/bash
# Copyright 2021 dfuse Platform Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"

# Protobuf definitions
PROTO=${1:-"$ROOT/proto"}

function main() {
  checks

  set -e

  cd "$ROOT/proto" &> /dev/null

  buf generate

  echo "generate.sh - `date` - `whoami`" > $ROOT/pb/last_generate.txt
}

function checks() {
  # The old `protoc-gen-go` did not accept any flags. Just using `protoc-gen-go --version` in this
  # version waits forever. So we pipe some wrong input to make it exit fast. This in the new version
  # which supports `--version` correctly print the version anyway and discard the standard input
  # so it's good with both version.
  result=`buf --version 2>&1 | grep -Eo '1.[0-9]+'`
  if [[ "$result" == "" ]]; then
    echo "Your version of 'buf' (at `which buf`) is not recent enough or is missing."
    echo ""
    echo "To fix your problem, perform those commands:"
    echo ""
    echo "  brew install bufbuild/buf/buf"
    echo ""
    echo "If you are not on MacOS, follow the instructions at https://buf.build/docs/installation to install 'buf'"
    echo ""
    echo "  buf --version"
    echo ""
    echo "Should print '1.18.0' (your printed version might differs)"
    exit 1
  fi
}

main "$@"
