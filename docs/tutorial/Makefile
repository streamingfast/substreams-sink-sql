DSN ?= psql://dev-node:insecure-change-me-in-prod@127.0.0.1:5432/substreams_example?sslmode=disable
ENDPOINT ?= mainnet.eth.streamingfast.io:443

.PHONY: build
build:
	cargo build --target wasm32-unknown-unknown --release

.PHONY: protogen
protogen:
	substreams protogen ./substreams.yaml --exclude-paths="sf/substreams,google"

.PHONY: stream_db_out
stream_db_out: build
	substreams run -e $(ENDPOINT) substreams.yaml db_out -t +10

.PHONY: sink_db_out
sink_db_out: build
	substreams-sink-sql setup "$(DSN)" --ignore-duplicate-table-errors sink/substreams.dev.yaml
	substreams-sink-sql run "$(DSN)" sink/substreams.dev.yaml
