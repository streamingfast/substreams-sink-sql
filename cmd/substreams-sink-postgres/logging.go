package main

import (
	"github.com/streamingfast/cli"
	"github.com/streamingfast/logging"
	"go.uber.org/zap"
)

var zlog, tracer = logging.RootLogger("sink-postgres", "github.com/streamingfast/substreams-sink-mongodb/cmd/substreams-sink-mongodb")

func init() {
	cli.SetLogger(zlog, tracer)

	logging.InstantiateLoggers(logging.WithDefaultLevel(zap.InfoLevel))
}
