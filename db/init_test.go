package db

import (
	_ "github.com/lib/pq"
	"github.com/streamingfast/logging"
)

var zlog, tracer = logging.PackageLogger("sink-postgres", "github.com/streamingfast/substreams-sink-postgres/db")

func init() {
	logging.InstantiateLoggers()
}
