package sinker

import (
	"github.com/streamingfast/dmetrics"
)

func RegisterMetrics() {
	metrics.Register()
}

var metrics = dmetrics.NewSet()

var FlushCount = metrics.NewCounter("substreams_sink_postgres_store_flush_count", "The amount of flush that happened so far")
var FlushedEntriesCount = metrics.NewGauge("substreams_sink_postgres_flushed_entries_count", "The number of flushed entries so far")
var FlushDuration = metrics.NewCounter("substreams_sink_postgres_store_flush_duration", "The amount of time spent flushing cache to db (in nanoseconds)")
