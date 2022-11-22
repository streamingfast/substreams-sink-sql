package sinker

import "github.com/streamingfast/dmetrics"

func RegisterMetrics() {
	metrics.Register()
}

var metrics = dmetrics.NewSet()

var SubstreamsErrorCount = metrics.NewCounter("substreams_sink_postgres_error", "The error count we encountered when interacting with Substreams for which we had to restart the connection loop")
var DataMessageCount = metrics.NewCounterVec("substreams_sink_postgres_data_message", []string{"module"}, "The number of data message received")
var ProgressMessageCount = metrics.NewCounterVec("substreams_sink_postgres_progress_message", []string{"module"}, "The number of progress message received")
var BlockCount = metrics.NewCounter("substreams_sink_postgres_block_count", "The number of blocks received")
var FlushedEntriesCount = metrics.NewCounter("substreams_sink_postgres_flushed_entries_count", "The number of flushed entries")
var FlushCount = metrics.NewCounter("substreams_sink_postgres_store_flush_count", "The amount of flush that happened so far")
var FlushDuration = metrics.NewCounter("substreams_sink_postgres_store_flush_duration", "The amount of time spent flushing cache to db")
