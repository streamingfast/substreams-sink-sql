package syncer

import "github.com/streamingfast/dmetrics"

func RegisterMetrics() {
	metrics.Register()
}

var metrics = dmetrics.NewSet()

var HeadBlockNumber = metrics.NewHeadBlockNumber("substreams-psql-sink")
var HeadBlockTime = metrics.NewHeadTimeDrift("substreams-psql-sink")
var SubstreamsErrorCount = metrics.NewCounter("substreams_psql_sink_error", "The error count we encountered when interacting with Substreams for which we had to restart the connection loop")
var DataMessageCount = metrics.NewCounterVec("substreams_psql_sink_data_message", []string{"module"}, "The number of data message received")
var ProgressMessageCount = metrics.NewCounterVec("substreams_psql_sink_progress_message", []string{"module"}, "The number of progress message received")
