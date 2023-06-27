package bundler

import (
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dmetrics"
	"go.uber.org/zap"
	"time"
)

type boundaryStats struct {
	creationStart time.Time

	boundaryProcessTime time.Duration
	procesingDataTime   time.Duration
	uploadedDuration    time.Duration

	totalBoundaryCount uint64
	boundary           *bstream.Range

	// averages
	avgUploadDuration          *dmetrics.AvgDurationCounter
	avgBoundaryProcessDuration *dmetrics.AvgDurationCounter
	avgDataProcessDuration     *dmetrics.AvgDurationCounter
}

func newStats() *boundaryStats {
	return &boundaryStats{
		avgUploadDuration:          dmetrics.NewAvgDurationCounter(30*time.Second, time.Second, "upload dur"),
		avgBoundaryProcessDuration: dmetrics.NewAvgDurationCounter(30*time.Second, time.Second, "boundary process dur"),
		avgDataProcessDuration:     dmetrics.NewAvgDurationCounter(30*time.Second, time.Second, "data process dur"),
	}
}

func (s *boundaryStats) startBoundary(b *bstream.Range) {
	s.creationStart = time.Now()
	s.boundary = b
	s.totalBoundaryCount++
	s.boundaryProcessTime = 0
	s.procesingDataTime = 0
	s.uploadedDuration = 0
}

func (s *boundaryStats) addUploadedDuration(dur time.Duration) {
	s.avgUploadDuration.AddDuration(dur)
	s.uploadedDuration = dur
}

func (s *boundaryStats) endBoundary() {
	dur := time.Since(s.creationStart)
	s.avgBoundaryProcessDuration.AddDuration(dur)
	s.boundaryProcessTime = dur
	s.avgDataProcessDuration.AddDuration(s.procesingDataTime)
}

func (s *boundaryStats) addProcessingDataDur(dur time.Duration) {
	s.procesingDataTime += dur
}

func (s *boundaryStats) Log() []zap.Field {
	return []zap.Field{
		zap.Uint64("file_count", s.totalBoundaryCount),
		zap.Stringer("boundary", s.boundary),
		zap.Duration("boundary_process_duration", s.boundaryProcessTime),
		zap.Duration("upload_duration", s.uploadedDuration),
		zap.Duration("data_process_duration", s.procesingDataTime),
		zap.Float64("avg_upload_dur", s.avgUploadDuration.Average()),
		zap.Float64("total_upload_dur", s.avgUploadDuration.Total()),
		zap.Float64("avg_boundary_process_dur", s.avgBoundaryProcessDuration.Average()),
		zap.Float64("total_boundary_process_dur", s.avgBoundaryProcessDuration.Total()),
		zap.Float64("avg_data_process_dur", s.avgDataProcessDuration.Average()),
		zap.Float64("total_data_process_dur", s.avgDataProcessDuration.Total()),
	}
}
