package tests

import (
	"fmt"
	"sync"

	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FakeStreamServer implements pbsubstreamsrpc.StreamServer for testing
// It supports buckets of messages separated by nil boundaries, where each call
// to Blocks processes the next bucket.
type FakeStreamServer struct {
	pbsubstreamsrpc.UnimplementedStreamServer
	messageBuckets [][]*pbsubstreamsrpc.Response
	currentBucket  int
	mu             sync.Mutex
}

// NewFakeStreamServer creates a new fake stream server with message buckets.
// Messages should be grouped in buckets, with nil representing bucket boundaries.
// Each call to Blocks will process the next bucket.
func NewFakeStreamServer(messages []*pbsubstreamsrpc.Response) *FakeStreamServer {
	buckets := [][]*pbsubstreamsrpc.Response{}
	currentBucket := []*pbsubstreamsrpc.Response{}

	for _, msg := range messages {
		if msg == nil {
			// nil message indicates bucket boundary
			if len(currentBucket) > 0 {
				buckets = append(buckets, currentBucket)
				currentBucket = []*pbsubstreamsrpc.Response{}
			}
		} else {
			currentBucket = append(currentBucket, msg)
		}
	}

	// Add final bucket if it has messages
	if len(currentBucket) > 0 {
		buckets = append(buckets, currentBucket)
	}

	return &FakeStreamServer{
		messageBuckets: buckets,
		currentBucket:  0,
	}
}

// Blocks implements the Stream RPC method
// Each call processes the next bucket of messages. When all buckets are exhausted,
// returns an error indicating test mock data is exhausted.
func (s *FakeStreamServer) Blocks(req *pbsubstreamsrpc.Request, stream pbsubstreamsrpc.Stream_BlocksServer) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if we have exhausted all buckets
	if s.currentBucket >= len(s.messageBuckets) {
		// We use Unauthenticated because it's a fatal error in the sinker which will stop processing
		return status.Error(codes.Unauthenticated, "test mock data exhausted: no more message buckets available")
	}

	// Get current bucket of messages
	messages := s.messageBuckets[s.currentBucket]
	s.currentBucket++

	// First send SessionInit message
	sessionInit := &pbsubstreamsrpc.Response{
		Message: &pbsubstreamsrpc.Response_Session{
			Session: &pbsubstreamsrpc.SessionInit{
				TraceId:            fmt.Sprintf("test-trace-id-bucket-%d", s.currentBucket-1),
				ResolvedStartBlock: 100,
				LinearHandoffBlock: 1000,
				MaxParallelWorkers: 1,
			},
		},
	}

	if err := stream.Send(sessionInit); err != nil {
		return fmt.Errorf("failed to send session init: %w", err)
	}

	// Send messages from current bucket
	for _, msg := range messages {
		if err := stream.Send(msg); err != nil {
			return fmt.Errorf("failed to send message: %w", err)
		}
	}

	// Stream terminates after sending all messages from current bucket
	return nil
}

// Reset resets the server to start from the first bucket again
func (s *FakeStreamServer) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentBucket = 0
}

// GetBucketCount returns the total number of message buckets
func (s *FakeStreamServer) GetBucketCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.messageBuckets)
}

// GetCurrentBucket returns the current bucket index (0-based)
func (s *FakeStreamServer) GetCurrentBucket() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentBucket
}
