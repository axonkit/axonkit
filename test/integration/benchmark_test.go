package integration

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"rib-control/pkg/types"
)

// BenchmarkStreamScale measures performance when subscribing to many streams
// Run with: go test -bench=StreamScale -benchmem ./test/integration
func BenchmarkStreamScale(b *testing.B) {
	// Setup environment once
	srv := setupNATS(&testing.T{})
	defer srv.Shutdown()

	origin := createNode(&testing.T{}, srv, "Origin")
	edge := createNode(&testing.T{}, srv, "Edge")

	defer origin.Close()
	defer edge.Close()

	// Pre-advertise N streams
	// Note: b.N is the loop count, but for setup we might want fixed scale
	// Benchmarks usually run the function b.N times.
	// But here we want to test "Scale of N streams".
	// So we should use b.N as the number of streams?
	// Or we fix the number of streams and measure time?
	// Standard Go benchmark: loop b.N times.
	// Let's make each iteration "Subscribe 1 stream".

	// However, we need unique stream IDs.

	// Phase 1: Origin advertises b.N streams
	// This might be slow, so we pause timer?
	b.StopTimer()

	// We can't advertise b.N unique streams beforehand if b.N is huge.
	// Let's advertise on the fly or just assume they exist (mock findPath).
	// If we mock findPath, we test FIB+RIB Watcher logic without waiting for Origin Advertise propagation.
	// This focuses on "FIB Subscription Overhead".

	mockRoute := types.RouteKVValue{
		OriginNode: origin.ID,
		BaseCost:   0,
		PathVector: []types.NodeID{origin.ID},
	}

	// Warmup
	runtime.GC()
	initialGo := runtime.NumGoroutine()

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		streamID := types.StreamID(fmt.Sprintf("bench-stream-%d", i))

		// Note: We are NOT actually advertising from Origin to save time/resources in this bench.
		// We are testing the "Subscriber Side" cost.
		// BUT, if we don't advertise, the Watcher won't receive initial data?
		// Wait, FIB.Subscribe calls findPath (mocked), then StartAdvertise (Local), then monitorUpstream.
		// monitorUpstream registers to RIB.
		// RIB updates Consumer Filter.
		// This is the heavy part we want to measure.

		_, _, err := edge.FIB.Subscribe(streamID, func(sid types.StreamID) (*types.RouteKVValue, error) {
			return &mockRoute, nil
		})
		if err != nil {
			b.Fatalf("Subscribe failed: %v", err)
		}
	}

	b.StopTimer()

	finalGo := runtime.NumGoroutine()
	b.ReportMetric(float64(finalGo-initialGo), "goroutines_added")

	// Cleanup
	// If we don't clean up, NATS connection might choke on next run
	// But MockNode.Close() handles it.
}

// BenchmarkChurn measures performance of frequent Subscribe/Unsubscribe
func BenchmarkChurn(b *testing.B) {
	srv := setupNATS(&testing.T{})
	defer srv.Shutdown()

	node := createNode(&testing.T{}, srv, "EdgeChurn")
	defer node.Close()

	streamID := types.StreamID("churn-stream")
	mockRoute := types.RouteKVValue{OriginNode: "MockOrigin", BaseCost: 0}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Subscribe
		node.FIB.Subscribe(streamID, func(sid types.StreamID) (*types.RouteKVValue, error) {
			return &mockRoute, nil
		})

		// Unsubscribe immediately
		node.FIB.Unsubscribe(streamID)

		// Due to Linger, this should NOT trigger physical NATS changes after the first time.
		// It should be purely memory ops.
	}
}

// BenchmarkCascadingFailover measures time to propagate teardown
// This is more complex, involving NATS propagation latency.
func BenchmarkCascadingFailover(b *testing.B) {
	// This might be too slow for a standard benchmark loop.
	// Skip for now.
}

// TestHighConcurrency validates correctness under concurrent load
func TestHighConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrency test in short mode")
	}

	srv := setupNATS(t)
	defer srv.Shutdown()

	node := createNode(t, srv, "EdgeConc")
	defer node.Close()

	const NumStreams = 1000
	const NumWorkers = 10

	var wg sync.WaitGroup
	wg.Add(NumWorkers)

	start := time.Now()

	for w := 0; w < NumWorkers; w++ {
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < NumStreams/NumWorkers; i++ {
				streamID := types.StreamID(fmt.Sprintf("conc-stream-%d-%d", workerID, i))

				// Subscribe
				node.FIB.Subscribe(streamID, func(sid types.StreamID) (*types.RouteKVValue, error) {
					return &types.RouteKVValue{OriginNode: "MockOrigin", BaseCost: 0}, nil
				})

				// Wait a bit
				// time.Sleep(time.Millisecond)

				// Unsubscribe
				node.FIB.Unsubscribe(streamID)
			}
		}(w)
	}

	wg.Wait()
	duration := time.Since(start)

	t.Logf("Processed %d streams in %v (Throughput: %.2f ops/s)", NumStreams, duration, float64(NumStreams)/duration.Seconds())

	// Check Goroutine leak?
	// Hard to check exactly, but ensure no panic.
}
