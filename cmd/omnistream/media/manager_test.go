package media

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/jiyeyuran/omnistream/pkg/types"
	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

func setupNats(t *testing.T) (*server.Server, *nats.Conn) {
	t.Helper()
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	srv := natsserver.RunServer(&opts)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	return srv, nc
}

func setupManager(t *testing.T, nodeID types.NodeID, nc *nats.Conn) *Manager {
	t.Helper()
	workerBin := os.Getenv("MEDIASOUP_WORKER_BIN")
	if workerBin == "" {
		t.Skip("MEDIASOUP_WORKER_BIN not set")
	}
	m := NewManager(nodeID, nc, workerBin, WithWorkerCount(1))
	if err := m.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	return m
}

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timeout")
}

func TestManager_StartInvalidWorkerBin(t *testing.T) {
	srv, nc := setupNats(t)
	defer srv.Shutdown()
	defer nc.Close()

	m := NewManager("node-invalid", nc, "/invalid/worker", WithWorkerCount(1))
	if err := m.Start(); err == nil {
		t.Fatalf("expected error for invalid worker bin")
	}
}

func TestManager_StartWithWorkerCount(t *testing.T) {
	srv, nc := setupNats(t)
	defer srv.Shutdown()
	defer nc.Close()

	workerBin := os.Getenv("MEDIASOUP_WORKER_BIN")
	if workerBin == "" {
		t.Skip("MEDIASOUP_WORKER_BIN not set")
	}
	m := NewManager("node-count", nc, workerBin, WithWorkerCount(1))
	if err := m.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if len(m.workers) != 1 || len(m.routers) != 1 {
		t.Fatalf("expected 1 worker and 1 router")
	}
	m.Close()
}

func TestManager_PublishConsumeLifecycle(t *testing.T) {
	srv, nc := setupNats(t)
	defer srv.Shutdown()
	defer nc.Close()

	m := setupManager(t, "node-1", nc)
	defer m.Close()

	streamID, err := m.Publish()
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	m.producersMu.RLock()
	lp := m.producers[streamID]
	m.producersMu.RUnlock()
	if lp == nil || lp.Id() == "" {
		t.Fatalf("expected producer id")
	}
	consumer, err := m.Consume("client-1", streamID)
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}
	if consumer == nil {
		t.Fatalf("expected consumer")
	}
	if err := m.Unsubscribe("client-1", streamID); err != nil {
		t.Fatalf("Unsubscribe failed: %v", err)
	}
	if err := m.Unpublish(streamID); err != nil {
		t.Fatalf("Unpublish failed: %v", err)
	}

	m.producersMu.RLock()
	_, ok := m.producers[streamID]
	m.producersMu.RUnlock()
	if ok {
		t.Fatalf("expected producer removed")
	}

	m.clientsMu.RLock()
	ctx, ok := m.clients["client-1"]
	m.clientsMu.RUnlock()
	if ok {
		ctx.Mu.Lock()
		count := len(ctx.Consumers)
		ctx.Mu.Unlock()
		if count != 0 {
			t.Fatalf("expected client consumers empty")
		}
	}
}

func TestManager_CloseInvokesOnStreamClosed(t *testing.T) {
	srv, nc := setupNats(t)
	defer srv.Shutdown()
	defer nc.Close()

	m := setupManager(t, "node-close", nc)
	streamID, err := m.Publish()
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	closed := make(chan types.StreamID, 1)
	m.OnStreamClosed = func(s types.StreamID) {
		closed <- s
	}
	m.Close()
	select {
	case got := <-closed:
		if got != streamID {
			t.Fatalf("expected %s, got %s", streamID, got)
		}
	case <-time.After(time.Second):
		t.Fatalf("expected OnStreamClosed to be called")
	}
}

func TestManager_ConsumeMissingStream(t *testing.T) {
	srv, nc := setupNats(t)
	defer srv.Shutdown()
	defer nc.Close()

	m := setupManager(t, "node-missing", nc)
	defer m.Close()

	if _, err := m.Consume("client-1", "missing.stream"); err == nil {
		t.Fatalf("expected error for missing stream")
	}
}

func TestManager_UnsubscribeMissingClient(t *testing.T) {
	srv, nc := setupNats(t)
	defer srv.Shutdown()
	defer nc.Close()

	m := NewManager("node-unsub", nc, "")
	if err := m.Unsubscribe("missing-client", "missing.stream"); err != nil {
		t.Fatalf("Unsubscribe failed: %v", err)
	}
}

func TestManager_ConnectPipeLocal(t *testing.T) {
	srv, nc := setupNats(t)
	defer srv.Shutdown()
	defer nc.Close()

	m := setupManager(t, "node-local", nc)
	defer m.Close()

	streamID, err := m.Publish()
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	pipeID, err := m.ConnectPipe(streamID, m.nodeID)
	if err != nil {
		t.Fatalf("ConnectPipe failed: %v", err)
	}
	if pipeID == "" {
		t.Fatalf("expected pipe id")
	}
	if err := m.Unpublish(streamID); err != nil {
		t.Fatalf("Unpublish failed: %v", err)
	}
}

func TestManager_ConnectPipeInvalidStream(t *testing.T) {
	srv, nc := setupNats(t)
	defer srv.Shutdown()
	defer nc.Close()

	m := setupManager(t, "node-invalid-stream", nc)
	defer m.Close()

	if _, err := m.ConnectPipe(types.StreamID(".producer"), "node-x"); err == nil {
		t.Fatalf("expected error for invalid stream id")
	}
}

func TestManager_ConnectPipeLocalMissingStream(t *testing.T) {
	srv, nc := setupNats(t)
	defer srv.Shutdown()
	defer nc.Close()

	m := setupManager(t, "node-local-missing", nc)
	defer m.Close()

	if _, err := m.ConnectPipe("missing.stream", m.nodeID); err == nil {
		t.Fatalf("expected error for missing local stream")
	}
}

func TestManager_PipeBetweenManagersPauseResume(t *testing.T) {
	srv, ncA := setupNats(t)
	defer srv.Shutdown()
	defer ncA.Close()

	ncB, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer ncB.Close()

	mA := setupManager(t, "node-a", ncA)
	defer mA.Close()
	mB := setupManager(t, "node-b", ncB)
	defer mB.Close()

	streamID, err := mA.Publish()
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	pipeID, err := mB.ConnectPipe(streamID, "node-a")
	if err != nil {
		t.Fatalf("ConnectPipe failed: %v", err)
	}
	if pipeID == "" {
		t.Fatalf("expected pipe id")
	}

	req := PipeRequest{Command: "PausePipeProducer", StreamID: streamID, PipeConsumerId: pipeID}
	data, _ := json.Marshal(req)
	respMsg, err := ncB.Request("signaling.pipe.node-a", data, time.Second)
	if err != nil {
		t.Fatalf("Pause response missing: %v", err)
	}
	var resp PipeResponse
	json.Unmarshal(respMsg.Data, &resp)
	if resp.Error != "" {
		t.Fatalf("Pause response error: %s", resp.Error)
	}

	req = PipeRequest{Command: "ResumePipeProducer", StreamID: streamID, PipeConsumerId: pipeID}
	data, _ = json.Marshal(req)
	respMsg, err = ncB.Request("signaling.pipe.node-a", data, time.Second)
	if err != nil {
		t.Fatalf("Resume response missing: %v", err)
	}
	json.Unmarshal(respMsg.Data, &resp)
	if resp.Error != "" {
		t.Fatalf("Resume response error: %s", resp.Error)
	}

	if err := mB.DisconnectPipe(streamID, pipeID, "node-a"); err != nil {
		t.Fatalf("DisconnectPipe failed: %v", err)
	}
	if err := mA.Unpublish(streamID); err != nil {
		t.Fatalf("Unpublish failed: %v", err)
	}
}

func TestManager_PipeProducerStateMismatch(t *testing.T) {
	srv, ncA := setupNats(t)
	defer srv.Shutdown()
	defer ncA.Close()

	ncB, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer ncB.Close()

	mA := setupManager(t, "node-a", ncA)
	defer mA.Close()
	mB := setupManager(t, "node-b", ncB)
	defer mB.Close()

	streamID, err := mA.Publish()
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	pipeID, err := mB.ConnectPipe(streamID, "node-a")
	if err != nil {
		t.Fatalf("ConnectPipe failed: %v", err)
	}

	req := PipeRequest{Command: "PausePipeProducer", StreamID: streamID, PipeConsumerId: pipeID + "-bad"}
	data, _ := json.Marshal(req)
	respMsg, err := ncB.Request("signaling.pipe.node-b", data, time.Second)
	if err != nil {
		t.Fatalf("PausePipeProducer request failed: %v", err)
	}
	var resp PipeResponse
	json.Unmarshal(respMsg.Data, &resp)
	if resp.Error == "" {
		t.Fatalf("expected mismatch error")
	}

	if err := mB.DisconnectPipe(streamID, pipeID, "node-a"); err != nil {
		t.Fatalf("DisconnectPipe failed: %v", err)
	}
	if err := mA.Unpublish(streamID); err != nil {
		t.Fatalf("Unpublish failed: %v", err)
	}
}

func TestManager_PublishDataLifecycle(t *testing.T) {
	srv, nc := setupNats(t)
	defer srv.Shutdown()
	defer nc.Close()

	m := setupManager(t, "node-data", nc)
	defer m.Close()

	streamID, err := m.PublishData()
	if err != nil {
		t.Fatalf("PublishData failed: %v", err)
	}
	if err := m.Unpublish(streamID); err != nil {
		t.Fatalf("Unpublish failed: %v", err)
	}

	m.producersMu.RLock()
	_, ok := m.producers[streamID]
	m.producersMu.RUnlock()
	if ok {
		t.Fatalf("expected data producer removed")
	}
}

func TestManager_PublishDataPipeBetweenManagers(t *testing.T) {
	srv, ncA := setupNats(t)
	defer srv.Shutdown()
	defer ncA.Close()

	ncB, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer ncB.Close()

	mA := setupManager(t, "node-a", ncA)
	defer mA.Close()
	mB := setupManager(t, "node-b", ncB)
	defer mB.Close()

	streamID, err := mA.PublishData()
	if err != nil {
		t.Fatalf("PublishData failed: %v", err)
	}
	pipeID, err := mB.ConnectPipe(streamID, "node-a")
	if err != nil {
		t.Fatalf("ConnectPipe failed: %v", err)
	}
	if pipeID == "" {
		t.Fatalf("expected pipe id")
	}

	req := PipeRequest{Command: "PausePipeProducer", StreamID: streamID, PipeConsumerId: pipeID}
	data, _ := json.Marshal(req)
	respMsg, err := ncB.Request("signaling.pipe.node-a", data, time.Second)
	if err != nil {
		t.Fatalf("Pause response missing: %v", err)
	}
	var resp PipeResponse
	json.Unmarshal(respMsg.Data, &resp)
	if resp.Error != "" {
		t.Fatalf("Pause response error: %s", resp.Error)
	}

	req = PipeRequest{Command: "ResumePipeProducer", StreamID: streamID, PipeConsumerId: pipeID}
	data, _ = json.Marshal(req)
	respMsg, err = ncB.Request("signaling.pipe.node-a", data, time.Second)
	if err != nil {
		t.Fatalf("Resume response missing: %v", err)
	}
	json.Unmarshal(respMsg.Data, &resp)
	if resp.Error != "" {
		t.Fatalf("Resume response error: %s", resp.Error)
	}

	if err := mB.DisconnectPipe(streamID, pipeID, "node-a"); err != nil {
		t.Fatalf("DisconnectPipe failed: %v", err)
	}
	if err := mA.Unpublish(streamID); err != nil {
		t.Fatalf("Unpublish failed: %v", err)
	}
}

func TestManager_HandleClosePipeRequest(t *testing.T) {
	srv, ncA := setupNats(t)
	defer srv.Shutdown()
	defer ncA.Close()

	ncB, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer ncB.Close()

	mA := setupManager(t, "node-a", ncA)
	defer mA.Close()
	mB := setupManager(t, "node-b", ncB)
	defer mB.Close()

	streamID, err := mA.Publish()
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	pipeID, err := mB.ConnectPipe(streamID, "node-a")
	if err != nil {
		t.Fatalf("ConnectPipe failed: %v", err)
	}
	req := PipeRequest{Command: "ClosePipe", PipeConsumerId: pipeID}
	data, _ := json.Marshal(req)
	if _, err := ncB.Request("signaling.pipe.node-a", data, time.Second); err != nil {
		t.Fatalf("ClosePipe request failed: %v", err)
	}
	waitUntil(t, func() bool {
		mA.servingMu.Lock()
		count := len(mA.servingConsumers)
		mA.servingMu.Unlock()
		return count == 0
	})
	if err := mA.Unpublish(streamID); err != nil {
		t.Fatalf("Unpublish failed: %v", err)
	}
}

func TestManager_HandlePingPayload(t *testing.T) {
	srv, ncA := setupNats(t)
	defer srv.Shutdown()
	defer ncA.Close()

	ncB, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer ncB.Close()

	mA := setupManager(t, "node-a", ncA)
	defer mA.Close()
	mB := setupManager(t, "node-b", ncB)
	defer mB.Close()

	mA.ProbeRTT("node-b")
	waitUntil(t, func() bool {
		mA.pingMu.Lock()
		ch, ok := mA.pingChannels["node-b"]
		hasProducer := ok && ch.producer != nil
		mA.pingMu.Unlock()
		return hasProducer
	})

	pingData, _ := json.Marshal(pingMessage{Type: "ping", ID: "1", TS: time.Now().UnixNano()})
	mA.HandlePingPayload("node-b", pingData)
	pongData, _ := json.Marshal(pingMessage{Type: "pong", ID: "2", TS: time.Now().UnixNano()})
	mA.HandlePingPayload("node-b", pongData)
}

func TestManager_PipeCleanupSignaling(t *testing.T) {
	srv, ncA := setupNats(t)
	defer srv.Shutdown()
	defer ncA.Close()

	ncB, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer ncB.Close()

	mA := setupManager(t, "node-a", ncA)
	defer mA.Close()
	mB := setupManager(t, "node-b", ncB)
	defer mB.Close()

	streamID, err := mA.Publish()
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	if _, err := mB.ConnectPipe(streamID, "node-a"); err != nil {
		t.Fatalf("ConnectPipe failed: %v", err)
	}

	deadRouterID := mA.routers[0].Id()
	waitUntil(t, func() bool {
		mB.pipeMu.RLock()
		defer mB.pipeMu.RUnlock()
		for _, pipes := range mB.pipeTransports {
			if _, ok := pipes[deadRouterID]; ok {
				return true
			}
		}
		return false
	})

	mA.workers[0].Close()

	waitUntil(t, func() bool {
		mB.pipeMu.RLock()
		defer mB.pipeMu.RUnlock()
		for _, pipes := range mB.pipeTransports {
			if _, ok := pipes[deadRouterID]; ok {
				return false
			}
		}
		return true
	})
}

func TestManager_HandleEnsurePingChannelInvalid(t *testing.T) {
	srv, nc := setupNats(t)
	defer srv.Shutdown()
	defer nc.Close()

	m := setupManager(t, "node-ensure", nc)
	defer m.Close()

	req := PipeRequest{
		Command:       "EnsurePingChannel",
		RequesterNode: "node-x",
		StreamID:      types.StreamID(".producer"),
	}
	data, _ := json.Marshal(req)
	respMsg, err := nc.Request("signaling.pipe.node-ensure", data, time.Second)
	if err != nil {
		t.Fatalf("EnsurePingChannel request failed: %v", err)
	}
	var resp PipeResponse
	json.Unmarshal(respMsg.Data, &resp)
	if resp.Error == "" {
		t.Fatalf("expected error for invalid stream id")
	}
}

func TestManager_WorkerRestart(t *testing.T) {
	srv, nc := setupNats(t)
	defer srv.Shutdown()
	defer nc.Close()

	m := setupManager(t, "node-restart", nc)
	old := m.workers[0]
	old.Close()
	waitUntil(t, func() bool {
		return m.workers[0] != nil && m.workers[0] != old
	})
	m.Close()
}

func TestManager_PipeReuseBetweenManagers(t *testing.T) {
	srv, ncA := setupNats(t)
	defer srv.Shutdown()
	defer ncA.Close()

	ncB, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer ncB.Close()

	mA := setupManager(t, "node-a", ncA)
	defer mA.Close()
	mB := setupManager(t, "node-b", ncB)
	defer mB.Close()

	streamID, err := mA.Publish()
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	pipeID1, err := mB.ConnectPipe(streamID, "node-a")
	if err != nil {
		t.Fatalf("ConnectPipe failed: %v", err)
	}
	pipeID2, err := mB.ConnectPipe(streamID, "node-a")
	if err != nil {
		t.Fatalf("ConnectPipe failed: %v", err)
	}
	if pipeID1 == "" || pipeID2 == "" {
		t.Fatalf("expected pipe ids")
	}
	mB.producersMu.RLock()
	count := len(mB.producers)
	mB.producersMu.RUnlock()
	if count != 1 {
		t.Fatalf("expected one producer, got %d", count)
	}
	if err := mB.DisconnectPipe(streamID, pipeID1, "node-a"); err != nil {
		t.Fatalf("DisconnectPipe failed: %v", err)
	}
	if err := mA.Unpublish(streamID); err != nil {
		t.Fatalf("Unpublish failed: %v", err)
	}
}

func TestManager_ProbeRTTAndCleanUpNode(t *testing.T) {
	srv, ncA := setupNats(t)
	defer srv.Shutdown()
	defer ncA.Close()

	ncB, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer ncB.Close()

	mA := setupManager(t, "node-a", ncA)
	defer mA.Close()
	mB := setupManager(t, "node-b", ncB)
	defer mB.Close()

	mA.ProbeRTT("node-b")
	waitUntil(t, func() bool {
		mA.pingMu.Lock()
		_, ok := mA.pingChannels["node-b"]
		mA.pingMu.Unlock()
		return ok
	})
	mA.CleanUpNode("node-b")
	waitUntil(t, func() bool {
		mA.pingMu.Lock()
		_, ok := mA.pingChannels["node-b"]
		mA.pingMu.Unlock()
		if ok {
			return false
		}
		mA.pipeMu.RLock()
		defer mA.pipeMu.RUnlock()
		for _, pipes := range mA.pipeTransports {
			if len(pipes) != 0 {
				return false
			}
		}
		return true
	})
}

func TestManager_ProbeRTTSelf(t *testing.T) {
	srv, nc := setupNats(t)
	defer srv.Shutdown()
	defer nc.Close()

	m := setupManager(t, "node-self", nc)
	defer m.Close()

	if _, ok := m.ProbeRTT(m.nodeID); ok {
		t.Fatalf("expected ProbeRTT to fail for self")
	}
}

func TestManager_WorkerCloseCleanup(t *testing.T) {
	srv, ncA := setupNats(t)
	defer srv.Shutdown()
	defer ncA.Close()

	ncB, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer ncB.Close()

	mA := setupManager(t, "node-a", ncA)
	mB := setupManager(t, "node-b", ncB)

	streamID, err := mA.Publish()
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	if _, err := mA.Consume("client-1", streamID); err != nil {
		t.Fatalf("Consume failed: %v", err)
	}
	if _, err := mB.ConnectPipe(streamID, "node-a"); err != nil {
		t.Fatalf("ConnectPipe failed: %v", err)
	}
	dataStreamID, err := mA.PublishData()
	if err != nil {
		t.Fatalf("PublishData failed: %v", err)
	}
	if _, err := mB.ConnectPipe(dataStreamID, "node-a"); err != nil {
		t.Fatalf("ConnectPipe failed: %v", err)
	}
	waitUntil(t, func() bool {
		mA.servingMu.Lock()
		count := len(mA.servingConsumers)
		mA.servingMu.Unlock()
		return count > 0
	})
	worker := mA.workers[0]
	worker.Close()

	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		mA.producersMu.RLock()
		producersEmpty := len(mA.producers) == 0
		mA.producersMu.RUnlock()
		mA.clientsMu.RLock()
		clientsEmpty := len(mA.clients) == 0
		mA.clientsMu.RUnlock()
		mA.servingMu.Lock()
		servingEmpty := len(mA.servingConsumers) == 0
		mA.servingMu.Unlock()
		mA.servingDataMu.Lock()
		servingDataEmpty := len(mA.servingDataConsumers) == 0
		mA.servingDataMu.Unlock()
		mA.directMu.Lock()
		directEmpty := len(mA.directTransports) == 0
		mA.directMu.Unlock()
		mA.pipeMu.RLock()
		pipeEmpty := true
		for _, pipes := range mA.pipeTransports {
			if len(pipes) != 0 {
				pipeEmpty = false
				break
			}
		}
		mA.pipeMu.RUnlock()
		if producersEmpty && clientsEmpty && servingEmpty && servingDataEmpty && directEmpty && pipeEmpty {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	mA.producersMu.RLock()
	producersCount := len(mA.producers)
	mA.producersMu.RUnlock()
	mA.clientsMu.RLock()
	clientsCount := len(mA.clients)
	mA.clientsMu.RUnlock()
	mA.servingMu.Lock()
	servingCount := len(mA.servingConsumers)
	mA.servingMu.Unlock()
	mA.servingDataMu.Lock()
	servingDataCount := len(mA.servingDataConsumers)
	mA.servingDataMu.Unlock()
	mA.directMu.Lock()
	directCount := len(mA.directTransports)
	mA.directMu.Unlock()
	mA.pipeMu.RLock()
	pipeCount := 0
	for _, pipes := range mA.pipeTransports {
		pipeCount += len(pipes)
	}
	mA.pipeMu.RUnlock()
	if producersCount != 0 || clientsCount != 0 || servingCount != 0 || servingDataCount != 0 || directCount != 0 || pipeCount != 0 {
		t.Fatalf("cleanup incomplete: producers=%d clients=%d serving=%d serving_data=%d direct=%d pipe=%d", producersCount, clientsCount, servingCount, servingDataCount, directCount, pipeCount)
	}

	mA.Close()
	mB.Close()
}
