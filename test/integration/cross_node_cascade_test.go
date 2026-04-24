package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/axonkit/axonkit/pkg/config"
	"github.com/axonkit/axonkit/pkg/discovery"
	"github.com/axonkit/axonkit/pkg/media"
	"github.com/axonkit/axonkit/pkg/routing"
	"github.com/axonkit/axonkit/pkg/rtc"
	"github.com/axonkit/axonkit/pkg/store"
	"github.com/axonkit/axonkit/pkg/types"
	"github.com/jiyeyuran/mediasoup-go/v2"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// axonkitNode simulates a full AxonKit node with all routing & media components.
type axonkitNode struct {
	ID       types.NodeID
	Info     types.NodeInfo
	MediaMgr *media.Manager
	RIB      *routing.RIB
	FIB      *routing.FIB
	RoomMgr  *rtc.RoomManager
	LinkDB   *routing.LinkDB
	Conn     *nats.Conn
}

func (n *axonkitNode) Close() {
	if n.RoomMgr != nil {
		n.RoomMgr.Close()
	}
	// MediaMgr.Close() tears down the internal FIB; the RIB is owned by
	// this test harness and must be closed separately.
	if n.MediaMgr != nil {
		n.MediaMgr.Close()
	}
	n.RIB.Close()
	n.Conn.Close()
}

func skipWithoutWorker(t *testing.T) string {
	t.Helper()
	bin := os.Getenv("MEDIASOUP_WORKER_BIN")
	if bin == "" {
		t.Skip("MEDIASOUP_WORKER_BIN not set; skipping integration test")
	}
	return bin
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

func createAxonkitNode(t *testing.T, srv interface{ ClientURL() string }, nodeID, workerBin string) *axonkitNode {
	t.Helper()

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("NATS connect failed for %s: %v", nodeID, err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("JetStream init failed for %s: %v", nodeID, err)
	}

	nodeInfo := types.NodeInfo{
		ID:      types.NodeID(nodeID),
		Region:  "TestRegion",
		Cluster: "TestCluster",
	}

	kv, err := store.NewNatsStore(context.Background(), js, routing.RibBucketName, routing.RIBTTL)
	if err != nil {
		t.Fatalf("NATS store creation failed for %s: %v", nodeID, err)
	}

	rib := routing.NewRIB(kv, nodeInfo)
	linkDB := routing.NewLinkDB(nodeInfo)
	finder := discovery.NewFinder(rib, linkDB, nodeInfo)

	webRtcCfg := config.WebRtcConfig{ListenIP: "127.0.0.1"}
	mgr := media.NewManager(nodeInfo.ID, nc, workerBin, webRtcCfg,
		media.WithWorkerCount(1),
		media.WithRIB(rib, finder),
	)
	if err := mgr.Start(); err != nil {
		t.Fatalf("MediaManager start failed for %s: %v", nodeID, err)
	}

	roomKV, err := store.NewNatsStore(context.Background(), js, "ROOM_STATE", 0)
	if err != nil {
		t.Fatalf("room state store failed for %s: %v", nodeID, err)
	}
	roomLeaseKV, err := store.NewNatsStore(context.Background(), js, rtc.RoomLeaseBucketName, rtc.RoomLeaseBucketTTL)
	if err != nil {
		t.Fatalf("room lease store failed for %s: %v", nodeID, err)
	}

	roomMgr := rtc.NewRoomManager(rtc.RoomManagerParams{
		Config: config.RoomConfig{
			EmptyTimeout:     5 * time.Minute,
			DepartureTimeout: 30 * time.Second,
			MaxParticipants:  100,
			AutoSubscribe:    true,
			Dynacast:         true,
		},
		MediaManager: mgr,
		KV:           roomKV,
		LeaseKV:      roomLeaseKV,
		NodeID:       string(nodeInfo.ID),
	})

	return &axonkitNode{
		ID:       nodeInfo.ID,
		Info:     nodeInfo,
		MediaMgr: mgr,
		RIB:      rib,
		FIB:      mgr.FIB(),
		RoomMgr:  roomMgr,
		LinkDB:   linkDB,
		Conn:     nc,
	}
}

// publishTrackOnNode creates a producer via PlainTransport on the given node,
// registers it in the MediaManager and advertises via RIB (simulating what
// RoomManager.CascadeCallbacks.OnTrackPublished does).
func publishTrackOnNode(t *testing.T, node *axonkitNode) types.StreamID {
	t.Helper()
	routers := node.MediaMgr.Routers()
	if len(routers) == 0 {
		t.Fatal("no routers available")
	}
	router := routers[0]

	transport, err := router.CreatePlainTransport(&mediasoup.PlainTransportOptions{
		ListenInfo: mediasoup.TransportListenInfo{
			Ip:       "127.0.0.1",
			Protocol: mediasoup.TransportProtocolUDP,
		},
	})
	if err != nil {
		t.Fatalf("CreatePlainTransport failed: %v", err)
	}

	producer, err := transport.Produce(&mediasoup.ProducerOptions{
		RtpParameters: testRtpParameters(),
	})
	if err != nil {
		t.Fatalf("Produce failed: %v", err)
	}

	streamID := types.NewStreamID(router.Id(), producer.Id())
	// RegisterProducer also advertises via RIB when the MediaManager was
	// constructed with media.WithRIB(...).
	node.MediaMgr.RegisterProducer(streamID, producer)
	return streamID
}

func testRtpParameters() *mediasoup.RtpParameters {
	return &mediasoup.RtpParameters{
		Mid: "VIDEO",
		Codecs: []*mediasoup.RtpCodecParameters{
			{
				MimeType:    "video/h264",
				PayloadType: 112,
				ClockRate:   90000,
				Parameters: mediasoup.RtpCodecSpecificParameters{
					PacketizationMode: 1,
					ProfileLevelId:    "4d0032",
				},
				RtcpFeedback: []*mediasoup.RtcpFeedback{
					{Type: "nack"},
					{Type: "nack", Parameter: "pli"},
					{Type: "goog-remb"},
				},
			},
			{
				MimeType:    "video/rtx",
				PayloadType: 113,
				ClockRate:   90000,
				Parameters:  mediasoup.RtpCodecSpecificParameters{Apt: 112},
			},
		},
		HeaderExtensions: []*mediasoup.RtpHeaderExtensionParameters{
			{Uri: "urn:ietf:params:rtp-hdrext:sdes:mid", Id: 10},
			{Uri: "urn:3gpp:video-orientation", Id: 13},
		},
		Encodings: []*mediasoup.RtpEncodingParameters{
			{Ssrc: 22222222, Rtx: &mediasoup.RtpEncodingRtx{Ssrc: 22222223}},
		},
		Rtcp: &mediasoup.RtcpParameters{Cname: "video-1"},
	}
}

// TestCrossNodeSubscribe verifies the full cross-node cascade flow:
//
//  1. Two AxonKit nodes (NodeA, NodeB) share a NATS cluster.
//  2. A producer is published on NodeA → RIB advertisement propagates.
//  3. NodeB's RoomManager.EnsureStream triggers FIB.Subscribe → ConnectPipe
//     creates a pipe transport between managers.
//  4. The pipe producer becomes locally available on NodeB.
//  5. ReleaseStream triggers FIB.Unsubscribe → DisconnectPipe cleans up.
func TestCrossNodeSubscribe(t *testing.T) {
	workerBin := skipWithoutWorker(t)

	srv := setupNATS(t)
	defer srv.Shutdown()

	nodeA := createAxonkitNode(t, srv, "NodeA", workerBin)
	defer nodeA.Close()
	nodeB := createAxonkitNode(t, srv, "NodeB", workerBin)
	defer nodeB.Close()

	// --- Step 1: NodeA publishes a track ---
	streamID := publishTrackOnNode(t, nodeA)
	t.Logf("NodeA published stream: %s", streamID)

	// Wait for RIB propagation
	waitFor(t, func() bool {
		candidates, _ := nodeB.RIB.GetCandidates(streamID)
		return len(candidates) > 0
	}, 5*time.Second)
	t.Log("RIB propagation confirmed on NodeB")

	// --- Step 2: NodeB EnsureStream (simulates subscription) ---
	room := nodeB.RoomMgr.GetOrCreateRoom("test-room", 0, "")
	err := room.TestEnsureStream(string(streamID))
	if err != nil {
		t.Fatalf("EnsureStream on NodeB failed: %v", err)
	}
	t.Log("EnsureStream succeeded on NodeB")

	// --- Step 3: Verify pipe producer exists on NodeB ---
	waitFor(t, func() bool {
		return nodeB.MediaMgr.GetProducer(streamID) != nil
	}, 5*time.Second)
	t.Log("Pipe producer available on NodeB")

	lpB := nodeB.MediaMgr.GetProducer(streamID)
	if lpB == nil {
		t.Fatal("expected pipe producer on NodeB")
	}
	if lpB.Id() == "" {
		t.Fatal("expected non-empty pipe producer ID on NodeB")
	}
	t.Logf("NodeB has pipe producer: %s", lpB.Id())

	// --- Step 4: ReleaseStream (simulates consumer closing) ---
	room.TestReleaseStream(string(streamID))
	t.Log("ReleaseStream called on NodeB")

	// FIB should tear down the pipe after linger period.
	// For this test we just verify no panic and the call completes.

	// --- Step 5: NodeA stops advertising ---
	if err := nodeA.RIB.StopAdvertise(streamID); err != nil {
		t.Fatalf("StopAdvertise on NodeA failed: %v", err)
	}
	nodeA.MediaMgr.UnregisterProducer(streamID)
	t.Log("NodeA stopped advertising")

	// Verify RIB clears
	waitFor(t, func() bool {
		candidates, _ := nodeB.RIB.GetCandidates(streamID)
		return len(candidates) == 0
	}, 5*time.Second)
	t.Log("RIB cleared on NodeB ✓")
}

// TestCrossNodeDataStream tests data track (DataProducer) cascading.
func TestCrossNodeDataStream(t *testing.T) {
	workerBin := skipWithoutWorker(t)

	srv := setupNATS(t)
	defer srv.Shutdown()

	nodeA := createAxonkitNode(t, srv, "DataNodeA", workerBin)
	defer nodeA.Close()
	nodeB := createAxonkitNode(t, srv, "DataNodeB", workerBin)
	defer nodeB.Close()

	routers := nodeA.MediaMgr.Routers()
	router := routers[0]

	direct, err := router.CreateDirectTransport(&mediasoup.DirectTransportOptions{})
	if err != nil {
		t.Fatalf("CreateDirectTransport failed: %v", err)
	}

	dp, err := direct.ProduceData(&mediasoup.DataProducerOptions{
		Label: "test-data",
		SctpStreamParameters: &mediasoup.SctpStreamParameters{
			StreamId: 100,
		},
	})
	if err != nil {
		t.Fatalf("ProduceData failed: %v", err)
	}

	streamID := types.NewStreamID(router.Id(), dp.Id())
	// RegisterDataProducer also advertises via RIB when the MediaManager was
	// constructed with media.WithRIB(...).
	nodeA.MediaMgr.RegisterDataProducer(streamID, dp)

	waitFor(t, func() bool {
		candidates, _ := nodeB.RIB.GetCandidates(streamID)
		return len(candidates) > 0
	}, 5*time.Second)

	room := nodeB.RoomMgr.GetOrCreateRoom("data-room", 0, "")
	if err := room.TestEnsureStream(string(streamID)); err != nil {
		t.Fatalf("EnsureStream for data on NodeB failed: %v", err)
	}

	waitFor(t, func() bool {
		return nodeB.MediaMgr.GetProducer(streamID) != nil
	}, 5*time.Second)

	lpB := nodeB.MediaMgr.GetProducer(streamID)
	if lpB == nil || lpB.Id() == "" {
		t.Fatal("expected data pipe producer on NodeB")
	}
	t.Logf("Data pipe producer on NodeB: %s", lpB.Id())

	room.TestReleaseStream(string(streamID))
}
