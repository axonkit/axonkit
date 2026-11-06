<p align="center">
  <img src="https://raw.githubusercontent.com/axonkit/.github/main/profile/banner.svg" alt="AxonKit" width="480">
</p>

# AxonKit

AxonKit is a distributed real-time communication server built on [mediasoup](https://mediasoup.org/) and [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream). It combines SFU media processing with intelligent cross-node stream routing, providing a scalable foundation for video conferencing, live streaming, and multi-modal AI applications.

Documentation, guides, and demos are available at [axonkit.io](https://axonkit.io).

## Features

### Media & Signaling
- **mediasoup-based SFU**: WebRTC media processing via mediasoup workers, with WebRtcServer for efficient port multiplexing.
- **DataChannel RPC**: Custom text-based protocol (AXON/1.0) over DataChannel for low-latency bidirectional signaling, inspired by NATS wire protocol.
- **Auto-Subscribe**: Server-side subscription management — consumers are created paused, delivered to the client, then resumed to avoid media loss.
- **Dynacast**: Dynamically pauses video encodings not consumed by any subscriber, saving publisher CPU and bandwidth. Supports both simulcast layer control and track-level pause/resume (for SVC codecs).
- **AdaptiveStream**: Client-driven subscription optimization based on UI visibility and element size.

### Distributed System
- **Intelligent Routing**: RIB (Routing Information Base) and FIB (Forwarding Information Base) provide dynamic, low-latency stream routing across clusters and regions.
- **Cluster & Region Aware**: Prioritizes local cluster traffic; uses real-time RTT probing (LinkDB) for cross-region path selection.
- **Self-Healing**: Automatic failover on upstream node failure with make-before-break path switching.
- **Loop Detection**: Path vector validation prevents routing loops in complex topologies.
- **Node Discovery**: Nodes register and discover each other via NATS KV — no static configuration required.

### Client Management
- **Reconnection**: Clients reconnect with SyncState to reconcile track differences seamlessly.
- **Node Forwarding**: JoinRequest and ConnectRequest support `nodeId` for cross-node request proxying via NATS.
- **Heartbeat**: DataChannel-based ping/pong for accurate participant liveness detection.
- **Token Refresh**: Token renewal over DataChannel to avoid reconnection on token expiry.

## Architecture

```
Client (mediasoup-client)
  │
  ├── HTTP POST /rtc/join ──────► AxonKit Node
  │                                  ├── mediasoup Worker/Router/WebRtcServer
  │                                  ├── SendTransport + RecvTransport + DirectTransport
  │                                  └── DataChannel (AXON/1.0 RPC)
  │                                        ├── Client→Server: publish, subscribe, mute, ...
  │                                        └── Server→Client: subscription.create, notifications, ...
  │
  └── (Reconnect / Connect) ───► Same or forwarded node via NATS
```

## Getting Started

### Prerequisites

- Go 1.25+
- `mediasoup-worker` binary — fetched automatically by `make run` / `make test` (version-locked to mediasoup-go), or download a prebuilt worker manually from the [mediasoup releases](https://github.com/versatica/mediasoup/releases). No C++ build environment is required.

A NATS server is optional: when `-nats` is unset each node starts an embedded in-process NATS server with JetStream.

### Build & Run

```bash
git clone https://github.com/axonkit/axonkit.git
cd axonkit

# Install dependencies
go mod tidy

# Build
make build

# Run in dev mode (generates temporary API credentials and enables GET /dev/token)
make run

# Or with full options; -api-key/-api-secret are required outside -dev
# (otherwise the node fails fast at startup)
./bin/axonkit \
  -node node-bj-1 \
  -cluster NewYork \
  -region US \
  -port 7880 \
  -worker ./mediasoup-worker \
  -api-key my-api-key \
  -api-secret my-api-secret \
  -nats nats://localhost:4222
```

#### Configuring the mediasoup-worker path

AxonKit spawns the `mediasoup-worker` binary to run its media workers.
The path is resolved via the `-worker` flag and defaults to
`./mediasoup-worker` (a worker binary sitting next to where you launch
the node).

`make run` and `make test` populate `./mediasoup-worker` automatically:
`cmd/fetch-worker` reads the worker version from the mediasoup-go
dependency and downloads the matching prebuilt release (skipped when the
binary is already up to date). For a custom location, download manually:

```bash
# Download a prebuilt worker from https://github.com/versatica/mediasoup/releases
curl -LO https://github.com/versatica/mediasoup/releases/download/3.32.2/mediasoup-worker-3.32.2-linux-x64-kernel6.tgz
tar xzf mediasoup-worker-3.32.2-linux-x64-kernel6.tgz
chmod +x mediasoup-worker

# Pass the path explicitly
./bin/axonkit -worker /usr/local/bin/mediasoup-worker ...
```

Integration tests read the same setting from `MEDIASOUP_WORKER_BIN`
(`make test` sets it to the auto-downloaded binary).

### Development

```bash
# Format code
make fmt

# Run linter
make lint

# Run tests with race detector
make test

# All: format + lint + build
make all
```

## Protocol Overview

### Connection Flow

1. Client sends **HTTP POST** `/rtc/join` with token, room info, and optional DTLS fingerprints
2. Server creates transports and signal DataChannels, returns `JoinResponse`
3. Client sets up mediasoup-client device, creates transports, establishes signal DataChannel
4. Client sends `ready` RPC over DataChannel with RTP capabilities
5. Server responds with other participants and begins auto-subscribing tracks

### RPC Wire Format (AXON/1.0)

```
REQ <method> <logId>\r\n     -- request
RES <logId> <status>\r\n     -- response
MSG <method> [<logId>]\r\n   -- notification (logId optional)
[AXON/1.0\r\n                -- optional headers block
Key: Value\r\n
\r\n]
<body bytes>                 -- JSON or binary payload
```

### Multi-Node Routing

```
Publisher Node (New York)              Subscriber Node (San Francisco)
  ┌──────────┐                          ┌──────────┐
  │ Producer │ ── RIB Advertise ──►     │   FIB    │
  │          │                          │ Subscribe│
  │  Router  │ ◄── Pipe Transport ────  │  Router  │
  │          │       (media)            │          │
  └──────────┘                          └──────────┘
       │                                      │
    NATS KV                                NATS KV
    (Routes)                              (Routes)
```

## License

Licensed under the [Apache License, Version 2.0](./LICENSE).
