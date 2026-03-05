# AxonKit

AxonKit is a distributed real-time communication server built on [mediasoup](https://mediasoup.org/) and [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream). It combines SFU media processing with intelligent cross-node stream routing, providing a scalable foundation for video conferencing, live streaming, and multi-modal AI applications.

## Features

### Media & Signaling
- **mediasoup-based SFU**: WebRTC media processing via mediasoup workers, with WebRtcServer for efficient port multiplexing.
- **DataChannel RPC**: Custom text-based protocol (OMNI/1.0) over DataChannel for low-latency bidirectional signaling, inspired by NATS wire protocol.
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
  │                                  └── DataChannel (OMNI/1.0 RPC)
  │                                        ├── Client→Server: publish, subscribe, mute, ...
  │                                        └── Server→Client: subscription.create, notifications, ...
  │
  └── (Reconnect / Connect) ───► Same or forwarded node via NATS
```

### Core Components

| Package | Description |
|---------|-------------|
| `pkg/server` | HTTP API endpoints (`/rtc/join`, `/rtc/connect`) and NATS RPC handler |
| `pkg/rtc` | Room and participant lifecycle, signal message handling, dynacast |
| `pkg/media` | mediasoup worker/router/WebRtcServer management, pipe transport |
| `pkg/proto` | Protocol definitions — RPC wire format, request/response types, models |
| `pkg/routing` | RIB, FIB, and LinkDB for distributed stream routing |
| `pkg/discovery` | Node registry and stream finder via NATS KV |
| `pkg/auth` | JWT token generation and verification |
| `pkg/config` | Configuration management |
| `pkg/store` | Abstraction layer for NATS KV and in-memory stores |
| `cmd/axonkit` | Main application entry point |

## Getting Started

### Prerequisites

- Go 1.25+
- NATS Server with JetStream enabled
- mediasoup system dependencies (C++ build tools for mediasoup worker)

### Build & Run

```bash
git clone https://github.com/axonkit/axonkit.git
cd axonkit

# Install dependencies
go mod tidy

# Build
make build

# Run in debug mode (no token verification)
make run

# Or with full options
./bin/axonkit \
  -config config.yaml \
  -node node-bj-1 \
  -cluster Beijing \
  -region CN \
  -port 7880 \
  -nats nats://localhost:4222
```

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

### RPC Wire Format (OMNI/1.0)

```
REQ <method> <logId>\r\n     -- request
RES <logId> <status>\r\n     -- response
MSG <method> [<logId>]\r\n   -- notification (logId optional)
[OMNI/1.0\r\n                -- optional headers block
Key: Value\r\n
\r\n]
<body bytes>                 -- JSON or binary payload
```

### Multi-Node Routing

```
Publisher Node (Beijing)              Subscriber Node (San Francisco)
  ┌──────────┐                          ┌──────────┐
  │ Producer  │ ── RIB Advertise ──►    │   FIB    │
  │          │                          │ Subscribe │
  │  Router  │ ◄── Pipe Transport ──── │  Router  │
  │          │       (media)            │          │
  └──────────┘                          └──────────┘
       │                                      │
    NATS KV                                NATS KV
    (Routes)                              (Routes)
```

## License

MIT
