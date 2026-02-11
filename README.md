# OmniStream

OmniStream is a distributed, intelligent media routing network built on top of [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream) and mediasoup. It leverages RIB (Routing Information Base) and FIB (Forwarding Information Base) concepts from traditional networking to provide dynamic, low-latency stream routing across clusters and regions.

## Features

- **Distributed Routing**: Automatically finds the optimal path for media streams across a mesh of nodes.
- **Dynamic Topology**: Nodes automatically discover each other via NATS; no static configuration required.
- **Intelligent Path Selection**:
  - **Cluster-Aware**: Prioritizes local cluster traffic to minimize latency and cost.
  - **Region-Aware**: Prefers same-region nodes when local cluster is unavailable.
  - **QoS-Based**: Uses real-time RTT (Round Trip Time) probing to select the best link.
- **Self-Healing**: Automatically detects node failures and reroutes streams to healthy upstream nodes.
- **Loop Detection**: Prevents routing loops using path vector validation.
- **Media Agnostic**: Designed to route generic streams (conceptually), with specific implementations for mediasoup.

## Architecture

OmniStream separates the control plane (Routing) from the data plane (Forwarding):

- **RIB (Routing Information Base)**:
  - Maintains the global view of available streams and their routes.
  - Stores routing data in NATS Key-Value buckets for distributed access.
  - Handles route advertisement and path calculation.
- **FIB (Forwarding Information Base)**:
  - Manages local active subscriptions on a specific node.
  - Handles "Pipe" connections (transport) between nodes.
  - Monitors upstream health and triggers failover.
- **LinkDB**:
  - Maintains real-time link quality statistics (RTT, Packet Loss).
  - Performs background probing between clusters to update cost metrics dynamically.

## Getting Started

### Prerequisites

- Go 1.25+
- NATS Server (with JetStream enabled)

### Installation

```bash
git clone https://github.com/jiyeyuran/omnistream.git
cd omnistream
go mod tidy
```

### Running the Demo

The project includes a demo scenario (`cmd/omnistream`) that simulates nodes in different regions (e.g., Beijing, NewYork, SanFrancisco).

1.  **Start NATS Server**:

    ```bash
    nats-server -js
    ```

2.  **Run Node 1 (Beijing)**:

    ```bash
    go run ./cmd/omnistream -node node-bj-1 -cluster Beijing -region CN
    ```

3.  **Run Node 2 (San Francisco)**:

    ```bash
    go run ./cmd/omnistream -node node-sf-1 -cluster SanFrancisco -region US-W
    ```

4.  **Publish a Stream (on Beijing Node)**:

    ```bash
    curl "http://localhost:8080/publish?stream=stream-1"
    ```

5.  **Subscribe to Stream (on SF Node)**:
    ```bash
    # Assuming SF node is on port 8081 (you may need to adjust flags to avoid port conflict)
    curl "http://localhost:8081/subscribe?stream=stream-1&client=client-1"
    ```

## Project Structure

- `pkg/discovery`: Node discovery and registry via NATS.
- `pkg/routing`: Core RIB, FIB, and LinkDB implementations.

* `pkg/store`: Abstraction layer for NATS KV Store.
* `cmd/omnistream`: Real application demonstrating the routing logic.
* `cmd/router`: A standalone router component (WIP).

## License

MIT
