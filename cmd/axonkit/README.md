# axonkit Demo

This demo illustrates a distributed media routing scenario using RIB (Routing Information Base) and FIB (Forwarding Information Base) within **axonkit**. It simulates edge nodes in different regions (CN, US-A, US-B) connecting via a NATS backbone.

## Prerequisites

- **Go 1.21+**
- **NATS Server** (with JetStream enabled)
  - Run locally: `nats-server -js`
- **mediasoup-worker** binary
  - Must be available locally. Update the `-worker` flag if it's not in the current directory.

## Building

```bash
go build -o mediasoup-demo ./cmd/mediasoup-demo
```

## Running the Demo

Start three separate terminal sessions to simulate three edge nodes.

### 1. CN-Edge Node (Publisher)

Located in Beijing, China. This node will host the original stream.

```bash
./mediasoup-demo \
  -node CN-Edge \
  -region CN \
  -cluster Beijing \
  -port 8080 \
  -worker ./mediasoup-worker
```

### 2. US-A-Edge Node (Subscriber)

Located in New York, USA.

```bash
./mediasoup-demo \
  -node US-A-Edge \
  -region US-A \
  -cluster NewYork \
  -port 8081 \
  -worker ./mediasoup-worker
```

### 3. US-B-Edge Node (Subscriber)

Located in San Francisco, USA.

```bash
./mediasoup-demo \
  -node US-B-Edge \
  -region US-B \
  -cluster SanFrancisco \
  -port 8082 \
  -worker ./mediasoup-worker
```

## Testing

Once all nodes are running, use `curl` to simulate client actions.

### 1. Publish a Stream

Publish `stream-1` to the CN-Edge node.

```bash
curl "http://localhost:8080/publish?stream=stream-1"
```

### 2. Subscribe from US-A

Client `client-1` connects to US-A-Edge to watch `stream-1`.

```bash
curl "http://localhost:8081/subscribe?stream=stream-1&client=client-1"
```

### 3. Subscribe from US-B

Client `client-2` connects to US-B-Edge to watch `stream-1`.

```bash
curl "http://localhost:8082/subscribe?stream=stream-1&client=client-2"
```

## Cleanup

To stop all demo processes:

```bash
ps aux | grep mediasoup-demo | awk '{print $2}' | xargs kill
```
