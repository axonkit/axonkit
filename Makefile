MODULE   := github.com/axonkit/axonkit
BINARY   := axonkit
CMD_DIR  := ./cmd/axonkit

# Worker binary for integration tests. Defaults to the binary fetched by the
# worker target below; an existing MEDIASOUP_WORKER_BIN takes precedence.
export MEDIASOUP_WORKER_BIN ?= $(CURDIR)/mediasoup-worker

.PHONY: all build run test lint fmt vet tidy clean worker verify-broadcast verify-broadcast-rtmp

all: fmt lint build

build:
	go build -o bin/$(BINARY) $(CMD_DIR)

# worker downloads the prebuilt mediasoup-worker binary that matches the
# mediasoup-go version in go.mod (see cmd/fetch-worker). run and test depend
# on it so a fresh checkout is runnable without manual setup.
worker:
	go run ./cmd/fetch-worker

run: worker build
	./bin/$(BINARY) -dev

test: worker
	go test ./... -race -count=1

lint:
	golangci-lint run ./...

fmt:
	gofmt -s -w .

vet:
	go vet ./...

tidy:
	go mod tidy

clean:
	rm -rf bin/

# verify-broadcast spins up a loopback axonkit + nats-server, publishes two
# fake room users via WHIP, exercises the /rtc/broadcast live→mix→live
# switch with an ffmpeg-driven WHIP mix source, and records the output
# to test/broadcast/out/ffmpeg-<timestamp>/ as an MP4 plus three probe
# frames. Fails if either switch direction does not appear in the
# recording.
#
# VIDEO_CODEC=h264|vp9|av1 selects the publisher codec (case-insensitive) —
# VP9/AV1 exercise the Enhanced-RTMP egress path. See
# test/broadcast/README.md for the full env var reference.
verify-broadcast:
	./test/broadcast/verify.sh

# verify-broadcast-rtmp pushes an H.264-with-B-frames + Opus stream to
# axonkit over RTMP, fans it out through /rtc/broadcast to a local
# ffmpeg RTMP recorder, and asserts the recorded MP4 decodes without
# errors. Exercises the RTMP ingest path (-rtmp-listen), the FLV
# B-frame composition-time handling, and the Enhanced-RTMP "Opus"
# audio FourCC. Artefacts land in test/broadcast/out/rtmp-<timestamp>/.
verify-broadcast-rtmp:
	./test/broadcast/verify-rtmp.sh
