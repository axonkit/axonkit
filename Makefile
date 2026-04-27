MODULE   := github.com/axonkit/axonkit
BINARY   := axonkit
CMD_DIR  := ./cmd/axonkit

.PHONY: all build run test lint fmt vet tidy clean verify-broadcast verify-broadcast-avflow verify-broadcast-rtmp

all: fmt lint build

build:
	go build -o bin/$(BINARY) $(CMD_DIR)

run: build
	./bin/$(BINARY) -debug

test:
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

# verify-broadcast-avflow runs the same pipeline but drives the mix leg
# through a real avflow (headless Chrome + WHIP) stack instead of an
# ffmpeg publisher. Requires an avflow checkout next to axonkit (or
# AVFLOW_REPO=...) with its web bundle built. Artefacts land in
# test/broadcast/out/avflow-<timestamp>/.
verify-broadcast-avflow:
	./test/broadcast/verify-avflow.sh

# verify-broadcast-rtmp pushes an H.264-with-B-frames + Opus stream to
# axonkit over RTMP, fans it out through /rtc/broadcast to a local
# ffmpeg RTMP recorder, and asserts the recorded MP4 decodes without
# errors. Exercises the RTMP ingest path (-rtmp-listen), the FLV
# B-frame composition-time handling, and the Enhanced-RTMP "Opus"
# audio FourCC. Artefacts land in test/broadcast/out/rtmp-<timestamp>/.
verify-broadcast-rtmp:
	./test/broadcast/verify-rtmp.sh
