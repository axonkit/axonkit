MODULE   := github.com/axonkit/axonkit
BINARY   := axonkit
CMD_DIR  := ./cmd/axonkit

.PHONY: all build run test lint fmt vet tidy clean

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
