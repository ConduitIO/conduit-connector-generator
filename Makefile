.PHONY: build test

VERSION=$(shell git describe --tags --dirty --always)

build:
	go build -ldflags "-X 'github.com/conduitio/conduit-connector-generator.version=${VERSION}'" -o conduit-connector-generator cmd/connector/main.go

test:
	go test $(GOTEST_FLAGS) -race ./...

lint:
	golangci-lint run
