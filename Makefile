.PHONY: build test

build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-generator.version=${VERSION}'" -o conduit-connector-algolia cmd/connector/main.go

test:
	go test $(GOTEST_FLAGS) -race ./...

lint:
	golangci-lint run
