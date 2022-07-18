.PHONY: build test

build:
	go build -o conduit-connector-generator cmd/generator/main.go

test:
	go test $(GOTEST_FLAGS) -race ./...

lint:
	golangci-lint run
