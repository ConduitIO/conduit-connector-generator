.PHONY: build test

build:
	go build -o conduit-plugin-generator cmd/generator/main.go

test:
	go test $(GOTEST_FLAGS) -race ./...

