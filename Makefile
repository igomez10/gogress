.PHONY: build test

build:
	go build -o gogress-cli cli/main.go

test:
	go test ./...
