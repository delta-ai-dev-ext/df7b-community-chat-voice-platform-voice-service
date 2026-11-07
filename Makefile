SERVICE_NAME ?= voice_service

.PHONY: install test cover build run lint

install:
	go mod download

test:
	go test ./... -race -count=1 -parallel 4

cover:
	go test ./... -race -count=1 -parallel 4 -coverpkg=./internal/... -coverprofile=coverage.out
	go tool cover -func=coverage.out | tail -1

build:
	docker build -t $(SERVICE_NAME):local .

run:
	go run ./cmd/server

lint:
	golangci-lint run ./...
