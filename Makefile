BINARY     := cube
CTL_BINARY := cubectl
MODULE     := github.com/Shreyas9468/cube
BUILD_DIR  := bin

# Read version from git tag, fallback to dev
VERSION    := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT     := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TIME := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")

LDFLAGS := -s -w \
  -X '$(MODULE)/version.Version=$(VERSION)' \
  -X '$(MODULE)/version.Commit=$(COMMIT)' \
  -X '$(MODULE)/version.BuildTime=$(BUILD_TIME)'

.PHONY: all build build-ctl run fmt vet lint test clean help

## all: format, vet, and build both binaries
all: fmt vet build build-ctl

## build: compile the cube orchestrator binary
build:
	@echo ">> Building $(BINARY) $(VERSION) ..."
	@go build -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY) .

## build-ctl: compile the cubectl CLI binary
build-ctl:
	@echo ">> Building $(CTL_BINARY) $(VERSION) ..."
	@go build -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(CTL_BINARY) ./cmd/cubectl

## run: build and launch the orchestrator (creates data/ dir if needed)
run: build
	@mkdir -p data
	@./$(BUILD_DIR)/$(BINARY)

## fmt: format all Go source files
fmt:
	@echo ">> Formatting ..."
	@go fmt ./...

## vet: run the Go static analyser
vet:
	@echo ">> Vetting ..."
	@go vet ./...

## test: run all unit tests with race detection
test:
	@echo ">> Testing ..."
	@go test -race -v ./...

## test-cover: run tests and open HTML coverage report
test-cover:
	@go test -race -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html
	@echo ">> Coverage report: coverage.html"

## lint: run golangci-lint (must be installed separately)
lint:
	@golangci-lint run ./...

## clean: remove built artifacts and databases
clean:
	@echo ">> Cleaning ..."
	@rm -rf $(BUILD_DIR) data/ coverage.out coverage.html

## help: list all targets with descriptions
help:
	@grep -E '^##' $(MAKEFILE_LIST) | sed 's/## //'
