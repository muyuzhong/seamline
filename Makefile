SHELL := /bin/bash
GO ?= go
BIN_DIR := bin
GOLANGCI_LINT_VERSION := $(shell tr -d '[:space:]' < .golangci-lint-version 2>/dev/null || echo v2.1.6)

.DEFAULT_GOAL := help

.PHONY: help fmt fmt-check vet lint install-lint test test-race build run-demo check clean

help: ## Show available Makefile targets
	@echo "Seamline — Resumable Dual-Speed AI Stream Broker"
	@echo ""
	@echo "Usage: make <target>"
	@echo ""
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z0-9_-]+:.*## / {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

fmt: ## Format all Go source files with gofmt
	gofmt -w .

fmt-check: ## Verify all Go files are formatted with gofmt
	@unformatted="$$(gofmt -l .)"; \
	if [ -n "$$unformatted" ]; then \
		echo "The following Go files must be formatted with 'make fmt' (gofmt):"; \
		echo "$$unformatted"; \
		exit 1; \
	fi

vet: ## Run go vet static analysis across all packages
	$(GO) vet ./...

lint: ## Run golangci-lint using .golangci.yml
	@if ! command -v golangci-lint >/dev/null 2>&1; then \
		echo "golangci-lint not found. Run 'make install-lint' to install $(GOLANGCI_LINT_VERSION)."; \
		exit 1; \
	fi
	golangci-lint run --timeout 5m --verbose ./...

install-lint: ## Install pinned golangci-lint version from .golangci-lint-version
	$(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

test: ## Run unit and integration tests
	$(GO) test -count=1 -v ./...

test-race: ## Run tests with Go race detector enabled (-count=2)
	$(GO) test -race -count=2 -v ./...

build: ## Build static seamline and simworker binaries (CGO_ENABLED=0) into ./bin
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 $(GO) build -trimpath -o $(BIN_DIR)/seamline ./cmd/seamline
	CGO_ENABLED=0 $(GO) build -trimpath -o $(BIN_DIR)/simworker ./cmd/simworker

run-demo: ## Start Seamline server with embedded F5 Live Playground on :8080
	$(GO) run ./cmd/seamline --addr :8080 --demo=true

check: fmt-check vet test-race ## Run mandatory pre-PR verification gate (fmt-check + vet + test-race)

clean: ## Remove compiled ./bin artifacts
	rm -rf $(BIN_DIR)
