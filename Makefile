default: build

GO_TOOLS_BIN_PATH := $(shell pwd)/.tools/bin
PATH := $(GO_TOOLS_BIN_PATH):$(PATH)
SHELL := env PATH='$(PATH)' GOBIN='$(GO_TOOLS_BIN_PATH)' $(shell which bash)

install-tools:
	@mkdir -p $(GO_TOOLS_BIN_PATH)
	@(which golangci-lint && golangci-lint version | grep '1.51') >/dev/null 2>&1 || curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GO_TOOLS_BIN_PATH) v1.51.2
	@go install github.com/mgechev/revive@v1.2.5
	@go install golang.org/x/tools/cmd/goimports@latest

PKG := github.com/CeresDB/horaedb-client-go
PACKAGES := $(shell go list ./... | tail -n +2)
PACKAGE_DIRECTORIES := $(subst $(PKG)/,,$(PACKAGES))

lint:
	golangci-lint run -v
	revive -formatter friendly -config revive.toml ./...

check-license:
	docker run --rm -v $(shell pwd):/github/workspace ghcr.io/korandoru/hawkeye-native:v3 check

test:
	go test -timeout 5m -race -cover ./...

tidy:
	go mod tidy

.PHONY: test check tidy check-license install-tools
