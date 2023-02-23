
default: build

GO_TOOLS_BIN_PATH := $(shell pwd)/.tools/bin
PATH := $(GO_TOOLS_BIN_PATH):$(PATH)
SHELL := env PATH='$(PATH)' GOBIN='$(GO_TOOLS_BIN_PATH)' $(shell which bash)

install-tools:
	@mkdir -p $(GO_TOOLS_BIN_PATH)
	@(which golangci-lint && golangci-lint version | grep '1.51') >/dev/null 2>&1 || curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GO_TOOLS_BIN_PATH) v1.51.2
	@go install github.com/AlekSi/gocov-xml@v1.1.0
	@go install github.com/axw/gocov/gocov@v1.1.0
	@go install github.com/mgechev/revive@v1.2.5
	@go install golang.org/x/tools/cmd/goimports@latest
	@go install gotest.tools/gotestsum@latest

PKG := github.com/CeresDB/ceresdb-client-go
PACKAGES := $(shell go list ./... | tail -n +2)
PACKAGE_DIRECTORIES := $(subst $(PKG)/,,$(PACKAGES))

check: install-tools
	@ echo "check license ..."
	@ make check-license
	@ echo "gofmt ..."
	@ gofmt -s -l -d $(PACKAGE_DIRECTORIES) 2>&1 | awk '{ print } END { if (NR > 0) { exit 1 } }'
	@ echo "golangci-lint ..."
	@ golangci-lint run $(PACKAGE_DIRECTORIES)
	@ echo "revive ..."
	@ revive -formatter friendly -config revive.toml $(PACKAGES)

check-license:
	@ sh ./tools/check-license.sh

test: install-tools
	@ echo "go test ..."
	@ go test -timeout 5m -race -cover $(PACKAGES)

tidy:
	go mod tidy