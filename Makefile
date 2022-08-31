
test:
	go clean -testcache && go test -v ./...

lint:
	golangci-lint -v run

tidy:
	go mod tidy
