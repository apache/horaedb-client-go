
test:
	go clean -testcache && go test -v ./...

lint:
	golangci-lint -v run

tidy:
	go mod tidy

check:
	@ echo "check license ..."
	@ make check-license

check-license:
	@ sh ./scripts/check-license.sh