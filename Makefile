
# install CLI (must use this when testing so that latest config is always used)
.PHONY: cli
cli:
	sudo cp conns_config.toml /usr/local/etc
	go install ./internal/cmd/connscli

# run all tests
.PHONY: tests
tests: 
	go test -race ./...
