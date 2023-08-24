VERSION := $(shell git describe --tags --always --dirty="-dev")
LDFLAGS := -ldflags='-X "main.version=$(VERSION)"'

GOTESTFLAGS = -race
GOTAGS = testing

GO ?= $(shell which go)

export GOEXPERIMENT=nocoverageredesign

.PHONY: test
test: compose
	$(GO) test -vet=off -tags='$(GOTAGS)' $(GOTESTFLAGS) -coverpkg="./..." -coverprofile=.coverprofile ./...
	grep -v 'cmd' < .coverprofile > .covprof && mv .covprof .coverprofile
	$(GO) tool cover -func=.coverprofile

.PHONY: coverage
coverage:
	$(GO) tool cover -html=.coverprofile

.PHONY: version
version:
	@echo $(VERSION)

.PHONY: dist
dist:
	goreleaser release --config .goreleaser.public.yaml --clean --snapshot

.PHONY: compose
compose:
	docker-compose up -d

.PHONY: lint
lint: $(GOPATH)/bin/golangci-lint
	golangci-lint run --timeout 5m ./...

$(GOPATH)/bin/golangci-lint:
	$(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.51.2

$(GOPATH)/bin/golines:
	$(GO) install github.com/segmentio/golines@latest

.PHONY: fmtcheck
fmtcheck: $(GOPATH)/bin/golines
	exit $(shell golines -m 128 -l . | wc -l)

.PHONY: fmtfix
fmtfix: $(GOPATH)/bin/golines
	golines -m 128 -w .

.PHONY: clean
clean:
	rm -rf dist
