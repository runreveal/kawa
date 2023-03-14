VERSION := $(shell git describe --tags --always --dirty="-dev")
LDFLAGS := -ldflags='-X "main.version=$(VERSION)"'
# GCFLAGS := -gcflags='-G=3'
GO = go
Q = @

GOTESTFLAGS = -race $(GCFLAGS)
ifndef Q
	GOTESTFLAGS += -v
endif

GOTAGS = testing

.PHONY: test
test: vet
	$Q$(GO) test -vet=off -tags='$(GOTAGS)' $(GOTESTFLAGS) -coverpkg="./..." -coverprofile=.coverprofile ./...
	$Qgrep -v 'cmd' < .coverprofile > .covprof && mv .covprof .coverprofile
	$Q$(GO) tool cover -func=.coverprofile

.PHONY: lint
lint: $(GOPATH)/bin/golangci-lint
	golangci-lint run --timeout 5m ./...

$(GOPATH)/bin/golangci-lint:
	$(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.51.2

.PHONY: build
build: vet
	$Q$(GO) build $(LDFLAGS) $(GCFLAGS) -o ./build/$(NAME) ./...

.PHONY: vet
vet:
	$Q$(GO) vet ./...

.PHONY: fmtcheck
fmtchk:
	$Qexit $(shell goimports -l . | grep -v '^vendor' | wc -l)

.PHONY: fmtfix
fmtfix:
	$Qgoimports -w $(shell find . -iname '*.go' | grep -v vendor)

