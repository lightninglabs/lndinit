PKG := github.com/lightninglabs/lndinit
ESCPKG := github.com\/lightninglabs\/lndinit
TOOLS_DIR := tools

GO_BIN := ${GOPATH}/bin
GOIMPORTS_BIN := $(GO_BIN)/gosimports

COMMIT := $(shell git describe --tags --dirty)
COMMIT_HASH := $(shell git rev-parse HEAD)

GOBUILD := go build -v
GOINSTALL := go install -v
GOTEST := go test -v
DOCKER_TOOLS := docker run -v $$(pwd):/build lndinit-tools

GOFILES_NOVENDOR = $(shell find . -type f -name '*.go' -not -path "./vendor/*")

RM := rm -f
CP := cp
MAKE := make
XARGS := xargs -L 1

VERSION_TAG = $(shell git describe --tags)

DEV_TAGS = kvdb_etcd kvdb_postgres
RELEASE_TAGS = $(DEV_TAGS)

BUILD_SYSTEM = darwin-amd64 \
linux-386 \
linux-amd64 \
linux-armv6 \
linux-armv7 \
linux-arm64 \
windows-386 \
windows-amd64 \
windows-arm

# By default we will build all systems. But with the 'sys' tag, a specific
# system can be specified. This is useful to release for a subset of
# systems/architectures.
ifneq ($(sys),)
BUILD_SYSTEM = $(sys)
endif

ifneq ($(tag),)
VERSION_TAG = $(tag)
endif

# We only return the part inside the double quote here to avoid escape issues
# when calling the external release script. The second parameter can be used to
# add additional ldflags if needed (currently only used for the release).
make_ldflags = $(2) -X $(PKG).Commit=$(COMMIT)

DEV_GCFLAGS := -gcflags "all=-N -l"
LDFLAGS := -ldflags "$(call make_ldflags, $(DEV_TAGS), -s -w)"
DEV_LDFLAGS := -ldflags "$(call make_ldflags, $(DEV_TAGS))"

# For the release, we want to remove the symbol table and debug information (-s)
# and omit the DWARF symbol table (-w). Also we clear the build ID.
RELEASE_LDFLAGS := $(call make_ldflags, $(RELEASE_TAGS), -s -w -buildid=)

GREEN := "\\033[0;32m"
NC := "\\033[0m"
define print
	echo $(GREEN)$1$(NC)
endef

default: scratch

all: scratch install

# ============
# DEPENDENCIES
# ============
$(GOIMPORTS_BIN):
	@$(call print, "Installing goimports.")
	cd $(TOOLS_DIR); go install -trimpath $(GOIMPORTS_PKG)

# ============
# INSTALLATION
# ============

build:
	@$(call print, "Building debug lndinit.")
	$(GOBUILD) -tags="$(DEV_TAGS)" -o lndinit-debug $(DEV_GCFLAGS) $(DEV_LDFLAGS) $(PKG)

install:
	@$(call print, "Installing lndinit.")
	$(GOINSTALL) -tags="$(DEV_TAGS)" $(LDFLAGS) $(PKG)

release-install:
	@$(call print, "Installing release lndinit.")
	env CGO_ENABLED=0 $(GOINSTALL) -v -trimpath -ldflags="$(RELEASE_LDFLAGS)" -tags="$(RELEASE_TAGS)" $(PKG)

release:
	@$(call print, "Creating release of lndinit.")
	./release.sh build-release "$(VERSION_TAG)" "$(BUILD_SYSTEM)" "$(RELEASE_LDFLAGS)"

docker-tools:
	@$(call print, "Building tools docker image.")
	docker build -q -t lndinit-tools $(TOOLS_DIR)

scratch: build

# =========
# UTILITIES
# =========

unit: 
	@$(call print, "Running unit tests.")
	$(GOTEST) ./...

fmt: $(GOIMPORTS_BIN)
	@$(call print, "Fixing imports.")
	gosimports -w $(GOFILES_NOVENDOR)
	@$(call print, "Formatting source.")
	gofmt -l -w -s $(GOFILES_NOVENDOR)

lint: docker-tools
	@$(call print, "Linting source.")
	$(DOCKER_TOOLS) golangci-lint run -v $(LINT_WORKERS)

vendor:
	@$(call print, "Re-creating vendor directory.")
	rm -r vendor/; go mod vendor

clean:
	@$(call print, "Cleaning source.$(NC)")
	$(RM) ./lndinit-debug
	$(RM) -r ./vendor .vendor-new

.PHONY: all \
	default \
	build \
	install \
	scratch \
	fmt \
	lint \
	vendor \
	clean
