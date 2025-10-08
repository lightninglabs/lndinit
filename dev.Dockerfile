ARG GO_VERSION=1.24.6
ARG BASE_IMAGE=lightninglabs/lnd
ARG BASE_IMAGE_VERSION=v0.20.0-beta.rc1

FROM golang:${GO_VERSION}-alpine as builder

# Allow defining the CGO_ENABLED variable so we can build binaries
# that will work in a different type of container.
ARG CGO_ENABLED=1

# Force Go to use the cgo based DNS resolver. This is required to ensure DNS
# queries required to connect to linked containers succeed.
ENV GODEBUG netdns=cgo

# Copy in the local repository to build from.
COPY . /go/src/github.com/lightninglabs/lndinit

# Install dependencies and build the binaries.
RUN apk add --no-cache --update alpine-sdk \
  git \
  make \
  &&  cd /go/src/github.com/lightninglabs/lndinit \
  &&  make install

# Start a new, final image.
FROM ${BASE_IMAGE}:${BASE_IMAGE_VERSION} as final

# Copy the binary from the builder image.
COPY --from=builder /go/bin/lndinit /bin/
