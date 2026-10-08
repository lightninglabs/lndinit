# contrib

Developer helper scripts. These are not part of the `lndinit` binary or its
release build; they are convenience tools for local development.

## build-dev-image

Builds a development `lndinit` image from the **current worktree**, layered on
top of an `lnd` (or `litd`) base that is built from a context you provide.

This is the local-dev twin of the
[`docker.yml`](../.github/workflows/docker.yml) release workflow: that workflow
layers `lndinit` onto a *released* base when a `docker/v*` tag is pushed; this
script lets you do the same thing manually against an *unreleased* base — a
local checkout, a branch, a fork, a PR, or a specific commit — while iterating.

`lndinit` is built, by default, from the worktree the script lives in (including
any uncommitted changes), so you usually pass just the base context — and even
that defaults to upstream `lnd` master (`lightning-terminal` master with
`--litd`) when omitted. You never have to point at `lndinit` explicitly, but
`--lndinit-context` lets you.

### Requirements

Docker with [BuildKit](https://docs.docker.com/build/buildkit) / `buildx`. Both
`dev.Dockerfile`s (the base's and `lndinit`'s) use BuildKit cache mounts, so
every build goes through `docker buildx build`. Override the container tool with
`CONTAINER_TOOL=` (or `DOCKER=`) if you don't use `docker`.

### Usage

```sh
contrib/build-dev-image [options] <base-context>
```

`<base-context>` is a local path (e.g. `../lnd`), a git URL with an optional
`#ref` (branch, tag, or commit), or a GitHub PR URL
(`https://github.com/<owner>/<repo>/pull/<n>`). It defaults to upstream `lnd`
master (`https://github.com/lightningnetwork/lnd.git#master`), or
`lightning-terminal` master with `--litd`, when omitted.

The script builds the base from that context using the base's own
`dev.Dockerfile` (tagged `lnd-dev:<base-tag>`), then layers this worktree's
`lndinit` on top (tagged `lndinit:lnd-dev-<base-tag>` by default), passing
`BASE_IMAGE` / `BASE_IMAGE_VERSION` into `lndinit`'s `dev.Dockerfile`.

The script bundles no Dockerfile of its own — each build invokes the
project's own `dev.Dockerfile`:

- lnd: [`dev.Dockerfile`](https://github.com/lightningnetwork/lnd/blob/master/dev.Dockerfile)
- litd: [`dev.Dockerfile`](https://github.com/lightninglabs/lightning-terminal/blob/master/dev.Dockerfile)
- lndinit: [`dev.Dockerfile`](../dev.Dockerfile)

`<base-tag>` defaults, for a local checkout, to `<branch>-<short-commit>` —
with a `-dirty` suffix **and** a `YYYYMMDDHHMMSS` timestamp when **tracked**
files are modified (as `git describe --dirty` reports — untracked files alone
don't count), so successive dirty builds at the same commit get distinct tags
(and Kubernetes actually rolls out the new image instead of reusing a cached
tag); the branch is omitted on a detached HEAD. For a git URL it is the
`#ref` (or `default` when the URL has no `#ref`), and `local` for a non-git
directory. With `--base-image-ref`, the default output tag is the base ref's
own tag rather than `<base-name>-<base-tag>`.

### Examples

```sh
# No args: lnd from upstream master + lndinit from this worktree (local image):
contrib/build-dev-image

# lnd from a local checkout:
contrib/build-dev-image ../lnd

# lnd from an unreleased branch, pushed to a dev registry:
contrib/build-dev-image --push --name my.registry/lndinit --tag dev \
    https://github.com/lightningnetwork/lnd.git#my-feature-branch

# Build a specific lnd PR (by number, or paste the PR URL):
contrib/build-dev-image --pr 9500
contrib/build-dev-image https://github.com/lightningnetwork/lnd/pull/9500

# litd base from a local checkout — a directory named exactly lightning-terminal
# auto-selects litd (any other name needs --litd):
contrib/build-dev-image ../lightning-terminal

# Skip the base build; layer this worktree's lndinit onto a published base:
contrib/build-dev-image --base-image-ref lightninglabs/lnd:v0.21.0-beta

# Push an already-built image without rebuilding:
contrib/build-dev-image --push-only --name my.registry/lndinit --tag dev
```

### Options

| Option | Description |
|--------|-------------|
| `--litd` | Build on `litd` (lightning-terminal) instead of `lnd`: selects the litd default context and `--pr` repo, and names the base image `litd-dev`. A base context (directory or repo) named **exactly** `lightning-terminal` auto-selects litd (e.g. `../lightning-terminal`); any other name — even a `litd` checkout — needs `--litd`. |
| `--base-name NAME` | Name (not including a tag) for the locally-built base image. Default: `lnd-dev` (`litd-dev` with `--litd`). |
| `--base-tag TAG` | Tag for the locally-built base image. Default: derived from the base context (see above). |
| `--base-image-ref REF` | Skip building the base and layer `lndinit` onto this already-built or published base (`name:tag`, split on the last colon so a registry port is preserved; published `lnd` tags are listed on [Docker Hub](https://hub.docker.com/r/lightninglabs/lnd/tags)). Mutually exclusive with `<base-context>`. |
| `--pr N` | Build the default repo's GitHub PR *N* (lnd, or lightning-terminal with `--litd`), tagged `pr-N`. The PR head commit is resolved via `git ls-remote` (errors clearly if #*N* is an issue or doesn't exist). Shorthand for passing a PR URL as `<base-context>`. Mutually exclusive with `<base-context>`. |
| `--lndinit-context CTX` | Build `lndinit` from `CTX` instead of this worktree (local path or git URL with optional `#ref`). |
| `--name NAME` | Name (not including a tag) for the output `lndinit` image. Default: `lndinit`. |
| `--tag TAG` | Tag (not including a name) for the output `lndinit` image. Default: `<base-name>-<base-tag>`, or the base ref's tag with `--base-image-ref`. |
| `--push` | `docker push` the final image (you must already be `docker login`'d to the target registry). |
| `--push-only` | Push an already-built image (`<name>:<tag>` from `--name`/`--tag`) **without** rebuilding; requires `--tag`. Mutually exclusive with a base context / `--pr` / `--base-image-ref`. |
| `--platform PLATFORM` | `buildx --platform`. Default: the build host's. A multi-arch (comma-separated) value requires both a prebuilt `--base-image-ref` (the locally-built base must be single-arch to layer onto) and `--push` (a manifest list cannot be loaded into the local docker store). |
| `--dry-run` | Print the `buildx` commands without running them. |
| `-h`, `--help` | Show help. |
