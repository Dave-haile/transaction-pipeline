# crates/ingestion/Dockerfile
#
# Multi-stage build for the ingestion service. Built from the WORKSPACE
# ROOT as the build context (see the build command below) - not from
# crates/ingestion/ - because Cargo needs to see the whole workspace
# manifest (root Cargo.toml/Cargo.lock, plus every member's Cargo.toml)
# to resolve dependencies consistently, even though we're only compiling
# the ingestion binary out of it.

# ARGs declared before the first FROM are available to every FROM in
# this file. Defaults point at the official public images, so a plain
# `docker build` (no extra flags) works on any machine out of the box -
# Windows, a fresh Linux box, GitHub Actions, wherever. Overriding these
# (via docker-compose's build.args, see docker-compose.yml) is only
# needed on networks like this VM's corporate one that block plain HTTP
# apt access.
ARG RUST_BASE_IMAGE=rust:1.97-slim-bookworm
ARG RUNTIME_BASE_IMAGE=debian:bookworm-slim

# ---------- Stage 1: builder ----------
# rust:1.82-slim has the full toolchain (rustc, cargo) on a slimmed-down
# Debian base - much smaller than the default rust:1.82 image, but still
# has apt available for the system libs rdkafka needs below.
FROM ${RUST_BASE_IMAGE} AS builder

# Native build dependencies for rdkafka, which compiles the C library
# librdkafka from source: cmake drives that build, build-essential gives
# us a C/C++ compiler, and the *-dev packages provide the header files
# and link libraries librdkafka builds against.

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    pkg-config \
    libssl-dev \
    zlib1g-dev \
    libzstd-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# --- Dependency-caching layer ---
# Copy ONLY the manifest files first. As long as none of these Cargo.toml
# files change, Docker reuses everything below as a cached layer instead
# of re-running cargo, no matter how much your actual .rs source changes.
COPY Cargo.toml Cargo.lock ./
COPY crates/common/Cargo.toml crates/common/Cargo.toml
COPY crates/ingestion/Cargo.toml crates/ingestion/Cargo.toml
COPY crates/processor/Cargo.toml crates/processor/Cargo.toml

# Cargo also needs *something* at each crate's expected source path to
# successfully parse the workspace - it checks that a bin/lib target
# file exists when reading the manifest, even before compiling anything.
# These are throwaway stub files, immediately discarded once we copy the
# real source in below; they exist purely so this caching layer's `cargo
# build` command has valid targets to prefetch dependencies for.
RUN mkdir -p crates/common/src crates/ingestion/src crates/processor/src && \
    echo "fn main() {}" > crates/ingestion/src/main.rs && \
    echo "fn main() {}" > crates/processor/src/main.rs && \
    echo "// stub" > crates/common/src/lib.rs

# Builds and caches every dependency ingestion needs, using the stub
# source above. This is the slow step (~compiling rdkafka and friends)
# that we want Docker to cache and skip on future rebuilds.
RUN cargo build --release -p ingestion

# --- Real source ---
# NOW copy the actual code. This layer invalidates on every source
# change (as it should), but the dependency layer above stays cached,
# so this rebuild only recompiles YOUR crates, not the whole dependency
# tree.
COPY crates/common crates/common
COPY crates/ingestion crates/ingestion

# Touch main.rs so Cargo definitely sees it as newer than the stub build
# artifacts and actually recompiles it - belt-and-suspenders against any
# filesystem timestamp edge cases from the COPY above.
RUN touch crates/ingestion/src/main.rs crates/common/src/lib.rs && \
    cargo build --release -p ingestion

# ---------- Stage 2: runtime ----------
# debian:bookworm-slim instead of rust:1.82-slim: no compiler, no cargo,
# no source code - just enough OS to run the compiled binary. This is
# the image that actually ships; the builder stage above is discarded
# entirely once the final image is built.
FROM ${RUNTIME_BASE_IMAGE}

# Runtime (non-dev) versions of the libraries rdkafka linked against at
# compile time. ca-certificates is needed for any outbound TLS
# connections (not currently used here, but cheap to include and saves
# a confusing failure later if you add one).
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    zlib1g \
    libzstd1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy ONLY the compiled binary from the builder stage - none of the
# source code, Cargo caches, or build toolchain end up in this final
# image. This is the entire point of a multi-stage build.
COPY --from=builder /app/target/release/ingestion /app/ingestion

# Documents which port this container listens on. Doesn't actually
# publish the port (that's done in docker-compose.yml) - this is
# metadata for anyone reading the Dockerfile, and some tooling uses it.
EXPOSE 3001

ENV RUST_LOG=info

CMD ["/app/ingestion"]