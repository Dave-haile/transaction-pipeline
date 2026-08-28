# crates/processor/Dockerfile
# Same structure/reasoning as crates/ingestion/Dockerfile - only the
# crate name differs throughout.

FROM rust:1.97-slim-bookworm AS builder

RUN sed -i 's|http://deb.debian.org|https://deb.debian.org|g' /etc/apt/sources.list.d/*.sources 2>/dev/null || \
    sed -i 's|http://deb.debian.org|https://deb.debian.org|g' /etc/apt/sources.list; \
    apt-get update -o Acquire::https::Verify-Peer=false -o Acquire::https::Verify-Host=false && \
    apt-get install -y --no-install-recommends -o Acquire::https::Verify-Peer=false -o Acquire::https::Verify-Host=false ca-certificates && \
    apt-get update && \
    apt-get install -y \
        build-essential \
        cmake \
        pkg-config \
        libssl-dev \
        zlib1g-dev \
        libzstd-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY crates/common/Cargo.toml crates/common/Cargo.toml
COPY crates/ingestion/Cargo.toml crates/ingestion/Cargo.toml
COPY crates/processor/Cargo.toml crates/processor/Cargo.toml

RUN mkdir -p crates/common/src crates/ingestion/src crates/processor/src && \
    echo "fn main() {}" > crates/ingestion/src/main.rs && \
    echo "fn main() {}" > crates/processor/src/main.rs && \
    echo "// stub" > crates/common/src/lib.rs

RUN cargo build --release -p processor

COPY crates/common crates/common
COPY crates/processor crates/processor

RUN touch crates/processor/src/main.rs crates/common/src/lib.rs && \
    cargo build --release -p processor

FROM debian:bookworm-slim

RUN sed -i 's|http://deb.debian.org|https://deb.debian.org|g' /etc/apt/sources.list.d/*.sources 2>/dev/null || \
    sed -i 's|http://deb.debian.org|https://deb.debian.org|g' /etc/apt/sources.list; \
    apt-get update -o Acquire::https::Verify-Peer=false -o Acquire::https::Verify-Host=false && \
    apt-get install -y --no-install-recommends -o Acquire::https::Verify-Peer=false -o Acquire::https::Verify-Host=false ca-certificates && \
    apt-get update && \
    apt-get install -y \
        libssl3 \
        zlib1g \
        libzstd1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/processor /app/processor

EXPOSE 9091
ENV RUST_LOG=info
CMD ["/app/processor"]
