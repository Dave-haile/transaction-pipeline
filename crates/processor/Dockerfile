# crates/processor/Dockerfile
# Same structure as crates/ingestion/Dockerfile - see that file's
# comments for the full reasoning behind each step. Only the crate
# name changes throughout.

# ---------- Stage 1: builder ----------
FROM local/rust-1.82-slim-https:latest AS builder

RUN apt-get update && apt-get install -y \
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

# ---------- Stage 2: runtime ----------
FROM local/debian-bookworm-slim-https:latest

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    zlib1g \
    libzstd1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/processor /app/processor

EXPOSE 9091

ENV RUST_LOG=info

CMD ["/app/processor"]