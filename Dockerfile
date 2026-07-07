# ── chef: shared Rust build environment and dependency planner ────────────
FROM rust:1-alpine AS chef

# build-base provides gcc + musl-dev + make in case any dependency's build
# script compiles C code.
RUN apk add --no-cache build-base
RUN cargo install cargo-chef --locked

WORKDIR /app

# ── planner: compute dependency recipe for Docker layer caching ────────────
FROM chef AS planner

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY static ./static
RUN cargo chef prepare --recipe-path recipe.json

# ── builder: compile a static release binary (musl) ───────────────────────
FROM chef AS builder

COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --locked --recipe-path recipe.json

COPY src ./src
COPY static ./static
RUN cargo build --release --locked

# ── runtime: minimal Alpine image with just the binary ────────────────────
FROM alpine:latest AS runtime

RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=builder /app/target/release/tmd /usr/local/bin/tmd

# downloads/, sessions/, config.yaml, and data.yaml live under /app.
VOLUME ["/app"]

CMD ["tmd"]
