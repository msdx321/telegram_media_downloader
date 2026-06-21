# ── builder: compile a static release binary (musl) ───────────────────────
FROM rust:1-alpine AS builder

# build-base provides gcc + musl-dev + make in case any dependency's build
# script compiles C code.
RUN apk add --no-cache build-base

WORKDIR /app

# Cache dependencies: build a throwaway library crate first so the expensive
# dep tree (grammers, tokio, axum, ...) is reused unless Cargo.toml/Cargo.lock
# change. Using a library (not a bin) means there is no stale `tmd` binary
# fingerprint to confuse cargo when the real source is overlaid next.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && echo '' > src/lib.rs \
    && cargo build --release --locked \
    && rm -rf src

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
