# ── builder: compile the release binary ───────────────────────────────────
FROM rust:1-bookworm AS builder

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

# ── runtime: minimal image with just the binary ──────────────────────────
FROM debian:bookworm-slim AS runtime

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/tmd /usr/local/bin/tmd

# downloads/, sessions/, config.yaml, and data.yaml live under /app.
VOLUME ["/app"]

CMD ["tmd"]
