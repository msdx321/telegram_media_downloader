FROM python:3.11.9-alpine AS compile-image

WORKDIR /app

# Build deps for pip packages that need compilation
RUN apk add --no-cache --virtual .build-deps gcc musl-dev

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install python deps from pyproject.toml (skip building the app itself — source not yet copied)
COPY pyproject.toml ./
RUN uv sync --no-dev --no-install-project --frozen 2>/dev/null || \
    uv sync --no-dev --no-install-project

# Install rclone (runtime binary)
RUN apk add --no-cache rclone


FROM python:3.11.9-alpine AS runtime-image

WORKDIR /app

# Copy virtual env from build stage
COPY --from=compile-image /app/.venv /app/.venv

# Copy rclone to the path expected by the app
COPY --from=compile-image /usr/bin/rclone /app/rclone/rclone

# Copy app source code
COPY . /app

# Pre-generate filter cache
RUN /app/.venv/bin/python gen_filter_cache.py

CMD ["/app/.venv/bin/python", "media_downloader.py"]
