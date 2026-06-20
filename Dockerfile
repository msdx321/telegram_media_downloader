FROM python:3.11.9-alpine AS compile-image

WORKDIR /app

# Build deps for pip packages that need compilation
RUN apk add --no-cache --virtual .build-deps gcc musl-dev

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install python deps (requirements.txt generated via: uv export --no-dev --no-hashes)
COPY pyproject.toml requirements.txt ./
RUN grep -v '^-e \.' requirements.txt | uv pip install --system --no-cache -r /dev/stdin


FROM python:3.11.9-alpine AS runtime-image

WORKDIR /app

# Copy installed deps from build stage
COPY --from=compile-image /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages

# Copy app source code
COPY . /app

CMD ["python", "media_downloader.py"]
