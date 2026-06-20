FROM python:3.11.9-alpine AS compile-image

WORKDIR /app

# Build deps for pip packages that need compilation
RUN apk add --no-cache --virtual .build-deps gcc musl-dev

# Install python deps
COPY requirements.txt /app/
RUN pip install --no-cache-dir \
    --trusted-host pypi.org \
    --trusted-host files.pythonhosted.org \
    --trusted-host pypi.python.org \
    -r requirements.txt

# Install rclone (runtime binary)
RUN apk add --no-cache rclone


FROM python:3.11.9-alpine AS runtime-image

WORKDIR /app

# Copy installed deps from build stage
COPY --from=compile-image /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages

# Copy rclone to the path expected by the app
COPY --from=compile-image /usr/bin/rclone /app/rclone/rclone

# Copy app source code
COPY . /app

# Pre-generate filter cache
RUN python gen_filter_cache.py

CMD ["python", "media_downloader.py"]
