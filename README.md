# Telegram Media Downloader

Native Rust Telegram media downloader.

## Build

```sh
cargo build --release
```

## Run

Create `config.yaml`, then run:

```sh
cargo run --release
```

The downloader also starts a no-auth web UI at the configured `web_host` and `web_port` with live stats, per-file progress bars, and pause/resume controls. The page uses a long-lived browser connection for updates, so it does not refresh while downloads run.

The release binary is `target/release/tmd`.

## Docker

```sh
docker build -t tmd-rs .
docker run --rm \
  -v "$PWD/config.yaml:/app/config.yaml" \
  -v "$PWD/downloads:/app/downloads" \
  -v "$PWD/sessions:/app/sessions" \
  -p 5000:5000 \
  tmd-rs
```

Prebuilt multi-arch images are published to Docker Hub as `tmd-rs` on every
push to `master` and on version tags (`v*`).

## Configuration

The app reads `config.yaml` from the working directory.

```yaml
api_hash: your_api_hash
api_id: your_api_id
chat:
  - chat_id: telegram_chat_id
    last_read_message_id: 0
    download_filter: "file_size >= 10MB"
media_types:
  - audio
  - document
  - photo
  - video
  - voice
save_path: ./downloads
file_path_prefix:
  - chat_title
  - media_datetime
file_name_prefix:
  - message_id
  - file_name
file_name_prefix_split: " - "
max_download_task: 5
download_connections: 4
web_host: 0.0.0.0
web_port: 5001
check_interval_secs: 900
date_format: "%Y_%m"
```
