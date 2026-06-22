use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use grammers_client::media::Media;
use grammers_client::Client;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::{debug, info, warn};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{mpsc, Mutex};

use crate::app::{flood_wait_secs, sleep_cancellable, wait_paused, Shutdown};
use crate::config::Config;
use crate::format::{format_byte, truncate_filename, validate_title};
use crate::webui::WebState;

const MAX_FILE_ID_CACHE: usize = 4096;
const RETRY_LIMIT: u32 = 3;
const RETRY_DELAY_SECS: u64 = 5;
const DOWNLOAD_CHUNK_SIZE: u64 = 512 * 1024;
const CHUNK_RETRY_LIMIT: u32 = 3;
const PROGRESS_FLUSH_BYTES: u64 = 16 * 1024 * 1024;
const PROGRESS_REPORT_INTERVAL: Duration = Duration::from_millis(2000);

static DOWNLOAD_PROGRESS_STYLE: LazyLock<ProgressStyle> = LazyLock::new(|| {
    ProgressStyle::with_template(
        "{msg:>8} {wide_bar:.cyan/blue} {bytes:>8}/{total_bytes:8} {bytes_per_sec:>10} {eta}",
    )
    .unwrap_or_else(|_| ProgressStyle::default_bar())
    .progress_chars("##-")
});

#[cfg(target_os = "linux")]
unsafe extern "C" {
    fn posix_fallocate(fd: i32, offset: i64, len: i64) -> i32;
}

pub(crate) fn media_type_value(msg: &grammers_client::message::Message) -> String {
    match msg.media() {
        Some(Media::Photo(_)) => "photo".into(),
        Some(Media::Document(_)) => "document".into(),
        _ => String::new(),
    }
}

pub(crate) fn file_extension_value(msg: &grammers_client::message::Message) -> String {
    match msg.media() {
        Some(Media::Photo(_)) => "jpg".into(),
        Some(Media::Document(doc)) => mime_to_ext(doc.mime_type().unwrap_or("")).into(),
        _ => String::new(),
    }
}

pub(crate) fn media_file_name_value(msg: &grammers_client::message::Message) -> String {
    match msg.media() {
        Some(Media::Document(doc)) => doc.name().map(str::to_string).unwrap_or_default(),
        _ => String::new(),
    }
}

pub(crate) fn media_file_size_value(msg: &grammers_client::message::Message) -> i64 {
    match msg.media() {
        Some(Media::Photo(photo)) => photo.size().unwrap_or(0) as i64,
        Some(Media::Document(doc)) => doc.size().unwrap_or(0) as i64,
        _ => 0,
    }
}

pub(crate) fn media_duration_value(msg: &grammers_client::message::Message) -> i64 {
    match msg.media() {
        Some(Media::Document(doc)) => doc.duration().unwrap_or(0.0) as i64,
        _ => 0,
    }
}

pub(crate) fn media_resolution_value(msg: &grammers_client::message::Message) -> (i64, i64) {
    match msg.media() {
        Some(Media::Document(doc)) => doc
            .resolution()
            .map(|(width, height)| (width as i64, height as i64))
            .unwrap_or((0, 0)),
        _ => (0, 0),
    }
}

fn mime_to_ext(mime: &str) -> &str {
    match mime {
        "image/jpeg" => "jpg",
        "image/png" => "png",
        "image/gif" => "gif",
        "image/webp" => "webp",
        "audio/mpeg" => "mp3",
        "audio/ogg" => "ogg",
        "audio/mp4" => "m4a",
        "audio/x-wav" | "audio/wav" => "wav",
        "video/mp4" => "mp4",
        "video/webm" => "webm",
        "video/x-matroska" => "mkv",
        "application/pdf" => "pdf",
        "application/zip" => "zip",
        "application/x-rar-compressed" => "rar",
        "application/epub+zip" => "epub",
        _ => "unknown",
    }
}

fn document_kind(mime: &str) -> &str {
    if mime == "image/gif" {
        "animation"
    } else if mime.starts_with("audio/") {
        "audio"
    } else if mime.starts_with("video/") {
        "video"
    } else {
        "document"
    }
}

fn media_kind_and_ext(media: &Media) -> Option<(&str, String)> {
    match media {
        Media::Photo(_) => Some(("photo", "jpg".to_string())),
        Media::Document(doc) => {
            let mime = doc.mime_type().unwrap_or("");
            let ext = doc
                .name()
                .and_then(|name| name.rsplit_once('.').map(|(_, ext)| ext.to_string()))
                .unwrap_or_else(|| mime_to_ext(mime).to_string());
            Some((document_kind(mime), ext))
        }
        _ => None,
    }
}

pub(crate) fn media_matches_config(media: &Media, cfg: &Config) -> bool {
    let Some((kind, ext)) = media_kind_and_ext(media) else {
        return false;
    };
    if !cfg.media_types.iter().any(|item| item == kind) {
        return false;
    }

    let allowed = match kind {
        "audio" => &cfg.file_formats.audio,
        "document" => &cfg.file_formats.document,
        "video" => &cfg.file_formats.video,
        _ => return true,
    };
    allowed.iter().any(|item| item == "all" || item == &ext)
}

pub(crate) async fn download_media_inner(
    client: &Client,
    msg: &grammers_client::message::Message,
    cfg: &Config,
    file_ids: &Arc<Mutex<HashSet<String>>>,
    mp: &MultiProgress,
    web_state: &Arc<WebState>,
    shutdown: &Shutdown,
) -> anyhow::Result<bool> {
    let media = match msg.media() {
        Some(m) => m,
        None => return Ok(false),
    };
    let msg_id = msg.id();

    // Check file_unique_id cache
    let fid = match &media {
        Media::Photo(p) => p.id().to_string(),
        Media::Document(d) => d.id().to_string(),
        _ => String::new(),
    };
    if !fid.is_empty() {
        let cache = file_ids.lock().await;
        if cache.contains(&fid) {
            debug!("msg={msg_id}: already downloaded (file_unique_id), skipped");
            return Ok(false);
        }
        drop(cache);
    }

    let (temp_path, final_path) = build_media_paths(msg, &media, cfg)?;

    // Already fully downloaded?
    if tokio::fs::try_exists(&final_path).await? {
        debug!("msg={msg_id}: file already exists, skipped");
        let mut cache = file_ids.lock().await;
        if !fid.is_empty() {
            cache.insert(fid);
        }
        return Ok(false);
    }

    tokio::fs::create_dir_all(temp_path.parent().unwrap_or(Path::new("."))).await?;

    let total = match &media {
        Media::Photo(p) => p.size().unwrap_or(0) as u64,
        Media::Document(d) => d.size().unwrap_or(0) as u64,
        _ => 0,
    };
    let existing = resume_offset(&temp_path, total).await?;
    web_state
        .download_started(msg_id, &final_path, existing, total)
        .await;

    // The sidecar marks a prior run as fully fetched but not yet renamed into
    // place. Promote it directly instead of re-fetching the whole file.
    if total > 0 && existing == total {
        info!(
            "msg={msg_id}: already complete, finalizing {}",
            format_byte(total as f64)
        );
        finalize_download(
            msg_id,
            &fid,
            file_ids,
            &temp_path,
            &final_path,
            total,
            web_state,
        )
        .await?;
        return Ok(true);
    }

    if existing > 0 {
        info!(
            "msg={msg_id}: resuming from {} of {}",
            format_byte(existing as f64),
            format_byte(total as f64)
        );
    } else {
        info!(
            "msg={msg_id}: downloading {} -> {}",
            format_byte(total as f64),
            final_path.display()
        );
    }

    let mut last_err = None;
    for attempt in 0..RETRY_LIMIT {
        if shutdown.is_cancelled() {
            break;
        }
        if attempt > 0 {
            info!("msg={msg_id}: retry {}/{}", attempt + 1, RETRY_LIMIT);
            if sleep_cancellable(shutdown, Duration::from_secs(RETRY_DELAY_SECS)).await {
                break;
            }
        }

        // Re-evaluate the resume offset each attempt: a failed attempt leaves
        // the valid prefix on disk, so we only re-fetch what is missing.
        let downloaded = if attempt == 0 {
            existing
        } else {
            resume_offset(&temp_path, total).await?
        };

        let pb = mp.add(ProgressBar::new(total));
        pb.set_style(DOWNLOAD_PROGRESS_STYLE.clone());
        pb.set_message(format!("{msg_id}"));
        if downloaded > 0 {
            pb.set_position(downloaded);
        }

        if total == 0 {
            if !wait_paused(web_state, shutdown).await {
                pb.finish_and_clear();
                break;
            }
            let outcome = tokio::select! {
                r = client.download_media(&media, &temp_path) => r.map_err(|e| e.into()),
                _ = shutdown.cancelled() => Err(anyhow::anyhow!("interrupted")),
            };
            if let Err(e) = outcome {
                last_err = Some(e);
                pb.finish_and_clear();
                drop(pb);
                if shutdown.is_cancelled() {
                    break;
                }
                discard_partial(&temp_path).await;
                continue;
            }
            pb.set_position(total);
        } else if total > downloaded {
            let progress = DownloadProgress {
                pb: &pb,
                web_state,
                msg_id,
            };
            if let Err(e) = download_concurrent(
                client,
                &media,
                &temp_path,
                downloaded..total,
                cfg.download_connections.max(1) as u64,
                &progress,
                shutdown,
            )
            .await
            {
                // Keep the partial file so the next attempt resumes; only the
                // unfetched chunks need re-downloading.
                if !shutdown.is_cancelled() {
                    warn!("msg={msg_id}: {e:#}");
                }
                last_err = Some(e);
                pb.finish_and_clear();
                drop(pb);
                continue;
            }
        } else {
            pb.set_position(downloaded);
        }

        pb.finish_and_clear();
        drop(pb);

        let actual = tokio::fs::metadata(&temp_path)
            .await
            .map(|m| m.len())
            .unwrap_or(0);

        if total > 0 && actual != total {
            warn!("msg={msg_id}: size mismatch ({actual} vs {total}) - retrying");
            discard_partial(&temp_path).await;
            last_err = Some(anyhow::anyhow!("size mismatch"));
            continue;
        }

        finalize_download(
            msg_id,
            &fid,
            file_ids,
            &temp_path,
            &final_path,
            actual,
            web_state,
        )
        .await?;
        return Ok(true);
    }

    // On shutdown, keep the `.part` file so the download resumes next run. On a
    // real failure (retries exhausted), delete it so we start fresh.
    if !shutdown.is_cancelled() {
        discard_partial(&temp_path).await;
    }
    web_state.download_finished(msg_id, 0, false).await;
    if shutdown.is_cancelled() {
        return Err(anyhow::anyhow!("download interrupted"));
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("download failed")))
}

/// Promote a completed `.part` file to its final name, record its file id in
/// the dedup cache, and notify the web UI. The caller has already validated
/// `actual` against the expected size.
async fn finalize_download(
    msg_id: i32,
    fid: &str,
    file_ids: &Arc<Mutex<HashSet<String>>>,
    temp_path: &Path,
    final_path: &Path,
    actual: u64,
    web_state: &Arc<WebState>,
) -> anyhow::Result<()> {
    tokio::fs::create_dir_all(final_path.parent().unwrap_or(Path::new("."))).await?;
    tokio::fs::rename(temp_path, final_path).await?;
    let _ = tokio::fs::remove_file(progress_path(temp_path)).await;

    if !fid.is_empty() {
        let mut cache = file_ids.lock().await;
        if cache.len() >= MAX_FILE_ID_CACHE {
            // HashSet order is arbitrary, so this evicts a random entry
            // (not truly the oldest). Fine for a dedup cache: the worst
            // case is a one-off re-download of the evicted file.
            if let Some(evicted) = cache.iter().next().cloned() {
                cache.remove(&evicted);
            }
        }
        cache.insert(fid.to_string());
    }

    info!(
        "msg={msg_id}: saved {} -> {}",
        format_byte(actual as f64),
        final_path.display()
    );
    web_state.download_finished(msg_id, actual, true).await;
    Ok(())
}

struct DownloadProgress<'a> {
    pb: &'a ProgressBar,
    web_state: &'a Arc<WebState>,
    msg_id: i32,
}

async fn send_chunk(
    tx: &mpsc::Sender<(u64, Vec<u8>)>,
    chunk: (u64, Vec<u8>),
    shutdown: &Shutdown,
) -> Result<(), mpsc::error::SendError<(u64, Vec<u8>)>> {
    tokio::select! {
        permit = tx.reserve() => match permit {
            Ok(permit) => {
                permit.send(chunk);
                Ok(())
            }
            Err(_) => Err(mpsc::error::SendError(chunk)),
        },
        _ = shutdown.cancelled() => Err(mpsc::error::SendError(chunk)),
    }
}

async fn report_download_progress(
    progress: &DownloadProgress<'_>,
    start: u64,
    downloaded: u64,
    started: Instant,
) {
    progress.pb.set_position(downloaded);
    let elapsed = started.elapsed().as_secs_f64().max(0.001);
    progress
        .web_state
        .download_progress(
            progress.msg_id,
            downloaded,
            ((downloaded - start) as f64 / elapsed) as u64,
        )
        .await;
}

async fn download_concurrent(
    client: &Client,
    media: &Media,
    path: &Path,
    range: std::ops::Range<u64>,
    connections: u64,
    progress: &DownloadProgress<'_>,
    shutdown: &Shutdown,
) -> anyhow::Result<()> {
    let start = range.start;
    let total = range.end;
    let chunk_size = DOWNLOAD_CHUNK_SIZE;
    let start_chunk = start / chunk_size;
    let total_chunks = total.div_ceil(chunk_size);
    let workers = connections.min(total_chunks - start_chunk).max(1);

    let (tx, mut rx) = mpsc::channel::<(u64, Vec<u8>)>((workers as usize).max(1));
    // Set by the first worker to fail so its peers stop fetching after their
    // current chunk instead of downloading data that will be discarded.
    let abort = Arc::new(AtomicBool::new(false));
    let mut tasks = Vec::with_capacity(workers as usize);

    for worker in 0..workers {
        let client = client.clone();
        let media = media.clone();
        let tx = tx.clone();
        let web_state = progress.web_state.clone();
        let abort = abort.clone();
        let shutdown = shutdown.clone();

        tasks.push(tokio::spawn(async move {
            // Striped assignment: this worker owns chunks worker, worker+n, ...
            // Each chunk is fetched with its own stream so a short read (which
            // ends grammers' stream early) only affects that one piece.
            let mut idx = start_chunk + worker;
            while idx < total_chunks {
                if abort.load(Ordering::Relaxed) || shutdown.is_cancelled() {
                    break;
                }
                if !wait_paused(&web_state, &shutdown).await {
                    break;
                }
                let offset = idx * chunk_size;
                let expected = (total - offset).min(chunk_size);
                match fetch_chunk(&client, &media, idx, expected, &shutdown).await {
                    Ok(chunk) => {
                        if send_chunk(&tx, (offset, chunk), &shutdown).await.is_err() {
                            break; // receiver gone or shutdown requested
                        }
                    }
                    Err(e) => {
                        abort.store(true, Ordering::Relaxed);
                        return Err(e);
                    }
                }
                idx += workers;
            }
            Ok::<(), anyhow::Error>(())
        }));
    }
    drop(tx);

    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(start == 0)
        .open(path)
        .await?;
    // Preallocate the full file up front. On a NAS this avoids growing the
    // file extent-by-extent as chunks land, which fragments the layout and
    // can stall writes. Falls back to a sparse truncate if unsupported.
    preallocate(&file, total).await?;
    file.seek(std::io::SeekFrom::Start(start)).await?;

    let mut next = start;
    let mut fetched = start;
    let mut last_flushed = start;
    let started = Instant::now();
    let mut pending: HashMap<u64, Vec<u8>> = HashMap::default();
    let mut last_reported = start;
    let mut last_reported_at = Instant::now();
    loop {
        // Receiving the next chunk races against shutdown so a Ctrl+C doesn't
        // block on a stalled worker; anything already in the pipe is still
        // flushed below so the `.part` prefix stays contiguous.
        let (offset, chunk) = tokio::select! {
            m = rx.recv() => match m {
                Some(item) => item,
                None => break,
            },
            _ = shutdown.cancelled() => break,
        };
        fetched = (fetched + chunk.len() as u64).min(total);
        if last_reported_at.elapsed() >= PROGRESS_REPORT_INTERVAL {
            report_download_progress(progress, start, fetched, started).await;
            last_reported = fetched;
            last_reported_at = Instant::now();
        }

        pending.insert(offset, chunk);
        while let Some(chunk) = pending.remove(&next) {
            file.write_all(&chunk).await?;
            next += chunk.len() as u64;
            // Periodically record the contiguous write position so an
            // interruption can resume here. The `.part` file is preallocated to
            // `total`, so its size can't recover this point.
            if next - last_flushed >= PROGRESS_FLUSH_BYTES {
                write_progress(path, next).await;
                last_flushed = next;
            }
        }
    }
    if last_reported != fetched {
        report_download_progress(progress, start, fetched, started).await;
    }
    // Final flush: persist the full contiguous position (== `total` on success)
    // so a later run can finalize without re-fetching.
    write_progress(path, next).await;

    // Surface the first worker error (a chunk that exhausted its retries).
    let mut worker_err: Option<anyhow::Error> = None;
    let mut cancelled = shutdown.is_cancelled();
    for task in tasks {
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) if worker_err.is_none() => worker_err = Some(e),
            Ok(Err(_)) => {}
            Err(e) if e.is_cancelled() => cancelled = true,
            Err(e) if worker_err.is_none() => {
                worker_err = Some(anyhow::anyhow!("download worker join failed: {e}"))
            }
            Err(_) => {}
        }
    }
    if cancelled {
        // Leave the contiguous prefix on disk for resume; report interruption
        // so the caller keeps the `.part` file rather than deleting it.
        return Err(anyhow::anyhow!("download interrupted"));
    }
    if let Some(e) = worker_err {
        return Err(e);
    }
    // No worker errored, so every chunk must have been written contiguously.
    // A gap here would indicate a logic bug rather than a network failure.
    if next != total {
        return Err(anyhow::anyhow!(
            "incomplete download: wrote {next} of {total} bytes"
        ));
    }

    Ok(())
}

/// Fetch a single chunk at index `idx`, retrying transient failures.
///
/// A fresh `iter_download` stream is used per attempt. This matters because
/// grammers marks its stream done as soon as a read returns fewer bytes than
/// requested (see grammers `files.rs`); a transient short read would otherwise
/// end the stream and starve the rest of the fetch. Each chunk is also
/// validated against its expected size so a short piece is never written.
///
/// The fetch and its retry backoff are both raced against `shutdown` so a
/// Ctrl+C cancels the in-flight network read at once instead of waiting for
/// it (or its backoff) to finish.
async fn fetch_chunk(
    client: &Client,
    media: &Media,
    idx: u64,
    expected: u64,
    shutdown: &Shutdown,
) -> anyhow::Result<Vec<u8>> {
    let mut backoff = 0u64;
    let mut last_err: Option<anyhow::Error> = None;
    for attempt in 0..CHUNK_RETRY_LIMIT {
        if shutdown.is_cancelled() {
            break;
        }
        let delay = std::mem::take(&mut backoff);
        if delay > 0 && sleep_cancellable(shutdown, Duration::from_secs(delay)).await {
            break;
        }
        let mut stream = client
            .iter_download(media)
            .chunk_size(DOWNLOAD_CHUNK_SIZE as i32)
            .skip_chunks(i32::try_from(idx)?);
        let result = tokio::select! {
            r = stream.next() => r,
            _ = shutdown.cancelled() => break,
        };
        match result {
            Ok(Some(chunk)) if chunk.len() as u64 == expected => return Ok(chunk),
            Ok(Some(chunk)) => {
                warn!(
                    "chunk {idx}: short read {} B (expected {expected}), retry {}/{}",
                    chunk.len(),
                    attempt + 1,
                    CHUNK_RETRY_LIMIT
                );
                last_err = Some(anyhow::anyhow!(
                    "chunk {idx} short: {} vs {expected} bytes",
                    chunk.len()
                ));
                backoff = RETRY_DELAY_SECS;
            }
            Ok(None) => {
                warn!(
                    "chunk {idx}: stream ended early, retry {}/{}",
                    attempt + 1,
                    CHUNK_RETRY_LIMIT
                );
                last_err = Some(anyhow::anyhow!("chunk {idx} stream ended early"));
                backoff = RETRY_DELAY_SECS;
            }
            Err(e) => {
                backoff = flood_wait_secs(&e.to_string()).unwrap_or(RETRY_DELAY_SECS);
                warn!(
                    "chunk {idx}: fetch error: {e}; retry {}/{} in {backoff}s",
                    attempt + 1,
                    CHUNK_RETRY_LIMIT
                );
                last_err = Some(anyhow::anyhow!("chunk {idx}: {e}"));
            }
        }
    }
    if shutdown.is_cancelled() {
        return Err(anyhow::anyhow!("download interrupted"));
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("chunk {idx} failed after retries")))
}

/// Resume offset for `temp_path`: the byte position to continue downloading
/// from. The `.part` file is preallocated to `total` up front, so its size does
/// not reflect how much was actually fetched; the `.progress` sidecar (the
/// contiguous byte count the writer flushed) is the source of truth. Returns
/// `total` when the sidecar marks the file complete, the last flushed chunk
/// boundary when partial, or 0 (clearing any stale sidecar) when the temp is
/// missing.
async fn resume_offset(temp_path: &Path, total: u64) -> anyhow::Result<u64> {
    let len = match tokio::fs::metadata(temp_path).await {
        Ok(m) => m.len(),
        Err(_) => {
            let _ = tokio::fs::remove_file(progress_path(temp_path)).await;
            return Ok(0);
        }
    };
    let valid = read_progress(temp_path).await;
    if total > 0 && valid >= total {
        return Ok(total);
    }
    // Partial: resume from the last flushed chunk boundary, clamped to the real
    // file size so a stale sidecar can never make us skip data.
    let aligned = valid - (valid % DOWNLOAD_CHUNK_SIZE);
    Ok(aligned.min(len))
}

/// Path of the `.progress` sidecar recording how many contiguous bytes of
/// `temp_path` have been fetched.
fn progress_path(temp_path: &Path) -> PathBuf {
    let mut p = temp_path.as_os_str().to_owned();
    p.push(".progress");
    PathBuf::from(p)
}

/// Read the recorded contiguous byte count, or 0 when the sidecar is missing or
/// unreadable (treated as a fresh download).
async fn read_progress(temp_path: &Path) -> u64 {
    match tokio::fs::read(progress_path(temp_path)).await {
        Ok(bytes) => String::from_utf8_lossy(&bytes)
            .trim()
            .parse::<u64>()
            .unwrap_or(0),
        Err(_) => 0,
    }
}

/// Best-effort persist of the contiguous byte count. A failed write only means
/// the next resume re-fetches a little more.
async fn write_progress(temp_path: &Path, n: u64) {
    let _ = tokio::fs::write(progress_path(temp_path), n.to_string().into_bytes()).await;
}

/// Drop a partial `.part` download and its sidecar so the next attempt starts
/// fresh.
async fn discard_partial(temp_path: &Path) {
    let _ = tokio::fs::remove_file(temp_path).await;
    let _ = tokio::fs::remove_file(progress_path(temp_path)).await;
}

/// Preallocate `size` bytes for `file`, preferring real block allocation
/// (`posix_fallocate`) so network-attached storage doesn't grow the file
/// incrementally during chunked writes. Falls back to a sparse `set_len` when
/// preallocation is unavailable (or on non-Linux hosts). No-op for `size == 0`.
async fn preallocate(file: &tokio::fs::File, size: u64) -> anyhow::Result<()> {
    if size == 0 {
        return Ok(());
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        let fd = file.as_raw_fd();
        let len = i64::try_from(size)?;
        // posix_fallocate is a blocking syscall; run it off the async thread.
        let rc =
            tokio::task::spawn_blocking(move || unsafe { posix_fallocate(fd, 0, len) }).await?;
        if rc != 0 {
            // ENOTSUP (e.g. over NFS) / EDQUOT / etc.: fall back to a sparse truncate.
            file.set_len(size).await?;
        }
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        // No portable fallocate here; reserve the size sparsely instead.
        file.set_len(size).await?;
        Ok(())
    }
}

/// Returns (temp_path, final_path) for a media download.
fn build_media_paths(
    msg: &grammers_client::message::Message,
    media: &Media,
    cfg: &Config,
) -> anyhow::Result<(PathBuf, PathBuf)> {
    let chat_title = msg
        .peer()
        .and_then(|p| p.name())
        .map(validate_title)
        .unwrap_or_else(|| format!("{:?}", msg.peer_id()));

    let date = msg.date().naive_utc();
    let datetime_str = date.format(&cfg.date_format).to_string();

    let Some((media_type_str, _)) = media_kind_and_ext(media) else {
        return Err(anyhow::anyhow!("unsupported media"));
    };

    let mut dir: PathBuf = cfg.save_path.clone();
    for seg in &cfg.file_path_prefix {
        match seg.as_str() {
            "chat_title" => dir.push(&chat_title),
            "media_datetime" => dir.push(&datetime_str),
            "media_type" => dir.push(media_type_str),
            _ => {}
        }
    }

    let (stem, ext) = match media {
        Media::Photo(_) => (msg.id().to_string(), "jpg".to_string()),
        Media::Document(doc) => {
            let mut stem = String::new();
            let mut ext = doc
                .mime_type()
                .map(|m| mime_to_ext(m).to_string())
                .unwrap_or_else(|| "unknown".to_string());
            if let Some(name) = doc.name() {
                if let Some(dot) = name.rfind('.') {
                    stem = name[..dot].to_string();
                    ext = name[dot + 1..].to_string();
                } else {
                    stem = name.to_string();
                }
            }
            if stem.is_empty() {
                stem = format!("file_{}", doc.id());
            }
            let mut parts: Vec<String> = Vec::new();
            for seg in &cfg.file_name_prefix {
                match seg.as_str() {
                    "message_id" => parts.push(msg.id().to_string()),
                    "file_name" => parts.push(stem.clone()),
                    "caption" => {
                        let txt = msg.text();
                        if !txt.is_empty() {
                            parts.push(validate_title(txt));
                        }
                    }
                    _ => {}
                }
            }
            let sep = &cfg.file_name_prefix_split;
            (
                if parts.is_empty() {
                    msg.id().to_string()
                } else {
                    parts.join(sep)
                },
                ext,
            )
        }
        _ => return Err(anyhow::anyhow!("unsupported media")),
    };

    let fname = format!("{stem}.{ext}");
    let final_path = truncate_filename(&dir.join(&fname), 230);

    let temp_path = final_path.with_extension(format!("{ext}.part"));

    Ok((temp_path, final_path))
}
