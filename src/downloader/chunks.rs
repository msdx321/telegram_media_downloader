use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use grammers_client::media::Media;
use grammers_client::Client;
use log::warn;
use rustc_hash::FxHashMap as HashMap;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::app::{flood_wait_secs, sleep_cancellable, wait_paused, Shutdown};
use crate::downloader::finalize::preallocate;
use crate::downloader::progress::{
    report_download_progress, write_progress, DownloadProgress, DOWNLOAD_CHUNK_SIZE,
    PROGRESS_REPORT_INTERVAL,
};

const RETRY_DELAY_SECS: u64 = 5;
const CHUNK_RETRY_LIMIT: u32 = 3;
const PROGRESS_FLUSH_BYTES: u64 = 16 * 1024 * 1024;

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

pub(crate) async fn download_concurrent(
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
