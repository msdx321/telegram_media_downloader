use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use indicatif::{ProgressBar, ProgressStyle};

use crate::webui::WebState;

pub(crate) const DOWNLOAD_CHUNK_SIZE: u64 = 512 * 1024;
pub(crate) const PROGRESS_REPORT_INTERVAL: Duration = Duration::from_millis(2000);

static DOWNLOAD_PROGRESS_STYLE: LazyLock<ProgressStyle> = LazyLock::new(|| {
    ProgressStyle::with_template(
        "{msg:>8} {wide_bar:.cyan/blue} {bytes:>8}/{total_bytes:8} {bytes_per_sec:>10} {eta}",
    )
    .unwrap_or_else(|_| ProgressStyle::default_bar())
    .progress_chars("##-")
});

pub(crate) struct DownloadProgress<'a> {
    pub(crate) pb: &'a ProgressBar,
    pub(crate) web_state: &'a Arc<WebState>,
    pub(crate) msg_id: i32,
}

pub(crate) fn progress_style() -> ProgressStyle {
    DOWNLOAD_PROGRESS_STYLE.clone()
}

pub(crate) async fn report_download_progress(
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

/// Resume offset for `temp_path`: the byte position to continue downloading
/// from. The `.part` file is preallocated to `total` up front, so its size does
/// not reflect how much was actually fetched; the `.progress` sidecar (the
/// contiguous byte count the writer flushed) is the source of truth. Returns
/// `total` when the sidecar marks the file complete, the last flushed chunk
/// boundary when partial, or 0 (clearing any stale sidecar) when the temp is
/// missing.
pub(crate) async fn resume_offset(temp_path: &Path, total: u64) -> anyhow::Result<u64> {
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
pub(crate) fn progress_path(temp_path: &Path) -> PathBuf {
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
pub(crate) async fn write_progress(temp_path: &Path, n: u64) {
    let _ = tokio::fs::write(progress_path(temp_path), n.to_string().into_bytes()).await;
}
