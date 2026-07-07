use std::path::Path;
use std::sync::Arc;

use log::info;
use std::collections::HashSet;
use tokio::sync::Mutex;

use crate::format::format_byte;
use crate::webui::WebState;

use super::progress::progress_path;

const MAX_FILE_ID_CACHE: usize = 4096;

#[cfg(target_os = "linux")]
unsafe extern "C" {
    fn posix_fallocate(fd: i32, offset: i64, len: i64) -> i32;
}

/// Promote a completed `.part` file to its final name, record its file id in
/// the dedup cache, and notify the web UI. The caller has already validated
/// `actual` against the expected size.
pub(super) async fn finalize_download(
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

/// Drop a partial `.part` download and its sidecar so the next attempt starts
/// fresh.
pub(super) async fn discard_partial(temp_path: &Path) {
    let _ = tokio::fs::remove_file(temp_path).await;
    let _ = tokio::fs::remove_file(progress_path(temp_path)).await;
}

/// Preallocate `size` bytes for `file`, preferring real block allocation
/// (`posix_fallocate`) so network-attached storage doesn't grow the file
/// incrementally during chunked writes. Falls back to a sparse `set_len` when
/// preallocation is unavailable (or on non-Linux hosts). No-op for `size == 0`.
pub(super) async fn preallocate(file: &tokio::fs::File, size: u64) -> anyhow::Result<()> {
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
