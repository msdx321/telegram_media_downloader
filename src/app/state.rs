use std::sync::Arc;
use std::time::Instant;

use log::info;
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use tokio::sync::Mutex;

use crate::config::{ChatConfig, ChatData, Config, save_app_data, save_config};

use super::{CONFIG_FILE, DATA_FILE};

/// Build `data.yaml` from the live file-id cache and per-chat retry sets and
/// write it. `config.yaml` is updated incrementally by `update_chat_state` as
/// each chat finishes, so only `data.yaml` is saved here.
pub(super) async fn persist_state(
    file_ids: &Arc<Mutex<HashSet<String>>>,
    data_chats: &HashMap<String, ChatData>,
) -> anyhow::Result<()> {
    let mut data = crate::config::AppData::default();
    let mut ids: Vec<String> = file_ids.lock().await.iter().cloned().collect();
    ids.sort();
    data.downloaded_file_ids = ids;
    let mut chats: Vec<ChatData> = data_chats.values().cloned().collect();
    chats.sort_by(|a, b| a.chat_id.cmp(&b.chat_id));
    data.chat = chats;
    save_app_data(DATA_FILE, &data)
}

pub(super) async fn sync_retry_set(
    data_chats: &mut HashMap<String, ChatData>,
    chat_id: &str,
    live_retry: &Arc<Mutex<HashSet<i32>>>,
) {
    let mut retry: Vec<i32> = live_retry.lock().await.iter().copied().collect();
    retry.sort_unstable();
    if retry.is_empty() {
        data_chats.remove(chat_id);
    } else {
        data_chats.insert(
            chat_id.to_string(),
            ChatData {
                chat_id: chat_id.to_string(),
                ids_to_retry: retry,
            },
        );
    }
}

pub(super) fn update_chat_state(
    cfg: &mut Config,
    chat_cfg: &ChatConfig,
    last_read: i32,
) -> anyhow::Result<()> {
    for c in &mut cfg.chat {
        if c.chat_id == chat_cfg.chat_id {
            c.last_read_message_id = c.last_read_message_id.max(last_read);
            break;
        }
    }
    save_config(CONFIG_FILE, cfg)?;
    Ok(())
}

/// Log a one-time summary of the loaded configuration at startup.
pub(super) fn log_config_summary(
    cfg: &Config,
    data_chats: &HashMap<String, ChatData>,
    concurrency: usize,
) {
    let pending: usize = data_chats.values().map(|c| c.ids_to_retry.len()).sum();
    info!(
        "config: {} chat(s), {concurrency} parallel download slot(s), scan every {}s, save_path={}",
        cfg.chat.len(),
        cfg.check_interval_secs,
        cfg.save_path.display()
    );
    info!(
        "config: media_types=[{}] web_ui={}:{}",
        cfg.media_types.join(","),
        cfg.web_host,
        cfg.web_port
    );
    for c in &cfg.chat {
        let retry = data_chats
            .get(&c.chat_id)
            .map(|d| d.ids_to_retry.len())
            .unwrap_or(0);
        info!(
            "config: chat '{}' from msg {} ({} id(s) queued for retry){}",
            c.chat_id,
            c.last_read_message_id,
            retry,
            c.download_filter
                .as_deref()
                .map(|f| format!(" filter='{f}'"))
                .unwrap_or_default()
        );
    }
    if pending > 0 {
        info!("config: resuming with {pending} message id(s) pending retry across all chats");
    }
}

/// Log what was preserved across the run so the user knows resume is safe.
pub(super) async fn log_shutdown_summary(
    cfg: &Config,
    data_chats: &HashMap<String, ChatData>,
    file_ids: &Arc<Mutex<HashSet<String>>>,
    started: Instant,
) {
    let file_id_count = file_ids.lock().await.len();
    let pending: usize = data_chats.values().map(|c| c.ids_to_retry.len()).sum();
    info!(
        "shutdown: ran for {:.1}s; {file_id_count} file id(s) cached, {pending} message id(s) pending retry",
        started.elapsed().as_secs_f64()
    );
    for c in &cfg.chat {
        let retry = data_chats
            .get(&c.chat_id)
            .map(|d| d.ids_to_retry.len())
            .unwrap_or(0);
        info!(
            "shutdown: chat '{}' last_read={} ({} id(s) to retry) - partial downloads kept as .part for resume",
            c.chat_id, c.last_read_message_id, retry
        );
    }
    info!("shutdown: state flushed to {DATA_FILE} and {CONFIG_FILE}");
}
