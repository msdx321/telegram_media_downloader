use std::collections::BTreeMap;
use std::convert::Infallible;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use log::{info, warn};
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, broadcast, watch};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{StreamExt, once};

use crate::format::format_byte;

const INDEX_HTML: &str = include_str!("../../static/index.html");
const APP_CSS: &str = include_str!("../../static/app.css");
const PROGRESS_UPDATE_MS: u64 = 2000;

pub struct WebState {
    paused: AtomicBool,
    last_progress_publish_ms: AtomicU64,
    pause_tx: watch::Sender<bool>,
    status: Mutex<String>,
    stats: Mutex<DashboardStats>,
    updates: broadcast::Sender<String>,
}

#[derive(Default)]
struct DashboardStats {
    downloaded_files: u64,
    downloaded_bytes: u64,
    active: BTreeMap<i32, DownloadStat>,
}

struct DownloadStat {
    file_name: String,
    path: String,
    downloaded: u64,
    total: u64,
    speed_bps: u64,
}

#[derive(Serialize)]
struct DashboardSnapshot {
    status: String,
    paused: bool,
    downloaded_files: u64,
    downloaded_bytes: String,
    active_count: usize,
    active: Vec<DownloadSnapshot>,
}

#[derive(Serialize)]
struct DownloadSnapshot {
    msg_id: i32,
    file_name: String,
    path: String,
    downloaded: String,
    total: String,
    speed: String,
    percent: f64,
}

impl WebState {
    pub fn new() -> Self {
        let (updates, _) = broadcast::channel(64);
        let (pause_tx, _) = watch::channel(false);
        Self {
            paused: AtomicBool::new(false),
            last_progress_publish_ms: AtomicU64::new(0),
            pause_tx,
            status: Mutex::new("starting".to_string()),
            stats: Mutex::new(DashboardStats::default()),
            updates,
        }
    }

    pub async fn set_status(&self, status: &str) {
        *self.status.lock().await = status.to_string();
        self.publish().await;
    }

    pub async fn wait_if_paused(&self) {
        if !self.paused.load(Ordering::Relaxed) {
            return;
        }
        let mut pause_rx = self.pause_tx.subscribe();
        while self.paused.load(Ordering::Relaxed) {
            if pause_rx.changed().await.is_err() {
                break;
            }
        }
    }

    pub async fn download_started(&self, msg_id: i32, path: &Path, downloaded: u64, total: u64) {
        let mut stats = self.stats.lock().await;
        stats.active.insert(
            msg_id,
            DownloadStat {
                file_name: path
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned())
                    .unwrap_or_else(|| path.display().to_string()),
                path: path.display().to_string(),
                downloaded,
                total,
                speed_bps: 0,
            },
        );
        drop(stats);
        self.publish().await;
    }

    pub async fn download_progress(&self, msg_id: i32, downloaded: u64, speed_bps: u64) {
        if let Some(item) = self.stats.lock().await.active.get_mut(&msg_id) {
            item.downloaded = downloaded;
            item.speed_bps = speed_bps;
        }
        self.publish_progress().await;
    }

    pub async fn download_finished(&self, msg_id: i32, bytes: u64, completed: bool) {
        let mut stats = self.stats.lock().await;
        stats.active.remove(&msg_id);
        if completed {
            stats.downloaded_files += 1;
            stats.downloaded_bytes += bytes;
        }
        drop(stats);
        self.publish().await;
    }

    async fn set_paused(&self, paused: bool) {
        self.paused.store(paused, Ordering::Relaxed);
        let _ = self.pause_tx.send(paused);
        self.set_status(if paused { "paused" } else { "running" })
            .await;
    }

    fn subscribe(&self) -> broadcast::Receiver<String> {
        self.updates.subscribe()
    }

    async fn publish_progress(&self) {
        let now = now_millis();
        let mut last = self.last_progress_publish_ms.load(Ordering::Relaxed);
        loop {
            if now.saturating_sub(last) < PROGRESS_UPDATE_MS {
                return;
            }
            match self.last_progress_publish_ms.compare_exchange_weak(
                last,
                now,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => last = observed,
            }
        }
        self.publish().await;
    }

    async fn publish(&self) {
        if self.updates.receiver_count() == 0 {
            return;
        }
        let _ = self.updates.send(self.snapshot_json().await);
    }

    async fn snapshot_json(&self) -> String {
        serde_json::to_string(&self.snapshot().await).unwrap_or_else(|_| "{}".to_string())
    }

    async fn snapshot(&self) -> DashboardSnapshot {
        let status = self.status.lock().await.clone();
        let stats = self.stats.lock().await;
        let active = stats
            .active
            .iter()
            .map(|(msg_id, item)| {
                let percent = if item.total == 0 {
                    0.0
                } else {
                    item.downloaded as f64 * 100.0 / item.total as f64
                };
                DownloadSnapshot {
                    msg_id: *msg_id,
                    file_name: item.file_name.clone(),
                    path: item.path.clone(),
                    downloaded: format_byte(item.downloaded as f64),
                    total: format_byte(item.total as f64),
                    speed: format!("{}/s", format_byte(item.speed_bps as f64)),
                    percent,
                }
            })
            .collect();

        DashboardSnapshot {
            status,
            paused: self.paused.load(Ordering::Relaxed),
            downloaded_files: stats.downloaded_files,
            downloaded_bytes: format_byte(stats.downloaded_bytes as f64),
            active_count: stats.active.len(),
            active,
        }
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub async fn run(state: Arc<WebState>, host: String, port: u16) {
    let addr = format!("{host}:{port}");
    let listener = match TcpListener::bind(&addr).await {
        Ok(listener) => listener,
        Err(e) => {
            warn!("web ui: cannot bind {addr}: {e}");
            return;
        }
    };
    info!("web ui: http://{addr}");

    let app = Router::new()
        .route("/", get(index))
        .route("/events", get(events))
        .route("/pause", post(pause))
        .route("/resume", post(resume))
        .route("/static/app.css", get(app_css))
        .with_state(state);

    if let Err(e) = axum::serve(listener, app).await {
        warn!("web ui: server failed: {e}");
    }
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn app_css() -> impl IntoResponse {
    ([("content-type", "text/css; charset=utf-8")], APP_CSS)
}

async fn pause(State(state): State<Arc<WebState>>) {
    state.set_paused(true).await;
}

async fn resume(State(state): State<Arc<WebState>>) {
    state.set_paused(false).await;
}

async fn events(
    State(state): State<Arc<WebState>>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let initial = once(Ok(Event::default().data(state.snapshot_json().await)));
    let updates = BroadcastStream::new(state.subscribe())
        .filter_map(Result::ok)
        .map(|snapshot| Ok(Event::default().data(snapshot)));

    Sse::new(initial.chain(updates)).keep_alive(KeepAlive::default())
}
