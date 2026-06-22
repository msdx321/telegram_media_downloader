use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

/// Configuration loaded from `config.yaml`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub api_id: i32,
    pub api_hash: String,
    #[serde(default)]
    pub chat: Vec<ChatConfig>,
    #[serde(default = "default_media_types")]
    pub media_types: Vec<String>,
    #[serde(default)]
    pub file_formats: FileFormats,
    #[serde(default = "default_save_path")]
    pub save_path: PathBuf,
    #[serde(default = "default_file_path_prefix")]
    pub file_path_prefix: Vec<String>,
    #[serde(default = "default_file_name_prefix")]
    pub file_name_prefix: Vec<String>,
    #[serde(default = "default_file_name_prefix_split")]
    pub file_name_prefix_split: String,
    #[serde(default = "default_max_download_task")]
    pub max_download_task: usize,
    #[serde(default = "default_download_connections")]
    pub download_connections: usize,
    #[serde(default = "default_web_host")]
    pub web_host: String,
    #[serde(default = "default_web_port")]
    pub web_port: u16,
    #[serde(default = "default_check_interval_secs")]
    pub check_interval_secs: u64,
    #[serde(default = "default_date_format")]
    pub date_format: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatConfig {
    pub chat_id: String,
    #[serde(default)]
    pub last_read_message_id: i32,
    #[serde(default)]
    pub download_filter: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FileFormats {
    #[serde(default = "default_all")]
    pub audio: Vec<String>,
    #[serde(default = "default_all")]
    pub document: Vec<String>,
    #[serde(default = "default_all")]
    pub video: Vec<String>,
}

/// Runtime app state persisted to `data.yaml`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AppData {
    #[serde(default)]
    pub chat: Vec<ChatData>,
    #[serde(default)]
    pub downloaded_file_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatData {
    pub chat_id: String,
    #[serde(default)]
    pub ids_to_retry: Vec<i32>,
}

fn default_media_types() -> Vec<String> {
    vec![
        "audio".into(),
        "photo".into(),
        "video".into(),
        "document".into(),
        "voice".into(),
        "video_note".into(),
    ]
}

fn default_save_path() -> PathBuf {
    PathBuf::from("downloads")
}

fn default_file_path_prefix() -> Vec<String> {
    vec!["chat_title".into(), "media_datetime".into()]
}

fn default_file_name_prefix() -> Vec<String> {
    vec!["message_id".into(), "file_name".into()]
}

fn default_file_name_prefix_split() -> String {
    " - ".into()
}

fn default_date_format() -> String {
    "%Y_%m".into()
}

fn default_web_host() -> String {
    "0.0.0.0".into()
}

fn default_web_port() -> u16 {
    5000
}

fn default_check_interval_secs() -> u64 {
    15 * 60
}

fn default_all() -> Vec<String> {
    vec!["all".into()]
}

fn default_max_download_task() -> usize {
    5
}

fn default_download_connections() -> usize {
    4
}

pub fn load_config(path: &str) -> anyhow::Result<Config> {
    let contents = fs::read_to_string(path).with_context(|| format!("cannot read {path}"))?;
    serde_yaml::from_str(&contents).with_context(|| format!("invalid YAML in {path}"))
}

pub fn load_app_data(path: &str) -> anyhow::Result<AppData> {
    if !std::path::Path::new(path).exists() {
        return Ok(AppData::default());
    }
    let contents = fs::read_to_string(path).with_context(|| format!("cannot read {path}"))?;
    serde_yaml::from_str(&contents).with_context(|| format!("invalid YAML in {path}"))
}

pub fn save_app_data(path: &str, data: &AppData) -> anyhow::Result<()> {
    let yaml = serde_yaml::to_string(data)?;
    fs::write(path, yaml).with_context(|| format!("cannot write {path}"))
}

pub fn save_config(path: &str, config: &Config) -> anyhow::Result<()> {
    let yaml = serde_yaml::to_string(config)?;
    fs::write(path, yaml).with_context(|| format!("cannot write {path}"))
}
