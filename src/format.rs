use regex::Regex;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

/// Match reserved filename chars (like the Python validator).
static RE_BAD_CHARS: LazyLock<Regex> = LazyLock::new(|| Regex::new(r#"[/\\:*?"<>|\n]"#).unwrap());

/// Byte unit constants.
const BYTE_UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB", "PB"];

static RE_BYTE_STR: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"^(\d+)\s*(B|KB|MB|GB|TB|PB)$"#).unwrap());

static RE_DATETIME_LIT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"\d{4}[-/\.]\d{1,2}[-/\.]\d{1,2}\s+\d{1,2}:\d{1,2}:\d{1,2}"#).unwrap()
});

// ── public API ───────────────────────────────────────────────────────────

pub fn validate_title(title: &str) -> String {
    RE_BAD_CHARS.replace_all(title, "_").to_string()
}

pub fn parse_byte_str(s: &str) -> Option<u64> {
    let caps = RE_BYTE_STR.captures(s)?;
    let num: u64 = caps.get(1)?.as_str().parse().ok()?;
    let unit = caps.get(2)?.as_str();
    let power = BYTE_UNITS.iter().position(|u| *u == unit)? as u32;
    Some(num * 1024u64.pow(power))
}

pub fn format_byte(size: f64) -> String {
    if (0.0..1.0).contains(&size) {
        return format!("{:.0}b", size / 0.125);
    }
    let mut value = size;
    for unit in BYTE_UNITS {
        if value < 1024.0 {
            return format!("{:.1}{unit}", value);
        }
        value /= 1024.0;
    }
    format!("{:.1}PB", value)
}

/// Truncate the leaf filename so its UTF-8 byte length ≤ `limit`.
pub fn truncate_filename(path: &Path, limit: usize) -> PathBuf {
    let parent = path.parent().unwrap_or(Path::new(""));
    let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
    let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");

    let ext_bytes = ext.len();
    let dot = if ext.is_empty() { 0 } else { 1 };
    let max_stem = limit.saturating_sub(ext_bytes + dot);

    // Build UTF-8 string byte-by-byte so we don't split a char.
    let mut truncated = String::with_capacity(max_stem);
    for ch in stem.chars() {
        if truncated.len() + ch.len_utf8() > max_stem {
            break;
        }
        truncated.push(ch);
    }

    if ext.is_empty() {
        parent.join(truncated)
    } else {
        parent.join(format!("{truncated}.{ext}"))
    }
}

/// Replace date/time placeholders in filter strings (e.g. `>= 2023-01-01 00:00:00`).
/// Returns the string unchanged if no date patterns are found.
pub fn replace_date_time(text: &str, fmt: &str) -> String {
    use chrono::NaiveDateTime;

    let mut result = String::new();
    let mut last_end = 0;

    for m in RE_DATETIME_LIT.find_iter(text) {
        result.push_str(&text[last_end..m.start()]);
        let raw = m.as_str().replace(['/', '.'], "-");
        if let Ok(dt) = NaiveDateTime::parse_from_str(&raw, "%Y-%m-%d %H:%M:%S") {
            result.push_str(&dt.format(fmt).to_string());
        } else {
            result.push_str(m.as_str());
        }
        last_end = m.end();
    }
    result.push_str(&text[last_end..]);
    result
}
