//! Collector configuration: TOML file + env overrides + validation.
//!
//! ```toml
//! interval_secs = 15
//! listen = "0.0.0.0:8080"
//! [[targets]]
//! name = "beruang"
//! url = "http://localhost:8000/metrics"
//! ```

use std::path::PathBuf;

/// One Prometheus scrape target.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct TargetConfig {
    /// Short slug, injected as the `scrape_target` label on every sample.
    pub name: String,
    /// Full `/metrics` URL.
    pub url: String,
    /// Series-name prefixes accepted from this target (default covers
    /// HTTP/business series plus the optional visitor series —
    /// see `default_allow`). Never include `collector_`: the scrape
    /// pipeline denies it before the allow check (`scrape::DENY_PREFIX`).
    #[serde(default = "default_allow")]
    pub allow: Vec<String>,
}

/// Fjall hot store + Parquet history tuning.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct FjallSection {
    /// Keyspace directory (created if missing).
    #[serde(default = "default_fjall_dir")]
    pub dir: PathBuf,
    /// Cold `metrics_cold_*.parquet` directory.
    #[serde(default = "default_cold_dir")]
    pub cold_dir: PathBuf,
    /// Hot age after which keys compact to Parquet.
    #[serde(default = "default_retention_days")]
    pub retention_days: u64,
    /// Cold files older than this are purged (must exceed `retention_days`).
    #[serde(default = "default_cold_purge_days")]
    pub cold_purge_days: u64,
}

/// Runtime state: dynamically added targets.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct StateSection {
    /// JSON file persisting targets added through the control API.
    #[serde(default = "default_targets_file")]
    pub targets_file: PathBuf,
    /// Cap on dynamically added targets (config targets are exempt).
    #[serde(default = "default_max_dynamic_targets")]
    pub max_dynamic_targets: usize,
}

impl Default for StateSection {
    fn default() -> Self {
        Self {
            targets_file: default_targets_file(),
            max_dynamic_targets: default_max_dynamic_targets(),
        }
    }
}

/// Top-level collector config.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct Config {
    /// Seconds between scrape passes.
    #[serde(default = "default_interval_secs")]
    pub interval_secs: u64,
    /// Serve address for UI + `/metrics` + `/telemetry/parquet` + `/api/files`.
    #[serde(default = "default_listen")]
    pub listen: String,
    /// Prebuilt UI bundle directory served at `/`.
    #[serde(default = "default_static_dir")]
    pub static_dir: PathBuf,
    /// Per-target sample cap per scrape.
    #[serde(default = "default_max_samples")]
    pub max_samples_per_scrape: usize,
    /// Recent-samples ring capacity backing `/api/v1/query_range`
    /// (drop-oldest past the cap; see `query::RecentBuffer`).
    #[serde(default = "default_recent_buffer_samples")]
    pub recent_buffer_samples: usize,
    /// Largest accepted exposition body in bytes.
    #[serde(default = "default_max_body_bytes")]
    pub max_body_bytes: usize,
    /// Bounded job-queue capacity (manual + scheduled jobs).
    #[serde(default = "default_queue_capacity")]
    pub queue_capacity: usize,
    /// Optional shared secret required on mutating control endpoints
    /// (`x-control-token`); unset disables auth (`COLLECTOR_CONTROL_TOKEN`).
    #[serde(default)]
    pub control_token: Option<String>,
    /// Worker base URL used by `serve` to reverse-proxy the hot API and
    /// status (`COLLECTOR_UPSTREAM`). Ignored by `worker`.
    #[serde(default = "default_upstream")]
    pub upstream: String,
    /// Scrape targets (empty = serve-only mode, still serves history).
    #[serde(default)]
    pub targets: Vec<TargetConfig>,
    /// Storage tuning.
    #[serde(default)]
    pub fjall: FjallSection,
    /// Runtime state (dynamic targets).
    #[serde(default)]
    pub state: StateSection,
}

fn default_interval_secs() -> u64 {
    15
}

fn default_listen() -> String {
    "0.0.0.0:8080".to_string()
}

fn default_static_dir() -> PathBuf {
    PathBuf::from("dist")
}

fn default_max_samples() -> usize {
    5_000
}

fn default_recent_buffer_samples() -> usize {
    crate::query::DEFAULT_BUFFER_SAMPLES
}

fn default_max_body_bytes() -> usize {
    1_048_576
}

fn default_upstream() -> String {
    "http://127.0.0.1:8081".to_string()
}

fn default_queue_capacity() -> usize {
    256
}

fn default_targets_file() -> PathBuf {
    PathBuf::from("./data/targets.json")
}

fn default_max_dynamic_targets() -> usize {
    64
}

fn default_fjall_dir() -> PathBuf {
    PathBuf::from("./data/fjall")
}

fn default_cold_dir() -> PathBuf {
    PathBuf::from("./data/cold")
}

fn default_retention_days() -> u64 {
    30
}

fn default_cold_purge_days() -> u64 {
    32
}

impl Default for FjallSection {
    fn default() -> Self {
        Self {
            dir: default_fjall_dir(),
            cold_dir: default_cold_dir(),
            retention_days: default_retention_days(),
            cold_purge_days: default_cold_purge_days(),
        }
    }
}

/// Default allowlist: HTTP/business series plus the optional
/// visitor series (`visitors_total`, `unique_visitors_estimate`). An app
/// without visitor instrumentation scrapes fine — it stores zero visitor
/// rows instead of erroring. `collector_*` is deliberately absent: scraped
/// input with that prefix is denied before the allow check, so listing it
/// here could never match.
fn default_allow() -> Vec<String> {
    crate::targets::DEFAULT_ALLOW_PREFIXES
        .iter()
        .map(ToString::to_string)
        .collect()
}

/// Config load/validation errors (fatal at startup: fail fast, like init).
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// Config file unreadable or unparsable.
    #[error("config {path}: {detail}")]
    Load {
        /// File that failed to load.
        path: String,
        /// Why it failed.
        detail: String,
    },
    /// Semantically invalid config.
    #[error("invalid config: {0}")]
    Invalid(String),
}

/// Load `collector.toml` (or `path`), apply env overrides, validate.
pub fn load(path: &str) -> Result<Config, ConfigError> {
    let text = std::fs::read_to_string(path).map_err(|e| ConfigError::Load {
        path: path.to_string(),
        detail: e.to_string(),
    })?;
    let mut cfg: Config = toml::from_str(&text).map_err(|e| ConfigError::Load {
        path: path.to_string(),
        detail: e.to_string(),
    })?;
    if let Ok(listen) = std::env::var("COLLECTOR_LISTEN") {
        if !listen.trim().is_empty() {
            cfg.listen = listen;
        }
    }
    if let Ok(dir) = std::env::var("COLLECTOR_FJALL_DIR") {
        if !dir.trim().is_empty() {
            cfg.fjall.dir = PathBuf::from(dir);
        }
    }
    if let Ok(upstream) = std::env::var("COLLECTOR_UPSTREAM") {
        if !upstream.trim().is_empty() {
            cfg.upstream = upstream;
        }
    }
    if let Ok(token) = std::env::var("COLLECTOR_CONTROL_TOKEN") {
        if !token.trim().is_empty() {
            cfg.control_token = Some(token);
        }
    }
    cfg.validate()?;
    Ok(cfg)
}

impl Config {
    /// Structural checks: intervals sane, targets unique + well-formed,
    /// purge horizon beyond retention.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.interval_secs == 0 {
            return Err(ConfigError::Invalid(
                "interval_secs must be > 0".to_string(),
            ));
        }
        if self.max_samples_per_scrape == 0 {
            return Err(ConfigError::Invalid(
                "max_samples_per_scrape must be > 0".to_string(),
            ));
        }
        if self.recent_buffer_samples == 0 {
            return Err(ConfigError::Invalid(
                "recent_buffer_samples must be > 0".to_string(),
            ));
        }
        if self.max_body_bytes < 1024 {
            return Err(ConfigError::Invalid(
                "max_body_bytes must be >= 1024".to_string(),
            ));
        }
        if self.queue_capacity == 0 {
            return Err(ConfigError::Invalid(
                "queue_capacity must be > 0".to_string(),
            ));
        }
        if self.state.max_dynamic_targets == 0 {
            return Err(ConfigError::Invalid(
                "state.max_dynamic_targets must be > 0".to_string(),
            ));
        }
        if !(self.upstream.trim().is_empty()
            || self.upstream.starts_with("http://")
            || self.upstream.starts_with("https://"))
        {
            return Err(ConfigError::Invalid(
                "upstream must start with http(s)://".to_string(),
            ));
        }
        if self.fjall.retention_days == 0 {
            return Err(ConfigError::Invalid(
                "fjall.retention_days must be > 0".to_string(),
            ));
        }
        if self.fjall.cold_purge_days <= self.fjall.retention_days {
            return Err(ConfigError::Invalid(
                "fjall.cold_purge_days must exceed fjall.retention_days".to_string(),
            ));
        }
        let mut names = std::collections::HashSet::new();
        for t in &self.targets {
            if t.name.trim().is_empty() {
                return Err(ConfigError::Invalid(
                    "target name must not be empty".to_string(),
                ));
            }
            if !names.insert(t.name.clone()) {
                return Err(ConfigError::Invalid(format!(
                    "duplicate target name {:?}",
                    t.name
                )));
            }
            if !(t.url.starts_with("http://") || t.url.starts_with("https://")) {
                return Err(ConfigError::Invalid(format!(
                    "target {:?} url must start with http(s)://",
                    t.name
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal_toml() -> &'static str {
        "[[targets]]\nname = \"app\"\nurl = \"http://localhost:8000/metrics\"\n"
    }

    #[test]
    fn defaults_apply_to_minimal_config() {
        let cfg: Config = toml::from_str(minimal_toml()).unwrap();
        assert_eq!(cfg.interval_secs, 15);
        assert_eq!(cfg.listen, "0.0.0.0:8080");
        assert_eq!(cfg.max_samples_per_scrape, 5_000);
        assert_eq!(cfg.recent_buffer_samples, 20_000);
        assert_eq!(cfg.upstream, "http://127.0.0.1:8081");
        assert_eq!(cfg.queue_capacity, 256);
        assert_eq!(cfg.state.max_dynamic_targets, 64);
        assert_eq!(cfg.state.targets_file, PathBuf::from("./data/targets.json"));
        assert!(cfg.control_token.is_none());
        assert_eq!(cfg.fjall.retention_days, 30);
        assert_eq!(cfg.fjall.cold_purge_days, 32);
        // Visitor prefixes ship in the default allowlist.
        assert!(cfg.targets[0].allow.iter().any(|p| p == "visitors_"));
        assert!(cfg.targets[0].allow.iter().any(|p| p == "unique_"));
        cfg.validate().unwrap();
    }

    #[test]
    fn rejects_bad_configs() {
        let mut cfg: Config = toml::from_str(minimal_toml()).unwrap();
        cfg.interval_secs = 0;
        assert!(cfg.validate().is_err());

        let mut cfg: Config = toml::from_str(minimal_toml()).unwrap();
        cfg.recent_buffer_samples = 0;
        assert!(cfg.validate().is_err());

        let mut cfg: Config = toml::from_str(minimal_toml()).unwrap();
        cfg.fjall.cold_purge_days = cfg.fjall.retention_days;
        assert!(cfg.validate().is_err());

        let mut cfg: Config = toml::from_str(minimal_toml()).unwrap();
        cfg.targets.push(cfg.targets[0].clone());
        assert!(cfg.validate().is_err());

        let mut cfg: Config = toml::from_str(minimal_toml()).unwrap();
        cfg.targets[0].url = "localhost:8000/metrics".to_string();
        assert!(cfg.validate().is_err());

        let mut cfg: Config = toml::from_str(minimal_toml()).unwrap();
        cfg.upstream = "127.0.0.1:8081".to_string();
        assert!(cfg.validate().is_err());

        let mut cfg: Config = toml::from_str(minimal_toml()).unwrap();
        cfg.queue_capacity = 0;
        assert!(cfg.validate().is_err());

        let mut cfg: Config = toml::from_str(minimal_toml()).unwrap();
        cfg.state.max_dynamic_targets = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn missing_file_reports_path() {
        let err = load("./does-not-exist.toml").unwrap_err();
        assert!(err.to_string().contains("does-not-exist.toml"));
    }
}
