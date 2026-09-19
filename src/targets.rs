//! Runtime target registry: config targets plus dynamically added ones.
//!
//! `collector.toml` targets are declared with `origin = config` but can be
//! disabled or removed at runtime; those changes are persisted as overrides
//! in `state.targets_file` (atomic temp+rename) so they survive restarts.
//! Targets added through the control API are `origin = dynamic` and stored
//! in the same file.
//!
//! Two modes:
//! * `recurring` — swept by the periodic executor on every `interval_secs`.
//! * `once` — not swept; enqueued as a manual job when added/re-run.
//!
//! Target names are the `scrape_target` label and must stay unique. URLs are
//! deduplicated (exact, trimmed) against both config and dynamic targets.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use crate::config::TargetConfig;

/// Default series-name prefixes accepted from a dynamically added target.
/// Shared with `config.rs` so the built-in default stays in one place.
pub const DEFAULT_ALLOW_PREFIXES: &[&str] =
    &["http_", "beruang_", "tonggeret_", "visitors_", "unique_"];

/// Where a target came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Origin {
    /// Declared in `collector.toml`; overridable via `TargetOverride`.
    Config,
    /// Added through the control API; persisted.
    Dynamic,
}

/// How a target is scheduled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    /// Swept every `interval_secs`.
    #[default]
    Recurring,
    /// Run only when enqueued (manual).
    Once,
}

/// One scrape target in the runtime registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Target {
    /// Stable id (`cfg:<name>` for config; generated for dynamic).
    pub id: String,
    /// `scrape_target` label; unique across the registry.
    pub name: String,
    /// `/metrics` URL.
    pub url: String,
    /// Accepted series-name prefixes.
    pub allow: Vec<String>,
    /// Included in the periodic sweep (dynamic only can change).
    pub enabled: bool,
    /// Config or dynamic.
    pub origin: Origin,
    /// Recurring or once.
    pub mode: Mode,
    /// Unix seconds when added (0 for config).
    #[serde(default)]
    pub created_at: u64,
}

impl Target {
    /// Scrape-pipeline view of this target.
    #[must_use]
    pub fn scrape_config(&self) -> TargetConfig {
        TargetConfig {
            name: self.name.clone(),
            url: self.url.clone(),
            allow: self.allow.clone(),
        }
    }
}

/// Registry mutation failures (mapped to HTTP codes by `serve`).
#[derive(Debug, thiserror::Error)]
pub enum TargetError {
    /// Bad URL / duplicate / empty name.
    #[error("{0}")]
    Invalid(String),
    /// No target with that id.
    #[error("unknown target {0}")]
    NotFound(String),
    /// Dynamic-target cap reached.
    #[error("{0}")]
    Limit(String),
    /// State file could not be written.
    #[error("persist targets: {0}")]
    Persist(String),
}

/// Runtime override for a config target, persisted across restarts.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TargetOverride {
    /// Explicit enable/disable (`None` = leave as config declares).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    /// Tombstone: the config target is removed at runtime.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub removed: bool,
}

/// On-disk shape of `state.targets_file`.
#[derive(Debug, Default, Serialize, Deserialize)]
struct PersistedState {
    /// Dynamically added targets.
    #[serde(default)]
    targets: Vec<Target>,
    /// Config-target overrides, keyed by target id (`cfg:<name>`).
    #[serde(default)]
    overrides: BTreeMap<String, TargetOverride>,
}

/// Config + dynamic targets, with persistence for the dynamic set and
/// config overrides.
#[derive(Debug)]
pub struct TargetRegistry {
    path: PathBuf,
    max_dynamic: usize,
    inner: RwLock<RegistryInner>,
}

#[derive(Debug)]
struct RegistryInner {
    targets: Vec<Target>,
    overrides: BTreeMap<String, TargetOverride>,
}

impl TargetRegistry {
    /// Build from config targets + the persisted state file. A corrupt or
    /// unreadable state file is an error (fail fast, like storage init).
    ///
    /// Backward compatible: an older file holding a bare `[Target, …]` array
    /// (dynamic targets only) is still accepted; the next persist rewrites
    /// it in the object shape.
    pub fn load(
        config_targets: &[TargetConfig],
        path: &Path,
        max_dynamic: usize,
    ) -> Result<Self, String> {
        let mut all: Vec<Target> = config_targets
            .iter()
            .map(|t| Target {
                id: format!("cfg:{}", t.name),
                name: t.name.clone(),
                url: t.url.clone(),
                allow: t.allow.clone(),
                enabled: true,
                origin: Origin::Config,
                mode: Mode::Recurring,
                created_at: 0,
            })
            .collect();

        let mut state = PersistedState::default();
        if path.exists() {
            let text = std::fs::read_to_string(path)
                .map_err(|e| format!("read {}: {e}", path.display()))?;
            state = match serde_json::from_str::<PersistedState>(&text) {
                Ok(s) => s,
                // Legacy shape: a bare array of dynamic targets.
                Err(_) => PersistedState {
                    targets: serde_json::from_str::<Vec<Target>>(&text)
                        .map_err(|e| format!("parse {}: {e}", path.display()))?,
                    overrides: BTreeMap::new(),
                },
            };
        }

        // Apply config overrides (disable / tombstone) before dynamic targets.
        for target in &mut all {
            if let Some(ov) = state.overrides.get(&target.id) {
                if let Some(enabled) = ov.enabled {
                    target.enabled = enabled;
                }
            }
        }

        for mut t in state.targets {
            // Stored files only ever hold dynamic targets; enforce it so a
            // hand-edited file cannot masquerade as config.
            t.origin = Origin::Dynamic;
            if all.iter().any(|x| x.url == t.url || x.name == t.name) {
                return Err(format!(
                    "dynamic target {} conflicts with a config target",
                    t.name
                ));
            }
            all.push(t);
        }

        let dynamic_count = all.iter().filter(|t| t.origin == Origin::Dynamic).count();
        if dynamic_count > max_dynamic {
            return Err(format!(
                "{dynamic_count} dynamic targets exceed max_dynamic_targets {max_dynamic}"
            ));
        }

        Ok(Self {
            path: path.to_path_buf(),
            max_dynamic,
            inner: RwLock::new(RegistryInner {
                targets: all,
                overrides: state.overrides,
            }),
        })
    }

    /// All visible targets (config first, then dynamic), in insertion order.
    /// Tombstoned config targets are filtered out.
    #[must_use]
    pub fn list(&self) -> Vec<Target> {
        self.inner
            .read()
            .map(|i| {
                i.targets
                    .iter()
                    .filter(|t| !i.overrides.get(&t.id).is_some_and(|ov| ov.removed))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Recurring + enabled targets, for the periodic sweep.
    #[must_use]
    pub fn enabled_recurring(&self) -> Vec<Target> {
        self.list()
            .into_iter()
            .filter(|t| t.enabled && t.mode == Mode::Recurring)
            .collect()
    }

    /// Add one dynamic target. `name` defaults to a slug of the URL host.
    /// `allow` empty falls back to [`DEFAULT_ALLOW_PREFIXES`].
    pub fn add_dynamic(
        &self,
        name: Option<&str>,
        url: &str,
        allow: Option<&[String]>,
        mode: Mode,
    ) -> Result<Target, TargetError> {
        let url = validate_url(url)?;
        let mut guard = self
            .inner
            .write()
            .map_err(|_| TargetError::Persist("registry lock poisoned".to_string()))?;

        if guard.targets.iter().any(|t| t.url == url) {
            return Err(TargetError::Invalid(format!(
                "url already configured: {url}"
            )));
        }
        let dynamic = guard
            .targets
            .iter()
            .filter(|t| t.origin == Origin::Dynamic)
            .count();
        if dynamic >= self.max_dynamic {
            return Err(TargetError::Limit(format!(
                "dynamic target limit reached ({})",
                self.max_dynamic
            )));
        }

        let name = match name.map(str::trim).filter(|s| !s.is_empty()) {
            Some(n) => {
                if guard.targets.iter().any(|t| t.name == n) {
                    return Err(TargetError::Invalid(format!("name already used: {n}")));
                }
                n.to_string()
            }
            None => unique_slug(&guard.targets, &slug_from_url(&url)),
        };
        let allow = match allow.filter(|a| !a.is_empty()) {
            Some(a) => a.to_vec(),
            None => DEFAULT_ALLOW_PREFIXES
                .iter()
                .map(ToString::to_string)
                .collect(),
        };
        let target = Target {
            id: next_id(),
            name,
            url,
            allow,
            enabled: mode == Mode::Recurring,
            origin: Origin::Dynamic,
            mode,
            created_at: now_unix(),
        };
        guard.targets.push(target.clone());
        self.persist_locked(&guard)?;
        Ok(target)
    }

    /// Enable/disable any target. Config-target changes are persisted as an
    /// override; enabling/disabling also clears a removal tombstone (the
    /// config target reappears).
    pub fn set_enabled(&self, id: &str, enabled: bool) -> Result<Target, TargetError> {
        let mut guard = self
            .inner
            .write()
            .map_err(|_| TargetError::Persist("registry lock poisoned".to_string()))?;
        let idx = guard
            .targets
            .iter()
            .position(|t| t.id == id)
            .ok_or_else(|| TargetError::NotFound(id.to_string()))?;
        guard.targets[idx].enabled = enabled;
        if guard.targets[idx].origin == Origin::Config {
            let ov = guard.overrides.entry(id.to_string()).or_default();
            ov.enabled = Some(enabled);
            ov.removed = false;
        }
        let updated = guard.targets[idx].clone();
        self.persist_locked(&guard)?;
        Ok(updated)
    }

    /// Remove any target. Dynamic targets are dropped; config targets are
    /// tombstoned (hidden until re-enabled or the override is cleared).
    pub fn remove(&self, id: &str) -> Result<Target, TargetError> {
        let mut guard = self
            .inner
            .write()
            .map_err(|_| TargetError::Persist("registry lock poisoned".to_string()))?;
        let idx = guard
            .targets
            .iter()
            .position(|t| t.id == id)
            .ok_or_else(|| TargetError::NotFound(id.to_string()))?;
        if guard.targets[idx].origin == Origin::Config {
            guard.overrides.entry(id.to_string()).or_default().removed = true;
            let hidden = guard.targets[idx].clone();
            self.persist_locked(&guard)?;
            return Ok(hidden);
        }
        let removed = guard.targets.remove(idx);
        self.persist_locked(&guard)?;
        Ok(removed)
    }

    /// Find a target by exact (trimmed) URL.
    #[must_use]
    pub fn find_by_url(&self, url: &str) -> Option<Target> {
        let url = url.trim();
        self.list().into_iter().find(|t| t.url == url)
    }

    /// Write dynamic targets + config overrides, atomically (temp + rename).
    fn persist_locked(&self, inner: &RegistryInner) -> Result<(), TargetError> {
        let state = PersistedState {
            targets: inner
                .targets
                .iter()
                .filter(|t| t.origin == Origin::Dynamic)
                .cloned()
                .collect(),
            overrides: inner.overrides.clone(),
        };
        let json = serde_json::to_string_pretty(&state)
            .map_err(|e| TargetError::Persist(e.to_string()))?;
        if let Some(parent) = self.path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|e| TargetError::Persist(e.to_string()))?;
            }
        }
        let tmp = self.path.with_extension("json.tmp");
        std::fs::write(&tmp, json).map_err(|e| TargetError::Persist(e.to_string()))?;
        std::fs::rename(&tmp, &self.path).map_err(|e| TargetError::Persist(e.to_string()))?;
        Ok(())
    }
}

/// Validate/normalize a target URL (trimmed, http/https only).
pub fn validate_url(url: &str) -> Result<String, TargetError> {
    let url = url.trim();
    if url.is_empty() {
        return Err(TargetError::Invalid("url must not be empty".to_string()));
    }
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err(TargetError::Invalid(format!(
            "url must start with http(s)://: {url}"
        )));
    }
    if url.chars().any(char::is_whitespace) {
        return Err(TargetError::Invalid(format!("url has whitespace: {url}")));
    }
    Ok(url.to_string())
}

/// Host-derived slug used when no name is supplied.
fn slug_from_url(url: &str) -> String {
    let rest = url.split("://").nth(1).unwrap_or(url);
    let host = rest.split(['/', '?', '#']).next().unwrap_or(rest);
    let host = host.rsplit('@').next().unwrap_or(host);
    let host = host.split(':').next().unwrap_or(host);
    let slug: String = host
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '.' {
                c
            } else {
                '-'
            }
        })
        .collect();
    if slug.is_empty() {
        "target".to_string()
    } else {
        slug
    }
}

/// Ensure `slug` is unique among `targets`, suffixing `-2`, `-3`, …
fn unique_slug(targets: &[Target], slug: &str) -> String {
    if !targets.iter().any(|t| t.name == slug) {
        return slug.to_string();
    }
    for n in 2.. {
        let candidate = format!("{slug}-{n}");
        if !targets.iter().any(|t| t.name == candidate) {
            return candidate;
        }
    }
    unreachable!("unbounded suffix loop always returns")
}

/// Monotonic-ish id: `t<unix>:<counter>`.
fn next_id() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("t{}:{n}", now_unix())
}

/// Unix seconds (0 on clock skew).
#[must_use]
pub fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_targets() -> Vec<TargetConfig> {
        vec![TargetConfig {
            name: "beruang".to_string(),
            url: "http://localhost:8000/metrics".to_string(),
            allow: vec!["http_".to_string()],
        }]
    }

    #[test]
    fn loads_config_and_persists_dynamic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("targets.json");
        let registry = TargetRegistry::load(&config_targets(), &path, 8).unwrap();
        assert_eq!(registry.list().len(), 1);
        assert!(registry
            .find_by_url("http://localhost:8000/metrics")
            .is_some());

        let t = registry
            .add_dynamic(
                Some("orders"),
                "https://orders.example/metrics",
                None,
                Mode::Recurring,
            )
            .unwrap();
        assert_eq!(t.name, "orders");
        assert!(t.enabled);
        assert!(path.exists());

        // Re-load sees the dynamic target.
        let reloaded = TargetRegistry::load(&config_targets(), &path, 8).unwrap();
        assert_eq!(reloaded.list().len(), 2);
        assert_eq!(reloaded.enabled_recurring().len(), 2);
    }

    #[test]
    fn once_targets_are_not_swept() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("targets.json");
        let registry = TargetRegistry::load(&config_targets(), &path, 8).unwrap();
        registry
            .add_dynamic(None, "https://once.example/metrics", None, Mode::Once)
            .unwrap();
        let recurring: Vec<String> = registry
            .enabled_recurring()
            .into_iter()
            .map(|t| t.name)
            .collect();
        assert_eq!(recurring, vec!["beruang".to_string()]);
    }

    #[test]
    fn rejects_bad_and_duplicate_urls() {
        let dir = tempfile::tempdir().unwrap();
        let registry =
            TargetRegistry::load(&config_targets(), &dir.path().join("t.json"), 8).unwrap();
        assert!(registry
            .add_dynamic(None, "ftp://x", None, Mode::Once)
            .is_err());
        assert!(registry
            .add_dynamic(None, "http://localhost:8000/metrics", None, Mode::Once)
            .is_err());
        registry
            .add_dynamic(None, "https://a.example/metrics", None, Mode::Once)
            .unwrap();
        assert!(registry
            .add_dynamic(None, "https://a.example/metrics", None, Mode::Recurring)
            .is_err());
    }

    #[test]
    fn slug_collision_gets_suffix() {
        let dir = tempfile::tempdir().unwrap();
        let registry =
            TargetRegistry::load(&config_targets(), &dir.path().join("t.json"), 8).unwrap();
        let a = registry
            .add_dynamic(None, "https://same.example/a", None, Mode::Once)
            .unwrap();
        let b = registry
            .add_dynamic(None, "https://same.example/b", None, Mode::Once)
            .unwrap();
        assert_eq!(a.name, "same.example");
        assert_eq!(b.name, "same.example-2");
    }

    #[test]
    fn config_targets_can_be_disabled_and_removed_persistently() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.json");
        let registry = TargetRegistry::load(&config_targets(), &path, 1).unwrap();
        let disabled = registry.set_enabled("cfg:beruang", false).unwrap();
        assert!(!disabled.enabled);
        assert!(registry.enabled_recurring().is_empty());

        // Reload: the disable override sticks.
        let reloaded = TargetRegistry::load(&config_targets(), &path, 1).unwrap();
        assert!(!reloaded.list()[0].enabled);
        assert!(reloaded.enabled_recurring().is_empty());

        // Re-enabling clears it.
        reloaded.set_enabled("cfg:beruang", true).unwrap();
        let reloaded = TargetRegistry::load(&config_targets(), &path, 1).unwrap();
        assert!(reloaded.list()[0].enabled);

        // Remove tombstones it across reloads.
        reloaded.remove("cfg:beruang").unwrap();
        assert!(reloaded.list().is_empty());
        let reloaded = TargetRegistry::load(&config_targets(), &path, 1).unwrap();
        assert!(reloaded.list().is_empty());
        assert!(reloaded.enabled_recurring().is_empty());

        // Enabling a tombstoned config target revives it.
        reloaded.set_enabled("cfg:beruang", true).unwrap();
        assert_eq!(reloaded.list().len(), 1);
    }

    #[test]
    fn legacy_array_state_file_still_loads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("targets.json");
        std::fs::write(
            &path,
            r#"[{"id":"t1:0","name":"legacy","url":"https://legacy.example/metrics",
                "allow":["http_"],"enabled":true,"origin":"dynamic","mode":"recurring","created_at":0}]"#,
        )
        .unwrap();
        let registry = TargetRegistry::load(&config_targets(), &path, 8).unwrap();
        let names: Vec<String> = registry.list().into_iter().map(|t| t.name).collect();
        assert_eq!(names, vec!["beruang".to_string(), "legacy".to_string()]);
    }

    #[test]
    fn dynamic_cap_applies() {
        let dir = tempfile::tempdir().unwrap();
        let registry =
            TargetRegistry::load(&config_targets(), &dir.path().join("t.json"), 1).unwrap();
        registry
            .add_dynamic(None, "https://a.example", None, Mode::Once)
            .unwrap();
        let err = registry
            .add_dynamic(None, "https://b.example", None, Mode::Once)
            .unwrap_err();
        assert!(matches!(err, TargetError::Limit(_)));
    }

    #[test]
    fn enable_disable_and_remove_dynamic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("targets.json");
        let registry = TargetRegistry::load(&config_targets(), &path, 8).unwrap();
        let t = registry
            .add_dynamic(None, "https://a.example", None, Mode::Recurring)
            .unwrap();
        assert_eq!(registry.enabled_recurring().len(), 2);
        registry.set_enabled(&t.id, false).unwrap();
        assert_eq!(registry.enabled_recurring().len(), 1);
        let removed = registry.remove(&t.id).unwrap();
        assert_eq!(removed.id, t.id);
        assert_eq!(registry.list().len(), 1);
        let reloaded = TargetRegistry::load(&config_targets(), &path, 8).unwrap();
        assert_eq!(reloaded.list().len(), 1);
    }
}
