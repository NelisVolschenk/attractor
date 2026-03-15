//! Named artifact storage for large stage outputs.
//!
//! The context KV store is meant for small scalar routing values.  Large
//! blobs (prompt text, LLM responses) go in the [`ArtifactStore`].  Artifacts
//! below [`FILE_BACKING_THRESHOLD`] stay in memory; larger ones are written to
//! `{base_dir}/artifacts/{id}.json`.

use crate::error::EngineError;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

/// Artifacts whose JSON-serialised size meets or exceeds this threshold are
/// written to disk (when a `base_dir` is configured).
pub const FILE_BACKING_THRESHOLD: usize = 100 * 1024; // 100 KB

// ---------------------------------------------------------------------------
// ArtifactInfo
// ---------------------------------------------------------------------------

/// Metadata about a stored artifact.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactInfo {
    pub id: String,
    pub name: String,
    pub size_bytes: usize,
    pub stored_at: DateTime<Utc>,
    pub is_file_backed: bool,
}

// ---------------------------------------------------------------------------
// Internal entry
// ---------------------------------------------------------------------------

enum ArtifactEntry {
    Memory(JsonValue),
    FileBacked(PathBuf),
}

struct ArtifactStoreInner {
    entries: HashMap<String, (ArtifactInfo, ArtifactEntry)>,
    base_dir: Option<PathBuf>,
}

// ---------------------------------------------------------------------------
// ArtifactStore
// ---------------------------------------------------------------------------

/// Named, typed artifact storage for large stage outputs.
///
/// Thread-safe via `Arc<RwLock<…>>`.  Cheaply cloneable.
#[derive(Clone)]
pub struct ArtifactStore {
    inner: Arc<RwLock<ArtifactStoreInner>>,
}

impl std::fmt::Debug for ArtifactStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArtifactStore").finish()
    }
}

impl ArtifactStore {
    /// Create a new store.  Pass `Some(dir)` to enable file-backing for large
    /// artifacts.
    pub fn new(base_dir: Option<PathBuf>) -> Self {
        ArtifactStore {
            inner: Arc::new(RwLock::new(ArtifactStoreInner {
                entries: HashMap::new(),
                base_dir,
            })),
        }
    }

    /// Store a JSON artifact and return its metadata.
    ///
    /// If the serialised size ≥ [`FILE_BACKING_THRESHOLD`] **and** a
    /// `base_dir` is configured, the artifact is written to
    /// `{base_dir}/artifacts/{id}.json` and flagged as file-backed.
    /// Otherwise it is kept in memory.
    ///
    /// Overwriting an existing `id` replaces the previous entry.
    pub fn store(&self, id: &str, name: &str, data: JsonValue) -> ArtifactInfo {
        let serialised = serde_json::to_string(&data).unwrap_or_else(|_| "{}".to_string());
        let size_bytes = serialised.len();

        let mut guard = self
            .inner
            .write()
            .expect("artifact store write lock poisoned");

        let is_file_backed = size_bytes >= FILE_BACKING_THRESHOLD && guard.base_dir.is_some();

        let entry = if is_file_backed {
            let base_dir = guard.base_dir.as_ref().unwrap();
            let artifacts_dir = base_dir.join("artifacts");
            std::fs::create_dir_all(&artifacts_dir).unwrap_or(()); // best-effort; fall back to memory on error
            let safe_id = id.replace('/', "_");
            let file_path = artifacts_dir.join(format!("{safe_id}.json"));
            if std::fs::write(&file_path, &serialised).is_ok() {
                ArtifactEntry::FileBacked(file_path)
            } else {
                // Disk write failed — keep in memory.
                ArtifactEntry::Memory(data)
            }
        } else {
            ArtifactEntry::Memory(data)
        };

        let info = ArtifactInfo {
            id: id.to_owned(),
            name: name.to_owned(),
            size_bytes,
            stored_at: Utc::now(),
            is_file_backed,
        };

        guard.entries.insert(id.to_owned(), (info.clone(), entry));
        info
    }

    /// Retrieve artifact data by ID.
    ///
    /// Returns [`EngineError::Artifact`] if the ID is not found or the
    /// backing file cannot be read.
    pub fn retrieve(&self, id: &str) -> Result<JsonValue, EngineError> {
        let guard = self
            .inner
            .read()
            .expect("artifact store read lock poisoned");
        let (info, entry) = guard
            .entries
            .get(id)
            .ok_or_else(|| EngineError::Artifact(format!("artifact not found: {id}")))?;
        match entry {
            ArtifactEntry::Memory(v) => Ok(v.clone()),
            ArtifactEntry::FileBacked(path) => {
                let text = std::fs::read_to_string(path).map_err(|e| {
                    EngineError::Artifact(format!(
                        "failed to read artifact '{}' from '{}': {e}",
                        info.id,
                        path.display()
                    ))
                })?;
                serde_json::from_str(&text).map_err(|e| {
                    EngineError::Artifact(format!("failed to parse artifact '{}': {e}", info.id))
                })
            }
        }
    }

    /// Returns `true` if an artifact with `id` exists.
    pub fn has(&self, id: &str) -> bool {
        self.inner
            .read()
            .expect("artifact store read lock poisoned")
            .entries
            .contains_key(id)
    }

    /// List metadata for all stored artifacts, sorted by `stored_at`.
    pub fn list(&self) -> Vec<ArtifactInfo> {
        let guard = self
            .inner
            .read()
            .expect("artifact store read lock poisoned");
        let mut infos: Vec<ArtifactInfo> = guard
            .entries
            .values()
            .map(|(info, _)| info.clone())
            .collect();
        infos.sort_by_key(|i| i.stored_at);
        infos
    }

    /// Remove an artifact by ID.  No-op if the ID is absent.
    ///
    /// Deletes the backing file if the artifact is file-backed.
    pub fn remove(&self, id: &str) {
        let mut guard = self
            .inner
            .write()
            .expect("artifact store write lock poisoned");
        if let Some((_, ArtifactEntry::FileBacked(path))) = guard.entries.remove(id) {
            let _ = std::fs::remove_file(path); // best-effort
        }
    }

    /// Remove all artifacts.  Deletes all backing files.
    pub fn clear(&self) {
        let mut guard = self
            .inner
            .write()
            .expect("artifact store write lock poisoned");
        for (_, entry) in guard.entries.values() {
            if let ArtifactEntry::FileBacked(path) = entry {
                let _ = std::fs::remove_file(path);
            }
        }
        guard.entries.clear();
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn store_and_retrieve_memory() {
        let store = ArtifactStore::new(None);
        let data = json!({"key": "value"});
        store.store("a1", "my artifact", data.clone());
        let retrieved = store.retrieve("a1").unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn retrieve_missing_returns_error() {
        let store = ArtifactStore::new(None);
        let result = store.retrieve("nope");
        assert!(matches!(result.unwrap_err(), EngineError::Artifact(_)));
    }

    #[test]
    fn has_before_and_after_store() {
        let store = ArtifactStore::new(None);
        assert!(!store.has("x"));
        store.store("x", "X", json!(1));
        assert!(store.has("x"));
    }

    #[test]
    fn list_returns_all_sorted() {
        let store = ArtifactStore::new(None);
        store.store("b", "B", json!(2));
        store.store("a", "A", json!(1));
        let infos = store.list();
        assert_eq!(infos.len(), 2);
        // sorted by stored_at; names appear
        let names: Vec<&str> = infos.iter().map(|i| i.name.as_str()).collect();
        assert!(names.contains(&"A") && names.contains(&"B"));
    }

    #[test]
    fn remove_deletes_entry() {
        let store = ArtifactStore::new(None);
        store.store("z", "Z", json!(null));
        assert!(store.has("z"));
        store.remove("z");
        assert!(!store.has("z"));
    }

    #[test]
    fn remove_noop_on_missing() {
        let store = ArtifactStore::new(None);
        store.remove("nope"); // must not panic
    }

    #[test]
    fn clear_empties_store() {
        let store = ArtifactStore::new(None);
        store.store("a", "A", json!(1));
        store.store("b", "B", json!(2));
        store.clear();
        assert_eq!(store.list().len(), 0);
    }

    #[test]
    fn overwrite_replaces_entry() {
        let store = ArtifactStore::new(None);
        store.store("k", "old", json!("first"));
        store.store("k", "new", json!("second"));
        let v = store.retrieve("k").unwrap();
        assert_eq!(v, json!("second"));
    }

    #[test]
    fn file_backed_store_and_retrieve() {
        let dir = tempfile::tempdir().unwrap();
        let store = ArtifactStore::new(Some(dir.path().to_path_buf()));

        // Build data > 100 KB
        let big_string = "x".repeat(FILE_BACKING_THRESHOLD + 1);
        let data = json!({"payload": big_string});

        let info = store.store("big", "big artifact", data.clone());
        assert!(info.is_file_backed);

        // Backing file exists
        let file = dir.path().join("artifacts").join("big.json");
        assert!(file.exists());

        let retrieved = store.retrieve("big").unwrap();
        assert_eq!(retrieved["payload"], data["payload"]);
    }

    #[test]
    fn memory_only_never_writes_to_disk() {
        let store = ArtifactStore::new(None);
        let big_string = "x".repeat(FILE_BACKING_THRESHOLD + 1);
        let info = store.store("big", "big", json!(big_string));
        assert!(!info.is_file_backed);
    }

    #[test]
    fn file_backed_remove_deletes_file() {
        let dir = tempfile::tempdir().unwrap();
        let store = ArtifactStore::new(Some(dir.path().to_path_buf()));
        let big_string = "y".repeat(FILE_BACKING_THRESHOLD + 1);
        store.store("del", "del", json!(big_string));
        let file = dir.path().join("artifacts").join("del.json");
        assert!(file.exists());
        store.remove("del");
        assert!(!file.exists());
    }
}
