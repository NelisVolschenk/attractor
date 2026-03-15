//! Serialisable checkpoint of pipeline execution state.
//!
//! After each node completes the engine calls [`Checkpoint::save`] to write
//! an atomic JSON snapshot.  On restart, [`Checkpoint::load`] restores the
//! snapshot so the run continues from where it left off.

use crate::error::EngineError;
use crate::graph::Value;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

// ---------------------------------------------------------------------------
// Checkpoint
// ---------------------------------------------------------------------------

/// Serialisable snapshot of pipeline execution state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    /// When this checkpoint was written.
    pub timestamp: DateTime<Utc>,
    /// ID of the last **completed** node.
    pub current_node: String,
    /// IDs of all completed nodes in order.
    pub completed_nodes: Vec<String>,
    /// Per-node retry counters.
    pub node_retries: HashMap<String, u32>,
    /// Full context snapshot at checkpoint time.
    pub context_values: HashMap<String, Value>,
    /// Run-log entries accumulated so far.
    pub logs: Vec<String>,
}

impl Checkpoint {
    /// Create a fresh checkpoint (no nodes completed yet).
    pub fn initial() -> Self {
        Checkpoint {
            timestamp: Utc::now(),
            current_node: String::new(),
            completed_nodes: Vec::new(),
            node_retries: HashMap::new(),
            context_values: HashMap::new(),
            logs: Vec::new(),
        }
    }

    /// Canonical checkpoint path inside a run directory.
    ///
    /// Always `{logs_root}/checkpoint.json`.
    pub fn default_path(logs_root: &Path) -> PathBuf {
        logs_root.join("checkpoint.json")
    }

    /// Returns `true` when a checkpoint file already exists at
    /// `{logs_root}/checkpoint.json`.
    pub fn exists(logs_root: &Path) -> bool {
        Self::default_path(logs_root).exists()
    }

    /// Serialise to pretty JSON and write atomically via a `.tmp` rename.
    ///
    /// Creates the parent directory if it does not yet exist.
    pub fn save(&self, path: &Path) -> Result<(), EngineError> {
        // Ensure the parent directory exists.
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                EngineError::Checkpoint(format!(
                    "failed to create checkpoint directory '{}': {e}",
                    parent.display()
                ))
            })?;
        }

        let json = serde_json::to_string_pretty(self)
            .map_err(|e| EngineError::Checkpoint(format!("failed to serialise checkpoint: {e}")))?;

        // Write to a temp file, then rename for atomicity.
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, &json).map_err(|e| {
            EngineError::Checkpoint(format!(
                "failed to write checkpoint temp file '{}': {e}",
                tmp.display()
            ))
        })?;
        std::fs::rename(&tmp, path).map_err(|e| {
            EngineError::Checkpoint(format!(
                "failed to rename checkpoint to '{}': {e}",
                path.display()
            ))
        })?;

        Ok(())
    }

    /// Deserialise a checkpoint from a JSON file.
    pub fn load(path: &Path) -> Result<Self, EngineError> {
        let text = std::fs::read_to_string(path).map_err(|e| {
            EngineError::Checkpoint(format!(
                "failed to read checkpoint '{}': {e}",
                path.display()
            ))
        })?;
        serde_json::from_str(&text).map_err(|e| {
            EngineError::Checkpoint(format!(
                "failed to parse checkpoint '{}': {e}",
                path.display()
            ))
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn sample_checkpoint() -> Checkpoint {
        let mut cp = Checkpoint::initial();
        cp.current_node = "plan".to_string();
        cp.completed_nodes = vec!["start".to_string(), "plan".to_string()];
        cp.node_retries.insert("plan".to_string(), 1);
        cp.context_values
            .insert("goal".to_string(), Value::Str("build it".to_string()));
        cp.context_values.insert(
            "timeout".to_string(),
            Value::Duration(Duration::from_secs(30)),
        );
        cp.logs.push("stage started".to_string());
        cp
    }

    #[test]
    fn initial_checkpoint_is_empty() {
        let cp = Checkpoint::initial();
        assert!(cp.current_node.is_empty());
        assert!(cp.completed_nodes.is_empty());
        assert!(cp.node_retries.is_empty());
        assert!(cp.context_values.is_empty());
        assert!(cp.logs.is_empty());
    }

    #[test]
    fn default_path() {
        let root = Path::new("/tmp/run123");
        assert_eq!(
            Checkpoint::default_path(root),
            PathBuf::from("/tmp/run123/checkpoint.json")
        );
    }

    #[test]
    fn save_and_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = Checkpoint::default_path(dir.path());

        let cp = sample_checkpoint();
        cp.save(&path).unwrap();
        assert!(path.exists());

        let loaded = Checkpoint::load(&path).unwrap();
        assert_eq!(loaded.current_node, "plan");
        assert_eq!(loaded.completed_nodes, vec!["start", "plan"]);
        assert_eq!(*loaded.node_retries.get("plan").unwrap(), 1);
        assert_eq!(
            loaded.context_values.get("goal"),
            Some(&Value::Str("build it".to_string()))
        );
        // Duration round-trips correctly
        assert_eq!(
            loaded.context_values.get("timeout"),
            Some(&Value::Duration(Duration::from_secs(30)))
        );
        assert_eq!(loaded.logs, vec!["stage started"]);
    }

    #[test]
    fn save_creates_parent_dir() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("deep").join("nested");
        let path = nested.join("checkpoint.json");

        let cp = Checkpoint::initial();
        cp.save(&path).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn load_invalid_json_returns_engine_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("checkpoint.json");
        std::fs::write(&path, "not valid json").unwrap();

        let result = Checkpoint::load(&path);
        assert!(result.is_err());
        match result.unwrap_err() {
            EngineError::Checkpoint(_) => {}
            other => panic!("expected Checkpoint error, got {other:?}"),
        }
    }

    #[test]
    fn load_missing_file_returns_engine_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("no_such.json");
        let result = Checkpoint::load(&path);
        assert!(matches!(result.unwrap_err(), EngineError::Checkpoint(_)));
    }

    #[test]
    fn exists_false_before_save_true_after() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!Checkpoint::exists(dir.path()));
        let path = Checkpoint::default_path(dir.path());
        Checkpoint::initial().save(&path).unwrap();
        assert!(Checkpoint::exists(dir.path()));
    }

    #[test]
    fn saved_file_is_valid_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = Checkpoint::default_path(dir.path());
        sample_checkpoint().save(&path).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        let _: serde_json::Value = serde_json::from_str(&text).unwrap();
    }
}
