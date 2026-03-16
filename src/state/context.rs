//! Runtime state types: [`Context`], [`Outcome`], [`StageStatus`].
//!
//! `Context` is a thread-safe key-value store shared across all nodes in a
//! pipeline run.  `Outcome` is the structured return value from every handler
//! call.  `StageStatus` enumerates the terminal states a node can produce.

use crate::graph::Value;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

// ---------------------------------------------------------------------------
// StageStatus
// ---------------------------------------------------------------------------

/// Terminal state produced by a node handler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum StageStatus {
    /// Stage completed its work. Proceed to next edge. Reset retry counter.
    #[default]
    Success,
    /// Stage failed permanently. Engine looks for a fail edge or terminates.
    Fail,
    /// Stage completed with caveats. Treated as success for routing.
    PartialSuccess,
    /// Stage requests re-execution within the retry budget.
    Retry,
    /// Stage was skipped (condition not met).
    Skipped,
}

impl StageStatus {
    /// The lowercase string representation used in condition expressions.
    pub fn as_str(self) -> &'static str {
        match self {
            StageStatus::Success => "success",
            StageStatus::Fail => "fail",
            StageStatus::PartialSuccess => "partial_success",
            StageStatus::Retry => "retry",
            StageStatus::Skipped => "skipped",
        }
    }

    /// Returns `true` for `Success` and `PartialSuccess`.
    pub fn is_success(self) -> bool {
        matches!(self, StageStatus::Success | StageStatus::PartialSuccess)
    }
}

impl std::fmt::Display for StageStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ---------------------------------------------------------------------------
// Outcome
// ---------------------------------------------------------------------------

/// The result produced by every node handler.
///
/// The execution engine reads `status` for retry/fail routing, applies
/// `context_updates` to the shared context, and uses `preferred_label` /
/// `suggested_next_ids` during edge selection.
///
/// NLSpec Appendix C: the `status` field serialises as `"outcome"` in
/// `status.json` to match the external file-contract field name.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Outcome {
    /// Terminal status of the node execution.
    ///
    /// Serialised as `"outcome"` in JSON per NLSpec Appendix C.
    #[serde(rename = "outcome")]
    pub status: StageStatus,
    /// Preferred edge label to follow (empty → engine uses other criteria).
    pub preferred_label: String,
    /// Explicit next node IDs to try (empty → engine uses other criteria).
    pub suggested_next_ids: Vec<String>,
    /// Key-value pairs to merge into the run context after execution.
    pub context_updates: HashMap<String, Value>,
    /// Human-readable execution summary.
    pub notes: String,
    /// Reason for failure (populated when `status` is `Fail` or `Retry`).
    pub failure_reason: String,
}

impl Outcome {
    /// Create a successful outcome.
    pub fn success() -> Self {
        Outcome {
            status: StageStatus::Success,
            ..Default::default()
        }
    }

    /// Create a failed outcome with the given reason.
    pub fn fail(reason: impl Into<String>) -> Self {
        Outcome {
            status: StageStatus::Fail,
            failure_reason: reason.into(),
            ..Default::default()
        }
    }

    /// Create a retry outcome with the given reason.
    pub fn retry(reason: impl Into<String>) -> Self {
        Outcome {
            status: StageStatus::Retry,
            failure_reason: reason.into(),
            ..Default::default()
        }
    }
}

// ---------------------------------------------------------------------------
// Context — internal state
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct ContextInner {
    values: HashMap<String, Value>,
    logs: Vec<String>,
}

// ---------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------

/// Thread-safe key-value store shared across all nodes in a pipeline run.
///
/// Backed by `Arc<RwLock<…>>` so cheap clones are possible and parallel
/// readers can proceed concurrently.  Use [`Context::clone_isolated`] for
/// parallel-branch isolation (produces a *separate* `Arc`).
#[derive(Debug, Clone)]
pub struct Context {
    inner: Arc<RwLock<ContextInner>>,
}

impl Default for Context {
    fn default() -> Self {
        Self::new()
    }
}

impl Context {
    /// Create an empty context.
    pub fn new() -> Self {
        Context {
            inner: Arc::new(RwLock::new(ContextInner::default())),
        }
    }

    /// Insert or replace a key.
    pub fn set(&self, key: &str, value: Value) {
        self.inner
            .write()
            .expect("context write lock poisoned")
            .values
            .insert(key.to_owned(), value);
    }

    /// Return a cloned copy of the value for `key`, or `None` if absent.
    pub fn get(&self, key: &str) -> Option<Value> {
        self.inner
            .read()
            .expect("context read lock poisoned")
            .values
            .get(key)
            .cloned()
    }

    /// Return the string representation of `key`'s value, or `""` if absent.
    pub fn get_string(&self, key: &str) -> String {
        self.get(key)
            .map(|v| v.to_string_repr())
            .unwrap_or_default()
    }

    /// Return a cloned snapshot of all current key-value pairs.
    pub fn snapshot(&self) -> HashMap<String, Value> {
        self.inner
            .read()
            .expect("context read lock poisoned")
            .values
            .clone()
    }

    /// Create a new `Context` with the same data but a *separate* `Arc`.
    ///
    /// Mutations to the returned context do **not** affect the original,
    /// and vice-versa.  Used for parallel-branch isolation.
    pub fn clone_isolated(&self) -> Context {
        let guard = self.inner.read().expect("context read lock poisoned");
        Context {
            inner: Arc::new(RwLock::new(ContextInner {
                values: guard.values.clone(),
                logs: guard.logs.clone(),
            })),
        }
    }

    /// Merge all entries in `updates` into the context under a single lock.
    pub fn apply_updates(&self, updates: &HashMap<String, Value>) {
        if updates.is_empty() {
            return;
        }
        let mut guard = self.inner.write().expect("context write lock poisoned");
        for (k, v) in updates {
            guard.values.insert(k.clone(), v.clone());
        }
    }

    /// Append an entry to the run log.
    pub fn append_log(&self, entry: &str) {
        self.inner
            .write()
            .expect("context write lock poisoned")
            .logs
            .push(entry.to_owned());
    }

    /// Return a snapshot of log entries.
    pub fn logs_snapshot(&self) -> Vec<String> {
        self.inner
            .read()
            .expect("context read lock poisoned")
            .logs
            .clone()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_and_get() {
        let ctx = Context::new();
        ctx.set("x", Value::Int(42));
        assert_eq!(ctx.get("x"), Some(Value::Int(42)));
    }

    #[test]
    fn get_missing_key_returns_none() {
        let ctx = Context::new();
        assert_eq!(ctx.get("missing"), None);
    }

    #[test]
    fn get_string_missing_returns_empty() {
        let ctx = Context::new();
        assert_eq!(ctx.get_string("missing"), "");
    }

    #[test]
    fn get_string_coerces_int() {
        let ctx = Context::new();
        ctx.set("n", Value::Int(42));
        assert_eq!(ctx.get_string("n"), "42");
    }

    #[test]
    fn get_string_coerces_bool() {
        let ctx = Context::new();
        ctx.set("flag", Value::Bool(false));
        assert_eq!(ctx.get_string("flag"), "false");
    }

    #[test]
    fn clone_isolated_is_independent() {
        let ctx = Context::new();
        ctx.set("a", Value::Int(1));
        let fork = ctx.clone_isolated();
        fork.set("a", Value::Int(99));
        // original unchanged
        assert_eq!(ctx.get("a"), Some(Value::Int(1)));
        // fork has new value
        assert_eq!(fork.get("a"), Some(Value::Int(99)));
    }

    #[test]
    fn apply_updates_bulk() {
        let ctx = Context::new();
        let mut updates = HashMap::new();
        updates.insert("x".to_string(), Value::Str("hello".to_string()));
        updates.insert("y".to_string(), Value::Int(7));
        ctx.apply_updates(&updates);
        assert_eq!(ctx.get_string("x"), "hello");
        assert_eq!(ctx.get_string("y"), "7");
    }

    #[test]
    fn apply_updates_empty_is_noop() {
        let ctx = Context::new();
        ctx.apply_updates(&HashMap::new()); // must not panic
    }

    #[test]
    fn snapshot_returns_copy() {
        let ctx = Context::new();
        ctx.set("k", Value::Bool(true));
        let snap = ctx.snapshot();
        assert_eq!(snap.get("k"), Some(&Value::Bool(true)));
    }

    #[test]
    fn append_and_logs_snapshot() {
        let ctx = Context::new();
        ctx.append_log("line 1");
        ctx.append_log("line 2");
        let logs = ctx.logs_snapshot();
        assert_eq!(logs, vec!["line 1", "line 2"]);
    }

    #[test]
    fn outcome_success() {
        let o = Outcome::success();
        assert_eq!(o.status, StageStatus::Success);
        assert!(o.failure_reason.is_empty());
    }

    #[test]
    fn outcome_fail() {
        let o = Outcome::fail("bad thing");
        assert_eq!(o.status, StageStatus::Fail);
        assert_eq!(o.failure_reason, "bad thing");
    }

    #[test]
    fn outcome_retry() {
        let o = Outcome::retry("need more time");
        assert_eq!(o.status, StageStatus::Retry);
        assert_eq!(o.failure_reason, "need more time");
    }

    #[test]
    fn stage_status_as_str() {
        assert_eq!(StageStatus::Success.as_str(), "success");
        assert_eq!(StageStatus::Fail.as_str(), "fail");
        assert_eq!(StageStatus::PartialSuccess.as_str(), "partial_success");
        assert_eq!(StageStatus::Retry.as_str(), "retry");
        assert_eq!(StageStatus::Skipped.as_str(), "skipped");
    }

    #[test]
    fn stage_status_is_success() {
        assert!(StageStatus::Success.is_success());
        assert!(StageStatus::PartialSuccess.is_success());
        assert!(!StageStatus::Fail.is_success());
        assert!(!StageStatus::Retry.is_success());
        assert!(!StageStatus::Skipped.is_success());
    }

    #[test]
    fn stage_status_display() {
        assert_eq!(StageStatus::Retry.to_string(), "retry");
    }

    #[test]
    fn outcome_status_field_serializes_as_outcome() {
        // NLSpec Appendix C: status.json field must be named "outcome", not "status".
        let o = Outcome::success();
        let json = serde_json::to_string(&o).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(
            !parsed["outcome"].is_null(),
            "status.json must contain field \"outcome\", got: {json}"
        );
        assert!(
            parsed["status"].is_null(),
            "status.json must NOT contain field \"status\", got: {json}"
        );
        assert_eq!(parsed["outcome"], "success");
    }

    #[test]
    fn outcome_serde_roundtrip() {
        let o = Outcome {
            status: StageStatus::Fail,
            notes: "oops".into(),
            failure_reason: "disk full".into(),
            ..Default::default()
        };
        let json = serde_json::to_string(&o).unwrap();
        let back: Outcome = serde_json::from_str(&json).unwrap();
        assert_eq!(back.status, StageStatus::Fail);
        assert_eq!(back.notes, "oops");
    }

    #[test]
    fn stage_status_serializes_snake_case() {
        let s = serde_json::to_string(&StageStatus::PartialSuccess).unwrap();
        assert_eq!(s, "\"partial_success\"");
    }
}
