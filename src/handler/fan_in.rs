//! Fan-in handler for `shape=tripleoctagon` / `type="parallel.fan_in"` nodes.
//!
//! Reads parallel branch results stored in the context under the
//! `"parallel.results"` key (written by [`ParallelHandler`]), ranks them
//! using a heuristic, and records the winner.

use crate::error::EngineError;
use crate::graph::{Graph, Node, Value};
use crate::handler::Handler;
use crate::state::context::{Context, Outcome, StageStatus};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::path::Path;

// ---------------------------------------------------------------------------
// BranchResult (shared with ParallelHandler)
// ---------------------------------------------------------------------------

/// A single branch result serialised by [`crate::handler::parallel::ParallelHandler`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchResult {
    /// Target node ID of the branch edge.
    pub branch_id: String,
    /// Execution status of the branch.
    pub status: StageStatus,
    /// Human-readable notes.
    pub notes: String,
}

// ---------------------------------------------------------------------------
// heuristic_select
// ---------------------------------------------------------------------------

/// Select the best candidate from a slice of branch results.
///
/// Ranking: SUCCESS=0, PARTIAL_SUCCESS=1, RETRY=2, FAIL=3.
/// Tiebreak: lexicographic ascending on `branch_id`.
///
/// Returns `None` if the slice is empty.
pub fn heuristic_select(results: &[BranchResult]) -> Option<&BranchResult> {
    results.iter().min_by(|a, b| {
        rank(a.status)
            .cmp(&rank(b.status))
            .then_with(|| a.branch_id.cmp(&b.branch_id))
    })
}

fn rank(status: StageStatus) -> u8 {
    match status {
        StageStatus::Success => 0,
        StageStatus::PartialSuccess => 1,
        StageStatus::Retry => 2,
        StageStatus::Skipped => 2,
        StageStatus::Fail => 3,
    }
}

// ---------------------------------------------------------------------------
// FanInHandler
// ---------------------------------------------------------------------------

/// Handler for `shape=tripleoctagon` (`type="parallel.fan_in"`) nodes.
///
/// Reads branch results from `context["parallel.results"]`, picks the best
/// candidate using [`heuristic_select`], and records it in the context.
pub struct FanInHandler;

impl FanInHandler {
    pub fn new() -> Self {
        FanInHandler
    }
}

impl Default for FanInHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Handler for FanInHandler {
    async fn execute(
        &self,
        _node: &Node,
        context: &Context,
        _graph: &Graph,
        _logs_root: &Path,
    ) -> Result<Outcome, EngineError> {
        // Read "parallel.results" from context.
        let raw = context.get_string("parallel.results");
        if raw.is_empty() {
            return Ok(Outcome::fail("No parallel results to evaluate"));
        }

        // Deserialise.
        let results: Vec<BranchResult> = match serde_json::from_str(&raw) {
            Ok(v) => v,
            Err(e) => {
                return Ok(Outcome::fail(format!(
                    "Failed to parse parallel.results: {e}"
                )));
            }
        };

        if results.is_empty() {
            return Ok(Outcome::fail("Empty parallel results"));
        }

        // Check if all branches failed.
        let all_failed = results.iter().all(|r| r.status == StageStatus::Fail);
        if all_failed {
            return Ok(Outcome::fail("All parallel branches failed"));
        }

        // Pick the best.
        let best = match heuristic_select(&results) {
            Some(b) => b,
            None => return Ok(Outcome::fail("No candidates to select")),
        };

        let mut context_updates = std::collections::HashMap::new();
        context_updates.insert(
            "parallel.fan_in.best_id".to_string(),
            Value::Str(best.branch_id.clone()),
        );
        context_updates.insert(
            "parallel.fan_in.best_status".to_string(),
            Value::Str(best.status.as_str().to_string()),
        );

        Ok(Outcome {
            status: StageStatus::Success,
            context_updates,
            notes: format!("Selected best candidate: {}", best.branch_id),
            ..Default::default()
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{Graph, Node};
    fn make_results(pairs: Vec<(&str, StageStatus)>) -> Vec<BranchResult> {
        pairs
            .into_iter()
            .map(|(id, status)| BranchResult {
                branch_id: id.to_string(),
                status,
                notes: String::new(),
            })
            .collect()
    }

    // --- heuristic_select ---

    #[test]
    fn success_wins_over_partial() {
        let r = make_results(vec![
            ("B", StageStatus::PartialSuccess),
            ("A", StageStatus::Success),
        ]);
        assert_eq!(heuristic_select(&r).unwrap().branch_id, "A");
    }

    #[test]
    fn lexical_tiebreak() {
        let r = make_results(vec![
            ("Z", StageStatus::Success),
            ("A", StageStatus::Success),
        ]);
        assert_eq!(heuristic_select(&r).unwrap().branch_id, "A");
    }

    #[test]
    fn empty_returns_none() {
        assert!(heuristic_select(&[]).is_none());
    }

    #[test]
    fn fail_is_worst() {
        let r = make_results(vec![
            ("fail", StageStatus::Fail),
            ("retry", StageStatus::Retry),
        ]);
        assert_eq!(heuristic_select(&r).unwrap().branch_id, "retry");
    }

    // --- FanInHandler ---

    fn make_context_with_results(results: &[BranchResult]) -> Context {
        let ctx = Context::new();
        let json = serde_json::to_string(results).unwrap();
        ctx.set("parallel.results", Value::Str(json));
        ctx
    }

    fn make_simple_node() -> Node {
        let mut n = Node::default();
        n.id = "fan_in".to_string();
        n
    }

    #[tokio::test]
    async fn selects_best_and_records_context() {
        let handler = FanInHandler::new();
        let dir = tempfile::tempdir().unwrap();
        let results = make_results(vec![("B", StageStatus::Fail), ("A", StageStatus::Success)]);
        let ctx = make_context_with_results(&results);
        let g = Graph::new("test".into());
        let node = make_simple_node();
        let out = handler.execute(&node, &ctx, &g, dir.path()).await.unwrap();

        assert_eq!(out.status, StageStatus::Success);
        assert_eq!(
            out.context_updates.get("parallel.fan_in.best_id"),
            Some(&Value::Str("A".to_string()))
        );
    }

    #[tokio::test]
    async fn all_failed_returns_fail() {
        let handler = FanInHandler::new();
        let dir = tempfile::tempdir().unwrap();
        let results = make_results(vec![("A", StageStatus::Fail), ("B", StageStatus::Fail)]);
        let ctx = make_context_with_results(&results);
        let g = Graph::new("test".into());
        let node = make_simple_node();
        let out = handler.execute(&node, &ctx, &g, dir.path()).await.unwrap();
        assert_eq!(out.status, StageStatus::Fail);
    }

    #[tokio::test]
    async fn missing_results_returns_fail() {
        let handler = FanInHandler::new();
        let dir = tempfile::tempdir().unwrap();
        let ctx = Context::new(); // no parallel.results key
        let g = Graph::new("test".into());
        let node = make_simple_node();
        let out = handler.execute(&node, &ctx, &g, dir.path()).await.unwrap();
        assert_eq!(out.status, StageStatus::Fail);
    }

    #[tokio::test]
    async fn malformed_json_returns_fail() {
        let handler = FanInHandler::new();
        let dir = tempfile::tempdir().unwrap();
        let ctx = Context::new();
        ctx.set("parallel.results", Value::Str("not valid json".to_string()));
        let g = Graph::new("test".into());
        let node = make_simple_node();
        let out = handler.execute(&node, &ctx, &g, dir.path()).await.unwrap();
        assert_eq!(out.status, StageStatus::Fail);
    }
}
