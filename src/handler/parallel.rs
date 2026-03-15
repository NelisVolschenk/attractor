//! Parallel fan-out handler for `shape=component` / `type="parallel"` nodes.
//!
//! Fans out to all outgoing edges concurrently using [`tokio::task::JoinSet`].
//! Each branch receives an isolated context clone; results are collected and
//! stored in `context["parallel.results"]` for a downstream fan-in node.

use crate::error::EngineError;
use crate::events::PipelineEvent;
use crate::graph::{Graph, Node, Value};
use crate::handler::Handler;
use crate::handler::fan_in::BranchResult;
use crate::state::context::{Context, Outcome, StageStatus};
use async_trait::async_trait;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::broadcast;

// ---------------------------------------------------------------------------
// Join and error policies
// ---------------------------------------------------------------------------

/// Policy for determining when fan-out is considered complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinPolicy {
    /// Wait for ALL branches. Default.
    WaitAll,
    /// Satisfied after the first successful branch.
    FirstSuccess,
    /// At least K of N branches must succeed.
    KOfN(usize),
}

impl JoinPolicy {
    pub fn parse(s: &str) -> Self {
        if let Some(rest) = s.strip_prefix("k_of_n:") {
            if let Ok(k) = rest.parse::<usize>() {
                return JoinPolicy::KOfN(k);
            }
        }
        match s {
            "first_success" => JoinPolicy::FirstSuccess,
            _ => JoinPolicy::WaitAll,
        }
    }
}

/// Policy for handling branch failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorPolicy {
    /// Continue remaining branches on failure. Default.
    Continue,
    /// Cancel all remaining branches on first failure.
    FailFast,
    /// Ignore failures; use only successful results.
    Ignore,
}

impl ErrorPolicy {
    pub fn parse(s: &str) -> Self {
        match s {
            "fail_fast" => ErrorPolicy::FailFast,
            "ignore" => ErrorPolicy::Ignore,
            _ => ErrorPolicy::Continue,
        }
    }
}

// ---------------------------------------------------------------------------
// ParallelHandler
// ---------------------------------------------------------------------------

/// Handler for `shape=component` (`type="parallel"`) nodes.
///
/// Executes the DIRECT target node of each outgoing branch edge concurrently.
/// Results are stored in `context["parallel.results"]` as a JSON string.
pub struct ParallelHandler {
    registry: Arc<crate::handler::HandlerRegistry>,
    event_tx: broadcast::Sender<PipelineEvent>,
}

impl ParallelHandler {
    pub fn new(
        registry: Arc<crate::handler::HandlerRegistry>,
        event_tx: broadcast::Sender<PipelineEvent>,
    ) -> Self {
        ParallelHandler { registry, event_tx }
    }
}

#[async_trait]
impl Handler for ParallelHandler {
    async fn execute(
        &self,
        node: &Node,
        context: &Context,
        graph: &Graph,
        logs_root: &Path,
    ) -> Result<Outcome, EngineError> {
        let branches = graph.outgoing_edges(&node.id);
        if branches.is_empty() {
            return Ok(Outcome::fail("No branches to execute"));
        }

        // Read configuration from node extra attrs.
        let join_policy = node
            .extra
            .get("join_policy")
            .and_then(|v| v.as_str())
            .map(JoinPolicy::parse)
            .unwrap_or(JoinPolicy::WaitAll);
        let error_policy = node
            .extra
            .get("error_policy")
            .and_then(|v| v.as_str())
            .map(ErrorPolicy::parse)
            .unwrap_or(ErrorPolicy::Continue);
        let max_parallel: usize = node
            .extra
            .get("max_parallel")
            .and_then(|v| {
                if let Value::Int(n) = v {
                    Some(*n as usize)
                } else {
                    None
                }
            })
            .unwrap_or(4)
            .max(1);

        let branch_count = branches.len();
        let _ = self
            .event_tx
            .send(PipelineEvent::ParallelStarted { branch_count });

        let parallel_start = Instant::now();

        // Build a list of (branch_id, branch_node) to spawn.
        let mut branch_nodes: Vec<(String, Node)> = Vec::new();
        for (i, edge) in branches.iter().enumerate() {
            let branch_id = edge.to.clone();
            let branch_node = match graph.node(&branch_id) {
                Some(n) => n.clone(),
                None => {
                    return Ok(Outcome::fail(format!(
                        "Branch target node '{branch_id}' not found in graph"
                    )));
                }
            };
            let _ = self.event_tx.send(PipelineEvent::ParallelBranchStarted {
                branch: branch_id.clone(),
                index: i,
            });
            branch_nodes.push((branch_id, branch_node));
        }

        // Execute branches using a semaphore-bounded JoinSet.
        let semaphore = Arc::new(tokio::sync::Semaphore::new(max_parallel));
        let mut join_set: tokio::task::JoinSet<BranchResult> = tokio::task::JoinSet::new();

        let registry = Arc::clone(&self.registry);
        let graph_clone = graph.clone();
        let logs_root_buf = logs_root.to_path_buf();

        for (branch_id, branch_node) in branch_nodes {
            let sem = Arc::clone(&semaphore);
            let reg = Arc::clone(&registry);
            let g = graph_clone.clone();
            let lr = logs_root_buf.clone();
            let branch_ctx = context.clone_isolated();
            let bid = branch_id.clone();

            join_set.spawn(async move {
                let _permit = sem.acquire_owned().await.expect("semaphore closed");
                let branch_start = Instant::now();
                let handler = reg.resolve(&branch_node);
                let out = handler
                    .execute(&branch_node, &branch_ctx, &g, &lr)
                    .await
                    .unwrap_or_else(|e| Outcome::fail(e.to_string()));
                let _duration = branch_start.elapsed();
                BranchResult {
                    branch_id: bid,
                    status: out.status,
                    notes: out.notes,
                }
            });
        }

        // Collect results.
        let mut results: Vec<BranchResult> = Vec::with_capacity(branch_count);
        let mut should_abort = false;

        while let Some(res) = join_set.join_next().await {
            match res {
                Ok(branch_result) => {
                    // Emit branch completed event.
                    let _ = self.event_tx.send(PipelineEvent::ParallelBranchCompleted {
                        branch: branch_result.branch_id.clone(),
                        index: results.len(),
                        duration: std::time::Duration::ZERO,
                        success: branch_result.status.is_success(),
                    });

                    if error_policy == ErrorPolicy::FailFast
                        && branch_result.status == StageStatus::Fail
                        && !should_abort
                    {
                        should_abort = true;
                        join_set.abort_all();
                    }
                    results.push(branch_result);
                }
                Err(_join_err) => {
                    // Task aborted or panicked — record as fail.
                    results.push(BranchResult {
                        branch_id: "unknown".to_string(),
                        status: StageStatus::Fail,
                        notes: "branch task aborted".to_string(),
                    });
                }
            }
        }

        let duration = parallel_start.elapsed();
        let success_count = results.iter().filter(|r| r.status.is_success()).count();
        let failure_count = results
            .iter()
            .filter(|r| r.status == StageStatus::Fail)
            .count();

        let _ = self.event_tx.send(PipelineEvent::ParallelCompleted {
            duration,
            success_count,
            failure_count,
        });

        // Store results in context for fan-in.
        let results_json = serde_json::to_string(&results).unwrap_or_else(|_| "[]".to_string());
        context.set("parallel.results", Value::Str(results_json));

        // Determine overall outcome.
        let final_status = match join_policy {
            JoinPolicy::WaitAll => {
                if failure_count == 0 {
                    StageStatus::Success
                } else {
                    StageStatus::PartialSuccess
                }
            }
            JoinPolicy::FirstSuccess => {
                if success_count > 0 {
                    StageStatus::Success
                } else {
                    StageStatus::Fail
                }
            }
            JoinPolicy::KOfN(k) => {
                if success_count >= k {
                    StageStatus::Success
                } else {
                    StageStatus::Fail
                }
            }
        };

        Ok(Outcome {
            status: final_status,
            notes: format!(
                "Parallel: {success_count} succeeded, {failure_count} failed of {branch_count}"
            ),
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
    use crate::graph::{Edge, Graph, GraphAttrs, Node};
    use crate::handler::{CodergenHandler, HandlerRegistry, StartHandler};
    use crate::testing::MockCodergenBackend;
    use std::sync::Arc;

    fn make_parallel_graph(branch_ids: Vec<&str>) -> (Graph, String) {
        let mut g = Graph::new("test".into());
        g.graph_attrs = GraphAttrs {
            default_max_retry: 0,
            ..Default::default()
        };

        let parallel_id = "parallel_node".to_string();
        let mut par = Node::default();
        par.id = parallel_id.clone();
        par.shape = "component".to_string();
        g.nodes.insert(parallel_id.clone(), par);

        for bid in &branch_ids {
            let mut n = Node::default();
            n.id = bid.to_string();
            n.shape = "box".to_string();
            g.nodes.insert(bid.to_string(), n);

            g.edges.push(Edge {
                from: parallel_id.clone(),
                to: bid.to_string(),
                ..Default::default()
            });
        }

        (g, parallel_id)
    }

    fn make_registry_with_mock(mock: Arc<MockCodergenBackend>) -> Arc<HandlerRegistry> {
        let codergen = CodergenHandler::new(Some(Box::new(MockProxyBackend(mock))));
        let default = Arc::new(codergen);
        let mut reg = HandlerRegistry::new(Arc::new(StartHandler));
        reg.register("codergen", default.clone());
        Arc::new(reg)
    }

    // Proxy to allow Arc<MockCodergenBackend>
    struct MockProxyBackend(Arc<MockCodergenBackend>);

    #[async_trait::async_trait]
    impl crate::handler::CodergenBackend for MockProxyBackend {
        async fn run(
            &self,
            node: &Node,
            prompt: &str,
            ctx: &Context,
        ) -> Result<crate::handler::CodergenResult, EngineError> {
            self.0.run(node, prompt, ctx).await
        }
    }

    #[tokio::test]
    async fn all_success_returns_success() {
        let (tx, _rx) = broadcast::channel(64);
        let (graph, node_id) = make_parallel_graph(vec!["A", "B"]);
        let mock = Arc::new(MockCodergenBackend::new());
        let registry = make_registry_with_mock(mock);
        let handler = ParallelHandler::new(registry, tx);

        let dir = tempfile::tempdir().unwrap();
        let ctx = Context::new();
        let node = graph.node(&node_id).unwrap().clone();
        let out = handler
            .execute(&node, &ctx, &graph, dir.path())
            .await
            .unwrap();
        assert_eq!(out.status, StageStatus::Success);
    }

    #[tokio::test]
    async fn no_branches_returns_fail() {
        let (tx, _rx) = broadcast::channel(64);
        let mut g = Graph::new("test".into());
        let mut n = Node::default();
        n.id = "p".to_string();
        g.nodes.insert("p".to_string(), n.clone());

        let default_handler = Arc::new(StartHandler);
        let reg = Arc::new(HandlerRegistry::new(default_handler));
        let handler = ParallelHandler::new(reg, tx);
        let dir = tempfile::tempdir().unwrap();
        let ctx = Context::new();
        let out = handler.execute(&n, &ctx, &g, dir.path()).await.unwrap();
        assert_eq!(out.status, StageStatus::Fail);
    }

    #[tokio::test]
    async fn results_stored_in_context() {
        let (tx, _rx) = broadcast::channel(64);
        let (graph, node_id) = make_parallel_graph(vec!["A", "B"]);
        let mock = Arc::new(MockCodergenBackend::new());
        let registry = make_registry_with_mock(mock);
        let handler = ParallelHandler::new(registry, tx);

        let dir = tempfile::tempdir().unwrap();
        let ctx = Context::new();
        let node = graph.node(&node_id).unwrap().clone();
        handler
            .execute(&node, &ctx, &graph, dir.path())
            .await
            .unwrap();

        let results_str = ctx.get_string("parallel.results");
        assert!(!results_str.is_empty());
        let results: Vec<BranchResult> = serde_json::from_str(&results_str).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn first_success_policy() {
        let (tx, _rx) = broadcast::channel(64);
        let (mut graph, node_id) = make_parallel_graph(vec!["A", "B"]);
        // Add join_policy extra attr
        graph.nodes.get_mut(&node_id).unwrap().extra.insert(
            "join_policy".to_string(),
            Value::Str("first_success".to_string()),
        );
        let mock = Arc::new(MockCodergenBackend::new());
        let registry = make_registry_with_mock(mock);
        let handler = ParallelHandler::new(registry, tx);
        let dir = tempfile::tempdir().unwrap();
        let ctx = Context::new();
        let node = graph.node(&node_id).unwrap().clone();
        let out = handler
            .execute(&node, &ctx, &graph, dir.path())
            .await
            .unwrap();
        assert_eq!(out.status, StageStatus::Success);
    }
}
