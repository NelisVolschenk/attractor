//! Core pipeline execution engine (NLSpec §3).
//!
//! [`PipelineRunner`] orchestrates the full pipeline lifecycle:
//!
//! ```text
//! PARSE → TRANSFORM → VALIDATE → INITIALIZE → EXECUTE → FINALIZE
//! ```
//!
//! Use [`PipelineRunnerBuilder`] to configure and create a runner, then call
//! [`PipelineRunner::run`] or [`PipelineRunner::resume`].

pub mod edge_selection;
pub mod goal_gate;
pub mod retry;

pub use edge_selection::{normalize_label, select_edge};
pub use goal_gate::{check_goal_gates, resolve_gate_retry_target};
pub use retry::{BackoffConfig, RetryPolicy, execute_with_retry};

use crate::error::EngineError;
use crate::events::{EVENT_CHANNEL_CAPACITY, PipelineEvent};
use crate::graph::{Graph, Value};
use crate::handler::{
    CodergenHandler, ConditionalHandler, ExitHandler, FanInHandler, Handler, HandlerRegistry,
    ParallelHandler, StartHandler, ToolHandler, WaitForHumanHandler,
};
use crate::interviewer::{AutoApproveInterviewer, Interviewer};
use crate::parser::parse_dot;
use crate::state::checkpoint::Checkpoint;
use crate::state::context::{Context, Outcome, StageStatus};
use crate::transform::{
    StylesheetApplicationTransform, Transform, VariableExpansionTransform, apply_transforms,
};
use crate::validation::validate_or_raise;
use chrono::Utc;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::broadcast;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// RunConfig
// ---------------------------------------------------------------------------

/// Configuration for a single pipeline execution.
#[derive(Debug, Clone)]
pub struct RunConfig {
    /// Directory for logs, checkpoints, and artifacts.
    pub logs_root: PathBuf,
}

impl RunConfig {
    pub fn new(logs_root: impl Into<PathBuf>) -> Self {
        RunConfig {
            logs_root: logs_root.into(),
        }
    }
}

// ---------------------------------------------------------------------------
// RunResult
// ---------------------------------------------------------------------------

/// The result returned by [`PipelineRunner::run`] or [`PipelineRunner::resume`].
#[derive(Debug)]
pub struct RunResult {
    /// Final pipeline status.
    pub status: StageStatus,
    /// Node IDs in completion order.
    pub completed_nodes: Vec<String>,
    /// Final context state.
    pub context: Context,
}

// ---------------------------------------------------------------------------
// PipelineRunner
// ---------------------------------------------------------------------------

/// The execution engine for DOT-based pipelines.
///
/// Create via [`PipelineRunnerBuilder`]:
/// ```ignore
/// let (runner, events) = PipelineRunner::builder().build();
/// ```
pub struct PipelineRunner {
    pub(crate) registry: Arc<HandlerRegistry>,
    pub(crate) transforms: Vec<Box<dyn Transform>>,
    pub(crate) event_tx: broadcast::Sender<PipelineEvent>,
}

impl PipelineRunner {
    /// Return a new builder.
    pub fn builder() -> PipelineRunnerBuilder {
        PipelineRunnerBuilder::new()
    }

    /// Subscribe to pipeline events.
    pub fn events(&self) -> broadcast::Receiver<PipelineEvent> {
        self.event_tx.subscribe()
    }

    /// Parse, transform, validate, and execute a pipeline from DOT source.
    pub async fn run(&self, dot_source: &str, config: RunConfig) -> Result<RunResult, EngineError> {
        let (graph, context) = self.prepare(dot_source, &config.logs_root).await?;
        let node_outcomes: HashMap<String, Outcome> = HashMap::new();
        let node_retries: HashMap<String, u32> = HashMap::new();
        let completed_nodes: Vec<String> = vec![];

        let start_node_id = graph
            .start_node()
            .ok_or(EngineError::NoStartNode)?
            .id
            .clone();

        let run_id = Uuid::new_v4().to_string();
        let _ = self.event_tx.send(PipelineEvent::PipelineStarted {
            name: graph.name.clone(),
            id: run_id,
        });

        self.execute_loop(
            graph,
            context,
            start_node_id,
            completed_nodes,
            node_outcomes,
            node_retries,
            &config,
        )
        .await
    }

    /// Load a checkpoint and resume execution from where it left off.
    pub async fn resume(
        &self,
        dot_source: &str,
        config: RunConfig,
    ) -> Result<RunResult, EngineError> {
        let (graph, context) = self.prepare(dot_source, &config.logs_root).await?;

        // Load checkpoint.
        let cp_path = Checkpoint::default_path(&config.logs_root);
        let cp = Checkpoint::load(&cp_path)?;

        // Restore context.
        context.apply_updates(&cp.context_values);
        for entry in &cp.logs {
            context.append_log(entry);
        }

        let completed_nodes = cp.completed_nodes.clone();
        let node_retries = cp.node_retries.clone();

        // Restore actual node outcomes from checkpoint (V2-ATR-005).
        // Fall back to Outcome::success() only for nodes not recorded in the
        // checkpoint (e.g., checkpoints saved before this field was added).
        let mut node_outcomes: HashMap<String, Outcome> = HashMap::new();
        for node_id in &completed_nodes {
            let outcome = cp
                .node_outcomes
                .get(node_id)
                .cloned()
                .unwrap_or_else(Outcome::success);
            node_outcomes.insert(node_id.clone(), outcome);
        }

        // Determine next node: find edge from the last completed node.
        let next_node_id = if cp.current_node.is_empty() {
            graph
                .start_node()
                .ok_or(EngineError::NoStartNode)?
                .id
                .clone()
        } else {
            // Select the next edge from the last completed node using restored context.
            let synthetic_outcome = Outcome {
                status: context
                    .get_string("outcome")
                    .parse_stage_status()
                    .unwrap_or(StageStatus::Success),
                preferred_label: context.get_string("preferred_label"),
                ..Default::default()
            };
            match select_edge(&cp.current_node, &synthetic_outcome, &context, &graph) {
                Some(e) => e.to.clone(),
                None => {
                    // Already at terminal or no edge — re-check from current node
                    cp.current_node.clone()
                }
            }
        };

        let run_id = Uuid::new_v4().to_string();
        let _ = self.event_tx.send(PipelineEvent::PipelineStarted {
            name: graph.name.clone(),
            id: run_id,
        });

        self.execute_loop(
            graph,
            context,
            next_node_id,
            completed_nodes,
            node_outcomes,
            node_retries,
            &config,
        )
        .await
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Parse, transform, validate; create the run directory and context.
    async fn prepare(
        &self,
        dot_source: &str,
        logs_root: &Path,
    ) -> Result<(Graph, Context), EngineError> {
        // Parse.
        let graph = parse_dot(dot_source)?;

        // Apply transforms.
        let graph = apply_transforms(graph, &self.transforms);

        // Validate.
        validate_or_raise(&graph, &[])?;

        // Create logs directory.
        tokio::fs::create_dir_all(logs_root).await?;

        // Initialise context.
        let context = Context::new();
        context.set("graph.goal", Value::Str(graph.graph_attrs.goal.clone()));
        context.set("graph.name", Value::Str(graph.name.clone()));

        Ok((graph, context))
    }

    /// The main execution loop — shared between `run` and `resume`.
    #[allow(clippy::too_many_arguments)]
    #[allow(unused_assignments)]
    async fn execute_loop(
        &self,
        graph: Graph,
        context: Context,
        mut current_node_id: String,
        mut completed_nodes: Vec<String>,
        mut node_outcomes: HashMap<String, Outcome>,
        mut node_retries: HashMap<String, u32>,
        config: &RunConfig,
    ) -> Result<RunResult, EngineError> {
        let pipeline_start = Instant::now();
        let mut last_outcome = Outcome::success();

        loop {
            // Fetch the current node.
            let node = match graph.node(&current_node_id) {
                Some(n) => n.clone(),
                None => {
                    return Err(EngineError::Handler {
                        node_id: current_node_id.clone(),
                        message: "node not found in graph".to_string(),
                    });
                }
            };

            // ----------------------------------------------------------------
            // Step 1: Terminal node check
            // ----------------------------------------------------------------
            if node.shape == "Msquare" {
                // Execute the exit handler (no-op) for logging purposes.
                let handler = self.registry.resolve(&node);
                let exit_outcome = handler
                    .execute(&node, &context, &graph, &config.logs_root)
                    .await
                    .unwrap_or_else(|e| Outcome::fail(e.to_string()));

                // GAP-ATR-006/009: Engine ensures status.json is written for ALL nodes.
                let _ = write_node_status_json(&config.logs_root, &current_node_id, &exit_outcome)
                    .await;

                completed_nodes.push(current_node_id.clone());
                node_outcomes.insert(current_node_id.clone(), exit_outcome.clone());
                last_outcome = exit_outcome;

                // Goal gate check.
                match check_goal_gates(&graph, &node_outcomes) {
                    Ok(()) => break, // Clean pipeline exit.
                    Err(gate_id) => match resolve_gate_retry_target(&gate_id, &graph) {
                        Some(target) => {
                            current_node_id = target.to_string();
                            continue;
                        }
                        None => {
                            let duration = pipeline_start.elapsed();
                            let _ = self.event_tx.send(PipelineEvent::PipelineFailed {
                                error: format!("Goal gate unsatisfied: {gate_id}"),
                                duration,
                            });
                            return Err(EngineError::GoalGateUnsatisfied(gate_id));
                        }
                    },
                }
            }

            // ----------------------------------------------------------------
            // Step 2: Execute handler with retry
            // ----------------------------------------------------------------
            let handler = self.registry.resolve(&node);
            let policy = RetryPolicy::from_node(&node, &graph);
            let stage_index = completed_nodes.len();

            let _ = self.event_tx.send(PipelineEvent::StageStarted {
                name: node.id.clone(),
                index: stage_index,
            });
            let stage_start = Instant::now();

            context.set("current_node", Value::Str(current_node_id.clone()));

            let outcome = execute_with_retry(
                handler,
                &node,
                &context,
                &graph,
                &config.logs_root,
                &policy,
                &mut node_retries,
                &self.event_tx,
            )
            .await;

            let stage_duration = stage_start.elapsed();
            if outcome.status.is_success() {
                let _ = self.event_tx.send(PipelineEvent::StageCompleted {
                    name: node.id.clone(),
                    index: stage_index,
                    duration: stage_duration,
                });
            }

            last_outcome = outcome.clone();

            // GAP-ATR-006/009: Engine ensures status.json is written for ALL nodes.
            let _ = write_node_status_json(&config.logs_root, &current_node_id, &outcome).await;

            // ----------------------------------------------------------------
            // Step 3: Record completion
            // ----------------------------------------------------------------
            completed_nodes.push(current_node_id.clone());
            node_outcomes.insert(current_node_id.clone(), outcome.clone());
            context.append_log(&format!(
                "[{}] {} → {}",
                completed_nodes.len(),
                node.id,
                outcome.status
            ));

            // ----------------------------------------------------------------
            // Step 4: Apply context updates
            // ----------------------------------------------------------------
            context.apply_updates(&outcome.context_updates);
            context.set("outcome", Value::Str(outcome.status.as_str().to_string()));
            if !outcome.preferred_label.is_empty() {
                context.set(
                    "preferred_label",
                    Value::Str(outcome.preferred_label.clone()),
                );
            }

            // ----------------------------------------------------------------
            // Step 5: Save checkpoint
            // ----------------------------------------------------------------
            let cp = build_checkpoint(
                &context,
                &current_node_id,
                &completed_nodes,
                &node_retries,
                &node_outcomes,
            );
            let cp_path = Checkpoint::default_path(&config.logs_root);
            cp.save(&cp_path)?;
            let _ = self.event_tx.send(PipelineEvent::CheckpointSaved {
                node_id: current_node_id.clone(),
            });

            // ----------------------------------------------------------------
            // Step 6: Select next edge
            // ----------------------------------------------------------------
            let next_edge = select_edge(&current_node_id, &outcome, &context, &graph);

            if next_edge.is_none() {
                if outcome.status == StageStatus::Fail {
                    // Try node-level retry targets.
                    if !node.retry_target.is_empty() && graph.node(&node.retry_target).is_some() {
                        current_node_id = node.retry_target.clone();
                        continue;
                    }
                    if !node.fallback_retry_target.is_empty()
                        && graph.node(&node.fallback_retry_target).is_some()
                    {
                        current_node_id = node.fallback_retry_target.clone();
                        continue;
                    }
                    let duration = pipeline_start.elapsed();
                    let _ = self.event_tx.send(PipelineEvent::PipelineFailed {
                        error: format!("No fail edge from '{current_node_id}'"),
                        duration,
                    });
                    return Err(EngineError::NoFailEdge(current_node_id));
                }
                // No edges and not failing → done.
                break;
            }

            let edge = next_edge.unwrap();

            // ----------------------------------------------------------------
            // Step 7: Handle loop_restart
            // ----------------------------------------------------------------
            if edge.loop_restart {
                context.set("loop_restart_target", Value::Str(edge.to.clone()));
                break;
            }

            // ----------------------------------------------------------------
            // Step 8: Advance
            // ----------------------------------------------------------------
            current_node_id = edge.to.clone();
        }

        let duration = pipeline_start.elapsed();
        let final_status = last_outcome.status;

        if final_status.is_success() || final_status == StageStatus::Skipped {
            let _ = self.event_tx.send(PipelineEvent::PipelineCompleted {
                duration,
                artifact_count: 0,
            });
        } else {
            let _ = self.event_tx.send(PipelineEvent::PipelineFailed {
                error: last_outcome.failure_reason.clone(),
                duration,
            });
        }

        Ok(RunResult {
            status: final_status,
            completed_nodes,
            context,
        })
    }
}

// ---------------------------------------------------------------------------
// PipelineRunnerBuilder
// ---------------------------------------------------------------------------

/// Builder for [`PipelineRunner`].
pub struct PipelineRunnerBuilder {
    registry: HandlerRegistry,
    transforms: Vec<Box<dyn Transform>>,
    interviewer: Arc<dyn Interviewer>,
}

impl PipelineRunnerBuilder {
    /// Create a new builder with default configuration.
    ///
    /// Default registry:
    /// - `"start"` → `StartHandler`
    /// - `"exit"` → `ExitHandler`
    /// - `"conditional"` → `ConditionalHandler`
    /// - `"wait.human"` → `WaitForHumanHandler` (with `AutoApproveInterviewer`)
    /// - `"tool"` → `ToolHandler`
    /// - `"codergen"` (default) → `CodergenHandler` (simulation mode)
    ///
    /// Default transforms: `[VariableExpansionTransform, StylesheetApplicationTransform]`.
    pub fn new() -> Self {
        let interviewer: Arc<dyn Interviewer> = Arc::new(AutoApproveInterviewer);
        let codergen_handler = Arc::new(CodergenHandler::new(None));
        let mut registry = HandlerRegistry::new(codergen_handler.clone());
        registry.register("start", Arc::new(StartHandler));
        registry.register("exit", Arc::new(ExitHandler));
        registry.register("conditional", Arc::new(ConditionalHandler));
        registry.register(
            "wait.human",
            Arc::new(WaitForHumanHandler::new(interviewer.clone())),
        );
        registry.register("tool", Arc::new(ToolHandler));
        registry.register("codergen", codergen_handler);

        PipelineRunnerBuilder {
            registry,
            transforms: vec![
                Box::new(VariableExpansionTransform),
                Box::new(StylesheetApplicationTransform),
            ],
            interviewer,
        }
    }

    /// Register a handler for `type_str`.
    pub fn with_handler(mut self, type_str: &str, handler: Arc<dyn Handler>) -> Self {
        self.registry.register(type_str, handler);
        self
    }

    /// Append a transform (runs after built-in transforms).
    pub fn with_transform(mut self, t: Box<dyn Transform>) -> Self {
        self.transforms.push(t);
        self
    }

    /// Set the interviewer for `wait.human` nodes.
    pub fn with_interviewer(mut self, iv: Arc<dyn Interviewer>) -> Self {
        self.interviewer = iv.clone();
        // Re-register wait.human with the new interviewer.
        self.registry
            .register("wait.human", Arc::new(WaitForHumanHandler::new(iv)));
        self
    }

    /// Build the runner and return an event receiver.
    ///
    /// Registers `ParallelHandler` and `FanInHandler` automatically.
    /// `ParallelHandler` receives a clone of the registry (without parallel
    /// itself, since nested parallelism is not supported) so it can execute
    /// branch nodes.
    pub fn build(self) -> (PipelineRunner, broadcast::Receiver<PipelineEvent>) {
        let (tx, rx) = broadcast::channel(EVENT_CHANNEL_CAPACITY);

        // Clone the registry for use inside ParallelHandler (branch execution).
        // This snapshot does NOT include the ParallelHandler itself — nested
        // parallel is not supported and branch nodes are expected to be leaf
        // handler types (codergen, tool, wait.human, …).
        let branch_registry = Arc::new(self.registry.clone());
        let parallel_handler = Arc::new(ParallelHandler::new(branch_registry, tx.clone()));
        let fan_in_handler = Arc::new(FanInHandler::new());

        let mut registry = self.registry;
        registry.register("parallel", parallel_handler);
        registry.register("parallel.fan_in", fan_in_handler);

        let runner = PipelineRunner {
            registry: Arc::new(registry),
            transforms: self.transforms,
            event_tx: tx,
        };
        (runner, rx)
    }
}

impl Default for PipelineRunnerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Helper: write_node_status_json
// ---------------------------------------------------------------------------

/// Write `{logs_root}/{node_id}/status.json` for every executed node.
///
/// Handlers such as `CodergenHandler` write their own `status.json`; calling
/// this afterwards is idempotent — the engine's outcome is the same value.
/// For handlers that don't write the file (e.g. `StartHandler`, `ExitHandler`)
/// this is the only write that occurs, satisfying NLSpec §11.3 / §11.6.
async fn write_node_status_json(
    logs_root: &Path,
    node_id: &str,
    outcome: &Outcome,
) -> Result<(), EngineError> {
    let stage_dir = logs_root.join(node_id);
    tokio::fs::create_dir_all(&stage_dir).await?;
    let json = serde_json::to_string_pretty(outcome).map_err(|e| EngineError::Handler {
        node_id: node_id.to_string(),
        message: format!("failed to serialise outcome: {e}"),
    })?;
    tokio::fs::write(stage_dir.join("status.json"), json).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Helper: build_checkpoint
// ---------------------------------------------------------------------------

fn build_checkpoint(
    context: &Context,
    current_node: &str,
    completed: &[String],
    retries: &HashMap<String, u32>,
    outcomes: &HashMap<String, Outcome>,
) -> Checkpoint {
    Checkpoint {
        timestamp: Utc::now(),
        current_node: current_node.to_string(),
        completed_nodes: completed.to_vec(),
        node_retries: retries.clone(),
        context_values: context.snapshot(),
        logs: context.logs_snapshot(),
        node_outcomes: outcomes.clone(),
    }
}

// ---------------------------------------------------------------------------
// Helper trait: parse stage status from string
// ---------------------------------------------------------------------------

trait ParseStageStatus {
    fn parse_stage_status(self) -> Option<StageStatus>;
}

impl ParseStageStatus for String {
    fn parse_stage_status(self) -> Option<StageStatus> {
        match self.as_str() {
            "success" => Some(StageStatus::Success),
            "fail" => Some(StageStatus::Fail),
            "partial_success" => Some(StageStatus::PartialSuccess),
            "retry" => Some(StageStatus::Retry),
            "skipped" => Some(StageStatus::Skipped),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::CodergenHandler;
    use crate::testing::MockCodergenBackend;
    use std::sync::Arc;

    // ---------------------------------------------------------------------------
    // Test helper: build a simple linear DOT pipeline
    // ---------------------------------------------------------------------------

    fn linear_dot(goal: &str, nodes: Vec<&str>) -> String {
        let mut s = format!("digraph test {{\n  graph [goal=\"{goal}\"]\n");
        s.push_str("  start [shape=Mdiamond]\n");
        s.push_str("  exit  [shape=Msquare]\n");
        for n in &nodes {
            s.push_str(&format!("  {n} [label=\"{n}\" prompt=\"do {n}\"]\n"));
        }
        s.push_str("  start");
        for n in &nodes {
            s.push_str(&format!(" -> {n}"));
        }
        s.push_str(" -> exit\n}\n");
        s
    }

    fn make_runner(
        mock: Arc<MockCodergenBackend>,
    ) -> (PipelineRunner, broadcast::Receiver<PipelineEvent>) {
        let codergen = Arc::new(CodergenHandler::new(Some(Box::new(MockProxy(mock)))));
        PipelineRunner::builder()
            .with_handler("codergen", codergen)
            .build()
    }

    struct MockProxy(Arc<MockCodergenBackend>);

    #[async_trait::async_trait]
    impl crate::handler::CodergenBackend for MockProxy {
        async fn run(
            &self,
            node: &crate::graph::Node,
            prompt: &str,
            ctx: &Context,
        ) -> Result<crate::handler::CodergenResult, EngineError> {
            self.0.run(node, prompt, ctx).await
        }
    }

    // ---------------------------------------------------------------------------
    // Linear pipeline
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn linear_pipeline_runs_all_nodes() {
        let dir = tempfile::tempdir().unwrap();
        let mock = Arc::new(MockCodergenBackend::new());
        let (runner, _rx) = make_runner(mock.clone());
        let dot = linear_dot("test", vec!["plan", "implement", "review"]);
        let result = runner.run(&dot, RunConfig::new(dir.path())).await.unwrap();

        assert_eq!(result.status, StageStatus::Success);
        assert!(result.completed_nodes.contains(&"plan".to_string()));
        assert!(result.completed_nodes.contains(&"implement".to_string()));
        assert!(result.completed_nodes.contains(&"review".to_string()));
    }

    #[tokio::test]
    async fn context_updates_propagate() {
        let dir = tempfile::tempdir().unwrap();
        let mock = Arc::new(MockCodergenBackend::new());
        // Make "plan" return a context update.
        mock.add_response(
            "plan",
            crate::handler::CodergenResult::Outcome(Outcome {
                status: StageStatus::Success,
                context_updates: {
                    let mut m = HashMap::new();
                    m.insert("plan_done".to_string(), Value::Bool(true));
                    m
                },
                ..Default::default()
            }),
        );
        let (runner, _rx) = make_runner(mock);
        let dot = linear_dot("test", vec!["plan", "implement"]);
        let result = runner.run(&dot, RunConfig::new(dir.path())).await.unwrap();

        assert_eq!(result.context.get("plan_done"), Some(Value::Bool(true)));
    }

    #[tokio::test]
    async fn checkpoint_written_after_each_node() {
        let dir = tempfile::tempdir().unwrap();
        let mock = Arc::new(MockCodergenBackend::new());
        let (runner, _rx) = make_runner(mock);
        let dot = linear_dot("test", vec!["plan"]);
        runner.run(&dot, RunConfig::new(dir.path())).await.unwrap();

        let cp_path = Checkpoint::default_path(dir.path());
        assert!(cp_path.exists());
    }

    // ---------------------------------------------------------------------------
    // Conditional branching
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn conditional_branching_success_path() {
        let dir = tempfile::tempdir().unwrap();
        let dot = r#"
digraph test {
    graph [goal="branch test"]
    start [shape=Mdiamond]
    exit  [shape=Msquare]
    run_tests [prompt="run tests"]
    pass_path [prompt="passed"]
    fail_path [prompt="failed"]
    start -> run_tests
    run_tests -> pass_path [condition="outcome=success"]
    run_tests -> fail_path [condition="outcome=fail"]
    pass_path -> exit
    fail_path -> exit
}
"#;
        let mock = Arc::new(MockCodergenBackend::new());
        mock.add_success("run_tests");
        let (runner, _rx) = make_runner(mock.clone());
        let result = runner.run(dot, RunConfig::new(dir.path())).await.unwrap();

        assert_eq!(result.status, StageStatus::Success);
        assert!(result.completed_nodes.contains(&"pass_path".to_string()));
        assert!(!result.completed_nodes.contains(&"fail_path".to_string()));
    }

    #[tokio::test]
    async fn conditional_branching_fail_path() {
        let dir = tempfile::tempdir().unwrap();
        // default_max_retry=0: run_tests has no retry budget so a Fail outcome
        // flows immediately to edge selection (routing to the fail edge).
        // Without default_max_retry=0, V2-ATR-002 would retry run_tests and the
        // mock's default Success would route to pass_path instead.
        let dot = r#"
digraph test {
    graph [goal="branch test", default_max_retry=0]
    start [shape=Mdiamond]
    exit  [shape=Msquare]
    run_tests [prompt="run tests"]
    pass_path [prompt="passed"]
    fail_path [prompt="failed"]
    start -> run_tests
    run_tests -> pass_path [condition="outcome=success"]
    run_tests -> fail_path [condition="outcome=fail"]
    pass_path -> exit
    fail_path -> exit
}
"#;
        let mock = Arc::new(MockCodergenBackend::new());
        mock.add_fail("run_tests", "tests failed");
        let (runner, _rx) = make_runner(mock.clone());
        let result = runner.run(dot, RunConfig::new(dir.path())).await.unwrap();

        assert_eq!(result.status, StageStatus::Success);
        assert!(result.completed_nodes.contains(&"fail_path".to_string()));
        assert!(!result.completed_nodes.contains(&"pass_path".to_string()));
    }

    // ---------------------------------------------------------------------------
    // Retry
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn retry_then_success() {
        let dir = tempfile::tempdir().unwrap();
        let dot = r#"
digraph test {
    start [shape=Mdiamond]
    exit  [shape=Msquare]
    flaky [prompt="flaky node", max_retries=3]
    start -> flaky -> exit
}
"#;
        let mock = Arc::new(MockCodergenBackend::new());
        mock.add_retry("flaky", "not ready");
        mock.add_retry("flaky", "still not ready");
        mock.add_success("flaky");
        let (runner, _rx) = make_runner(mock.clone());
        let result = runner.run(dot, RunConfig::new(dir.path())).await.unwrap();
        assert_eq!(result.status, StageStatus::Success);
        assert_eq!(mock.call_count("flaky"), 3);
    }

    // ---------------------------------------------------------------------------
    // Goal gate
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn goal_gate_blocks_exit_until_satisfied() {
        let dir = tempfile::tempdir().unwrap();
        let dot = r#"
digraph test {
    start [shape=Mdiamond]
    exit  [shape=Msquare]
    critical [prompt="critical", goal_gate=true, max_retries=2]
    retry_point [prompt="retry"]
    graph [retry_target="retry_point"]
    start -> critical
    critical -> exit [condition="outcome=success"]
    critical -> retry_point [condition="outcome=fail"]
    retry_point -> critical
}
"#;
        // critical fails first time → goes to retry_point → back to critical → succeeds
        let mock = Arc::new(MockCodergenBackend::new());
        mock.add_fail("critical", "first try");
        mock.add_success("critical");
        // retry_point always succeeds
        let (runner, _rx) = make_runner(mock.clone());
        let result = runner.run(dot, RunConfig::new(dir.path())).await.unwrap();
        assert_eq!(result.status, StageStatus::Success);
    }

    // GAP-ATR-007: happy-path goal gate (all gates succeed, pipeline exits cleanly)

    #[tokio::test]
    async fn goal_gate_all_succeed_exits_cleanly() {
        // GAP-ATR-007: NLSpec §11.4 — before allowing exit the engine checks all
        // goal_gate nodes have status SUCCESS.  This test verifies the happy path:
        // goal_gate node succeeds on the first try → pipeline exits without any
        // retry_target being used.
        let dir = tempfile::tempdir().unwrap();
        let dot = r#"
digraph test {
    start    [shape=Mdiamond]
    exit     [shape=Msquare]
    critical [prompt="critical task", goal_gate=true]
    start -> critical -> exit
}
"#;
        let mock = Arc::new(MockCodergenBackend::new());
        // critical succeeds immediately — no retry needed
        mock.add_success("critical");
        let (runner, _rx) = make_runner(mock.clone());
        let result = runner.run(dot, RunConfig::new(dir.path())).await.unwrap();

        assert_eq!(result.status, StageStatus::Success);
        assert!(
            result.completed_nodes.contains(&"critical".to_string()),
            "critical (goal_gate) node must be in completed_nodes"
        );
        assert!(
            result.completed_nodes.contains(&"exit".to_string()),
            "pipeline must reach exit when all goal gates pass"
        );
        // Only one call to critical — no retry was needed
        assert_eq!(mock.call_count("critical"), 1);
    }

    // ---------------------------------------------------------------------------
    // Parse error
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn parse_error_propagates() {
        let dir = tempfile::tempdir().unwrap();
        let (runner, _rx) = PipelineRunner::builder().build();
        let result = runner
            .run("this is not valid dot", RunConfig::new(dir.path()))
            .await;
        assert!(result.is_err());
        matches!(result.unwrap_err(), EngineError::Parse(_));
    }

    // ---------------------------------------------------------------------------
    // Validation error
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn validation_error_missing_start() {
        let dir = tempfile::tempdir().unwrap();
        let dot = r#"digraph test { A -> B }"#;
        let (runner, _rx) = PipelineRunner::builder().build();
        let result = runner.run(dot, RunConfig::new(dir.path())).await;
        assert!(result.is_err());
    }

    // ---------------------------------------------------------------------------
    // Variable expansion
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn goal_variable_expanded_in_prompt() {
        let dir = tempfile::tempdir().unwrap();
        let dot = r#"
digraph test {
    graph [goal="build a rocket"]
    start [shape=Mdiamond]
    exit  [shape=Msquare]
    plan  [prompt="Work on: $goal"]
    start -> plan -> exit
}
"#;
        let mock = Arc::new(MockCodergenBackend::new());
        let (runner, _rx) = make_runner(mock.clone());
        runner.run(dot, RunConfig::new(dir.path())).await.unwrap();

        // Check the prompt file was written with expanded variable.
        let prompt_path = dir.path().join("plan").join("prompt.md");
        let prompt_text = std::fs::read_to_string(prompt_path).unwrap();
        assert_eq!(prompt_text, "Work on: build a rocket");
    }

    // ---------------------------------------------------------------------------
    // Event emission
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn events_emitted_for_lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        let mock = Arc::new(MockCodergenBackend::new());
        let (runner, mut rx) = make_runner(mock);
        let dot = linear_dot("test", vec!["plan"]);

        // Run in background, collect events.
        let handle =
            tokio::spawn(
                async move { runner.run(&dot, RunConfig::new(dir.path())).await.unwrap() },
            );

        let mut events = Vec::new();
        // Drain events with a short timeout.
        let timeout = tokio::time::Duration::from_millis(500);
        let _ = tokio::time::timeout(timeout, async {
            loop {
                match rx.recv().await {
                    Ok(ev) => {
                        let done = matches!(
                            ev,
                            PipelineEvent::PipelineCompleted { .. }
                                | PipelineEvent::PipelineFailed { .. }
                        );
                        events.push(ev);
                        if done {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        })
        .await;

        handle.await.unwrap();

        // At minimum: PipelineStarted and a stage event should be present.
        let has_started = events
            .iter()
            .any(|e| matches!(e, PipelineEvent::PipelineStarted { .. }));
        assert!(has_started, "Expected PipelineStarted event");
    }

    // ---------------------------------------------------------------------------
    // Resume
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn resume_continues_from_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let dot = linear_dot("test", vec!["plan", "implement", "review"]);

        // Run only "plan" node, then simulate a crash by saving a checkpoint.
        // We'll do this by running the pipeline normally first to get the checkpoint,
        // then verifying resume doesn't re-run plan.
        let mock1 = Arc::new(MockCodergenBackend::new());
        let (runner1, _rx1) = make_runner(mock1.clone());
        runner1.run(&dot, RunConfig::new(dir.path())).await.unwrap();

        // Manually create a checkpoint at "plan" position.
        let mut context_values = std::collections::HashMap::new();
        context_values.insert("graph.goal".to_string(), Value::Str("test".to_string()));
        context_values.insert("outcome".to_string(), Value::Str("success".to_string()));
        let cp = Checkpoint {
            timestamp: Utc::now(),
            current_node: "plan".to_string(),
            completed_nodes: vec!["start".to_string(), "plan".to_string()],
            node_retries: HashMap::new(),
            context_values,
            logs: Vec::new(),
            node_outcomes: HashMap::new(),
        };
        let cp_path = Checkpoint::default_path(dir.path());
        cp.save(&cp_path).unwrap();

        // Resume should pick up from after "plan" → "implement" then "review" → "exit".
        let mock2 = Arc::new(MockCodergenBackend::new());
        let (runner2, _rx2) = make_runner(mock2.clone());
        let result = runner2
            .resume(&dot, RunConfig::new(dir.path()))
            .await
            .unwrap();

        assert_eq!(result.status, StageStatus::Success);
        // plan should NOT be re-executed (already in checkpoint).
        assert_eq!(mock2.call_count("plan"), 0);
        // implement and review should run.
        assert!(mock2.call_count("implement") > 0 || mock2.call_count("review") > 0);
    }

    #[tokio::test]
    async fn resume_restores_goal_gate_outcome_from_checkpoint() {
        // V2-ATR-005: On resume, goal-gate node outcomes must be restored from the
        // checkpoint rather than being synthesised as Outcome::success().
        //
        // Scenario:
        //   Pipeline: start → critical [goal_gate=true] → middle → exit
        //   Graph retry_target = "critical" (so gate failure re-runs critical)
        //   Checkpoint state: critical completed with Fail outcome.
        //
        //   Without fix: resume synthesises Success for critical → goal gate passes
        //                → pipeline exits; critical is NOT re-executed (call_count=0).
        //   With fix:    resume restores Fail for critical → goal gate fires →
        //                routes to retry_target ("critical") → critical re-runs once
        //                (mock returns Success) → gate passes → exit (call_count=1).
        let dir = tempfile::tempdir().unwrap();
        let dot = r#"
digraph test {
    graph [goal="critical test", retry_target="critical"]
    start    [shape=Mdiamond]
    exit     [shape=Msquare]
    critical [goal_gate=true, prompt="critical work"]
    middle   [prompt="middle work"]
    start -> critical -> middle -> exit
}
"#;

        // Mock: critical returns Success when called (for the goal-gate retry).
        let mock = Arc::new(MockCodergenBackend::new());
        mock.add_success("critical");
        let (runner, _rx) = make_runner(mock.clone());

        // Create a checkpoint simulating: start + critical completed, critical had Fail.
        let mut context_values = HashMap::new();
        context_values.insert(
            "graph.goal".to_string(),
            Value::Str("critical test".to_string()),
        );
        context_values.insert("outcome".to_string(), Value::Str("success".to_string()));

        let mut node_outcomes_map = HashMap::new();
        node_outcomes_map.insert("start".to_string(), Outcome::success());
        node_outcomes_map.insert(
            "critical".to_string(),
            Outcome::fail("first attempt failed"),
        );

        let cp = Checkpoint {
            timestamp: Utc::now(),
            current_node: "critical".to_string(),
            completed_nodes: vec!["start".to_string(), "critical".to_string()],
            node_retries: HashMap::new(),
            context_values,
            logs: Vec::new(),
            node_outcomes: node_outcomes_map,
        };
        let cp_path = Checkpoint::default_path(dir.path());
        tokio::fs::create_dir_all(dir.path()).await.unwrap();
        cp.save(&cp_path).unwrap();

        let result = runner
            .resume(dot, RunConfig::new(dir.path()))
            .await
            .unwrap();

        assert_eq!(result.status, StageStatus::Success);
        // V2-ATR-005: critical must be re-executed once because the Fail outcome
        // restored from the checkpoint triggers the goal gate, routing to retry_target.
        assert_eq!(
            mock.call_count("critical"),
            1,
            "goal gate must fire on resume when critical had Fail outcome; \
             critical should be called once, got {} calls",
            mock.call_count("critical")
        );
    }

    // ---------------------------------------------------------------------------
    // 10+ node pipeline
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn ten_node_pipeline_completes() {
        let dir = tempfile::tempdir().unwrap();
        let nodes: Vec<&str> = vec!["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"];
        let mock = Arc::new(MockCodergenBackend::new());
        let (runner, _rx) = make_runner(mock);
        let dot = linear_dot("big test", nodes.clone());
        let result = runner.run(&dot, RunConfig::new(dir.path())).await.unwrap();
        assert_eq!(result.status, StageStatus::Success);
        for n in &nodes {
            assert!(
                result.completed_nodes.contains(&n.to_string()),
                "Missing node {n}"
            );
        }
    }

    // ---------------------------------------------------------------------------
    // GAP-ATR-006 / GAP-ATR-009: status.json written for ALL nodes
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn all_nodes_write_status_json() {
        // NLSpec §11.3/§11.6: outcome must be written to
        // `{logs_root}/{node_id}/status.json` for EVERY executed node,
        // including start (shape=Mdiamond) and exit (shape=Msquare).
        let dir = tempfile::tempdir().unwrap();
        let mock = Arc::new(MockCodergenBackend::new());
        let (runner, _rx) = make_runner(mock.clone());
        let dot = linear_dot("test", vec!["task"]);
        runner.run(&dot, RunConfig::new(dir.path())).await.unwrap();

        // start node must have status.json
        let start_status = dir.path().join("start").join("status.json");
        assert!(
            start_status.exists(),
            "start node (shape=Mdiamond) must write status.json"
        );
        let start_json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&start_status).unwrap()).unwrap();
        assert_eq!(start_json["outcome"], "success");

        // codergen task node must have status.json
        let task_status = dir.path().join("task").join("status.json");
        assert!(
            task_status.exists(),
            "task (codergen) node must write status.json"
        );

        // exit node must have status.json
        let exit_status = dir.path().join("exit").join("status.json");
        assert!(
            exit_status.exists(),
            "exit node (shape=Msquare) must write status.json"
        );
        let exit_json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&exit_status).unwrap()).unwrap();
        assert_eq!(exit_json["outcome"], "success");
    }

    // ---------------------------------------------------------------------------
    // GAP-ATR-010: Tool handler (shape=parallelogram) runs e2e through PipelineRunner
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn tool_node_runs_e2e() {
        // NLSpec §11.6: tool handler executes configured tool/command.
        // Verify shape=parallelogram node routes to ToolHandler and runs
        // `echo hello` end-to-end through PipelineRunner.
        let dir = tempfile::tempdir().unwrap();
        let dot = r#"
digraph test {
    graph [goal="tool test"]
    start [shape=Mdiamond]
    exit  [shape=Msquare]
    echo_task [shape=parallelogram, tool_command="echo hello"]
    start -> echo_task -> exit
}
"#;
        let (runner, _rx) = PipelineRunner::builder().build();
        let result = runner.run(dot, RunConfig::new(dir.path())).await.unwrap();

        assert_eq!(result.status, StageStatus::Success);
        assert!(
            result.completed_nodes.contains(&"echo_task".to_string()),
            "tool node must be in completed_nodes"
        );
    }

    // ---------------------------------------------------------------------------
    // GAP-ATR-011: Completely new handler type registered and run e2e
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn new_handler_type_registered_and_run_e2e() {
        // NLSpec §11.6: custom handlers can be registered by type string.
        // Register a brand-new "my_custom_type" handler (not overriding any
        // built-in) and verify it executes through PipelineRunner.run().
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CustomHandler {
            call_count: Arc<AtomicUsize>,
        }

        #[async_trait::async_trait]
        impl crate::handler::Handler for CustomHandler {
            async fn execute(
                &self,
                _node: &crate::graph::Node,
                _context: &crate::state::context::Context,
                _graph: &crate::graph::Graph,
                _logs_root: &std::path::Path,
            ) -> Result<crate::state::context::Outcome, crate::error::EngineError> {
                self.call_count.fetch_add(1, Ordering::SeqCst);
                Ok(crate::state::context::Outcome::success())
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let call_count = Arc::new(AtomicUsize::new(0));
        let handler = Arc::new(CustomHandler {
            call_count: call_count.clone(),
        });

        let dot = r#"
digraph test {
    graph [goal="custom handler test"]
    start       [shape=Mdiamond]
    exit        [shape=Msquare]
    custom_node [type="my_custom_type", label="run custom"]
    start -> custom_node -> exit
}
"#;
        let (runner, _rx) = PipelineRunner::builder()
            .with_handler("my_custom_type", handler)
            .build();
        let result = runner.run(dot, RunConfig::new(dir.path())).await.unwrap();

        assert_eq!(result.status, StageStatus::Success);
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            1,
            "custom handler must be called once"
        );
        assert!(result.completed_nodes.contains(&"custom_node".to_string()));
    }

    // ---------------------------------------------------------------------------
    // GAP-ATR-019: WaitForHuman presents outgoing edge labels as choices
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn wait_human_presents_edge_labels_as_choices_to_interviewer() {
        // NLSpec §11.12: Wait.human presents choices derived from outgoing edge
        // labels. Verify the question options presented to the interviewer match
        // the three edge labels defined in the DOT.
        use crate::interviewer::{Answer, Interviewer, QuestionOption};

        let dir = tempfile::tempdir().unwrap();

        let dot = r#"
digraph test {
    graph [goal="human gate test"]
    start    [shape=Mdiamond]
    exit     [shape=Msquare]
    gate     [shape=hexagon, label="Which path?"]
    path_a   [prompt="path a"]
    path_b   [prompt="path b"]
    path_c   [prompt="path c"]
    start -> gate
    gate -> path_a [label="approve"]
    gate -> path_b [label="reject"]
    gate -> path_c [label="defer"]
    path_a -> exit
    path_b -> exit
    path_c -> exit
}
"#;
        // Build a RecordingInterviewer so we can inspect which question was
        // presented to the interviewer after the run.
        let recording_iv_inner = crate::interviewer::RecordingInterviewer::new(
            crate::interviewer::QueueInterviewer::new(vec![Answer::selected(QuestionOption {
                key: "A".to_string(),
                label: "approve".to_string(),
            })]),
        );
        let recording_arc = Arc::new(recording_iv_inner);
        let iv_ref = recording_arc.clone();

        let (runner, _rx) = PipelineRunner::builder()
            .with_interviewer(iv_ref as Arc<dyn Interviewer>)
            .build();

        runner.run(dot, RunConfig::new(dir.path())).await.unwrap();

        let recordings = recording_arc.recordings();
        assert_eq!(
            recordings.len(),
            1,
            "interviewer must be asked exactly once"
        );

        let question_options: Vec<String> = recordings[0]
            .question
            .options
            .iter()
            .map(|o| o.label.clone())
            .collect();

        // All three edge labels must appear as options.
        assert!(
            question_options.contains(&"approve".to_string()),
            "option 'approve' must be present, got: {question_options:?}"
        );
        assert!(
            question_options.contains(&"reject".to_string()),
            "option 'reject' must be present, got: {question_options:?}"
        );
        assert!(
            question_options.contains(&"defer".to_string()),
            "option 'defer' must be present, got: {question_options:?}"
        );
    }

    // ---------------------------------------------------------------------------
    // GAP-ATR-020: Parallel fan-out + fan-in e2e through PipelineRunner
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn parallel_fanout_fanin_e2e() {
        // NLSpec §11.12: parallel fan-out (shape=component) and fan-in
        // (shape=tripleoctagon) complete correctly in a full pipeline run.
        let dir = tempfile::tempdir().unwrap();
        let mock = Arc::new(MockCodergenBackend::new());
        let (runner, _rx) = make_runner(mock.clone());

        let dot = r#"
digraph test {
    graph [goal="parallel test"]
    start       [shape=Mdiamond]
    exit        [shape=Msquare]
    fan_out     [shape=component]
    branch_a    [prompt="branch a work"]
    branch_b    [prompt="branch b work"]
    branch_c    [prompt="branch c work"]
    fan_in      [shape=tripleoctagon]
    start -> fan_out
    fan_out -> branch_a
    fan_out -> branch_b
    fan_out -> branch_c
    branch_a -> fan_in
    branch_b -> fan_in
    branch_c -> fan_in
    fan_in -> exit
}
"#;
        let result = runner.run(dot, RunConfig::new(dir.path())).await.unwrap();

        assert_eq!(result.status, StageStatus::Success);
        assert!(result.completed_nodes.contains(&"fan_out".to_string()));
        assert!(result.completed_nodes.contains(&"fan_in".to_string()));

        // V2-ATR-008: ALL three branches must have been executed (AND, not OR).
        assert!(
            mock.call_count("branch_a") > 0,
            "branch_a must be executed; completed_nodes={:?}",
            result.completed_nodes
        );
        assert!(
            mock.call_count("branch_b") > 0,
            "branch_b must be executed; completed_nodes={:?}",
            result.completed_nodes
        );
        assert!(
            mock.call_count("branch_c") > 0,
            "branch_c must be executed; completed_nodes={:?}",
            result.completed_nodes
        );
    }

    // ---------------------------------------------------------------------------
    // GAP-ATR-021: Custom handler type verified through PipelineRunner.run() e2e
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn custom_handler_type_runs_full_pipeline() {
        // NLSpec §11.12: custom handler registration and execution works.
        // Uses a different type string to distinguish from GAP-ATR-011.
        use std::sync::atomic::{AtomicBool, Ordering};

        struct VerifyHandler {
            executed: Arc<AtomicBool>,
        }

        #[async_trait::async_trait]
        impl crate::handler::Handler for VerifyHandler {
            async fn execute(
                &self,
                _node: &crate::graph::Node,
                _context: &crate::state::context::Context,
                _graph: &crate::graph::Graph,
                _logs_root: &std::path::Path,
            ) -> Result<crate::state::context::Outcome, crate::error::EngineError> {
                self.executed.store(true, Ordering::SeqCst);
                Ok(crate::state::context::Outcome::success())
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let executed = Arc::new(AtomicBool::new(false));
        let handler = Arc::new(VerifyHandler {
            executed: executed.clone(),
        });

        let dot = r#"
digraph test {
    graph [goal="custom type e2e"]
    start      [shape=Mdiamond]
    exit       [shape=Msquare]
    verify_node [type="verify_action", label="verify action"]
    start -> verify_node -> exit
}
"#;
        let (runner, _rx) = PipelineRunner::builder()
            .with_handler("verify_action", handler)
            .build();
        let result = runner.run(dot, RunConfig::new(dir.path())).await.unwrap();

        assert_eq!(result.status, StageStatus::Success);
        assert!(
            executed.load(Ordering::SeqCst),
            "custom handler must have been executed"
        );
    }

    // ---------------------------------------------------------------------------
    // GAP-ATR-022: Stylesheet model override flows through to CodergenBackend
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn stylesheet_model_override_reaches_backend() {
        // NLSpec §11.12: stylesheet applies model override to nodes by shape,
        // and that override flows through to CodergenBackend.run() call.
        use crate::graph::Node;
        use crate::state::context::Context;
        use std::sync::Mutex;

        struct ModelCapturingBackend {
            captured_model: Arc<Mutex<Option<String>>>,
        }

        #[async_trait::async_trait]
        impl crate::handler::CodergenBackend for ModelCapturingBackend {
            async fn run(
                &self,
                node: &Node,
                _prompt: &str,
                _ctx: &Context,
            ) -> Result<crate::handler::CodergenResult, crate::error::EngineError> {
                *self.captured_model.lock().unwrap() = Some(node.llm_model.clone());
                Ok(crate::handler::CodergenResult::Outcome(
                    crate::state::context::Outcome::success(),
                ))
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let captured_model = Arc::new(Mutex::new(None::<String>));
        let backend = Arc::new(ModelCapturingBackend {
            captured_model: captured_model.clone(),
        });

        let dot = r#"
digraph test {
    graph [
        goal="stylesheet test",
        model_stylesheet="box { llm_model: gpt-4o-mini; }"
    ]
    start [shape=Mdiamond]
    exit  [shape=Msquare]
    task  [prompt="do the work"]
    start -> task -> exit
}
"#;

        struct BackendProxy(Arc<ModelCapturingBackend>);

        #[async_trait::async_trait]
        impl crate::handler::CodergenBackend for BackendProxy {
            async fn run(
                &self,
                node: &Node,
                prompt: &str,
                ctx: &Context,
            ) -> Result<crate::handler::CodergenResult, crate::error::EngineError> {
                self.0.run(node, prompt, ctx).await
            }
        }

        let codergen = Arc::new(CodergenHandler::new(Some(Box::new(BackendProxy(backend)))));
        let (runner, _rx) = PipelineRunner::builder()
            .with_handler("codergen", codergen)
            .build();

        runner.run(dot, RunConfig::new(dir.path())).await.unwrap();

        let model = captured_model.lock().unwrap().clone();
        assert_eq!(
            model.as_deref(),
            Some("gpt-4o-mini"),
            "stylesheet model override must reach CodergenBackend, got: {model:?}"
        );
    }

    // ---------------------------------------------------------------------------
    // GAP-ATR-023: Full integration smoke test (NLSpec §11.13)
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn full_smoke_test_goal_gate_checkpoint_artifacts() {
        // NLSpec §11.13: multi-node pipeline with goal_gate=true,
        // conditional branching, checkpoint verification, and all three
        // artifact files (prompt.md, response.md, status.json) for each
        // codergen node.
        let dir = tempfile::tempdir().unwrap();
        let mock = Arc::new(MockCodergenBackend::new());

        // plan → implement → review with goal gate on review.
        // review succeeds first time (goal gate satisfied → exit).
        mock.add_success("plan");
        mock.add_success("implement");
        mock.add_success("review");

        let dot = r#"
digraph smoke_test {
    graph [goal="build feature X"]
    start     [shape=Mdiamond]
    exit      [shape=Msquare]
    plan      [prompt="Plan the work for: $goal"]
    implement [prompt="Implement the plan"]
    review    [prompt="Review the implementation", goal_gate=true]
    start -> plan -> implement -> review -> exit
}
"#;
        let (runner, _rx) = make_runner(mock.clone());
        let result = runner.run(dot, RunConfig::new(dir.path())).await.unwrap();

        // Pipeline must succeed.
        assert_eq!(result.status, StageStatus::Success);

        // All nodes must complete.
        for node_id in &["start", "plan", "implement", "review", "exit"] {
            assert!(
                result.completed_nodes.contains(&node_id.to_string()),
                "node '{node_id}' must be in completed_nodes"
            );
        }

        // Checkpoint must exist.
        let cp_path = crate::state::checkpoint::Checkpoint::default_path(dir.path());
        assert!(cp_path.exists(), "checkpoint must be written");

        // All three artifacts must exist for each codergen node.
        for node_id in &["plan", "implement", "review"] {
            let node_dir = dir.path().join(node_id);
            assert!(
                node_dir.join("prompt.md").exists(),
                "{node_id}/prompt.md must exist"
            );
            assert!(
                node_dir.join("response.md").exists(),
                "{node_id}/response.md must exist"
            );
            assert!(
                node_dir.join("status.json").exists(),
                "{node_id}/status.json must exist"
            );

            // status.json must have the correct field name ("outcome").
            let status_text = std::fs::read_to_string(node_dir.join("status.json")).unwrap();
            let status_json: serde_json::Value = serde_json::from_str(&status_text).unwrap();
            assert_eq!(
                status_json["outcome"], "success",
                "{node_id}/status.json must have outcome=success"
            );
        }

        // Goal gate node (review) must be in completed_nodes with success.
        assert_eq!(
            mock.call_count("review"),
            1,
            "review (goal_gate) called exactly once"
        );

        // Variable expansion: prompt.md for 'plan' must contain the expanded goal.
        let plan_prompt =
            std::fs::read_to_string(dir.path().join("plan").join("prompt.md")).unwrap();
        assert!(
            plan_prompt.contains("build feature X"),
            "plan prompt must contain expanded $goal, got: {plan_prompt}"
        );
    }
}
