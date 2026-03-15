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
    CodergenHandler, ConditionalHandler, ExitHandler, Handler, HandlerRegistry, StartHandler,
    ToolHandler, WaitForHumanHandler,
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

        // Build synthetic outcomes for completed nodes (assume success for goal gate purposes).
        let mut node_outcomes: HashMap<String, Outcome> = HashMap::new();
        for node_id in &completed_nodes {
            node_outcomes.insert(node_id.clone(), Outcome::success());
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
            let cp = build_checkpoint(&context, &current_node_id, &completed_nodes, &node_retries);
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
    pub fn build(self) -> (PipelineRunner, broadcast::Receiver<PipelineEvent>) {
        let (tx, rx) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        let runner = PipelineRunner {
            registry: Arc::new(self.registry),
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
// Helper: build_checkpoint
// ---------------------------------------------------------------------------

fn build_checkpoint(
    context: &Context,
    current_node: &str,
    completed: &[String],
    retries: &HashMap<String, u32>,
) -> Checkpoint {
    Checkpoint {
        timestamp: Utc::now(),
        current_node: current_node.to_string(),
        completed_nodes: completed.to_vec(),
        node_retries: retries.clone(),
        context_values: context.snapshot(),
        logs: context.logs_snapshot(),
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
}
