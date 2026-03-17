//! Tool handler — executes a configured shell command for `shape=parallelogram` nodes.

use async_trait::async_trait;
use std::path::Path;
use tokio::process::Command;

use crate::error::EngineError;
use crate::graph::{Graph, Node, Value};
use crate::handler::Handler;
use crate::state::context::{Context, Outcome};

/// Handler for `shape=parallelogram` (`type="tool"`) nodes.
///
/// Reads the shell command from `node.extra["tool_command"]`, executes it via
/// `sh -c`, captures stdout/stderr, and returns an [`Outcome`] with the
/// output stored in `context_updates["tool.output"]`.
pub struct ToolHandler;

#[async_trait]
impl Handler for ToolHandler {
    async fn execute(
        &self,
        node: &Node,
        _context: &Context,
        _graph: &Graph,
        logs_root: &Path,
    ) -> Result<Outcome, EngineError> {
        // 1. Read tool_command from node extra attributes.
        let command = match node.extra.get("tool_command").and_then(|v| v.as_str()) {
            Some(cmd) if !cmd.is_empty() => cmd.to_owned(),
            _ => {
                return Ok(Outcome::fail("No tool_command specified"));
            }
        };

        // 2. Build the async command.
        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg(&command);
        cmd.current_dir(logs_root);

        // 3. Execute with optional timeout.
        let output_result = if let Some(timeout) = node.timeout {
            match tokio::time::timeout(timeout, cmd.output()).await {
                Ok(result) => result,
                Err(_) => {
                    return Ok(Outcome::fail(format!(
                        "Tool timed out after {}ms",
                        timeout.as_millis()
                    )));
                }
            }
        } else {
            cmd.output().await
        };

        // 4. Handle spawn / IO error.
        let output = match output_result {
            Ok(o) => o,
            Err(e) => {
                return Ok(Outcome::fail(format!("Failed to spawn command: {e}")));
            }
        };

        // 5. Build outcome based on exit code.
        let exit_code = output.status.code().unwrap_or(-1) as i64;
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();

        let mut context_updates = std::collections::HashMap::new();
        context_updates.insert("tool.exit_code".to_string(), Value::Int(exit_code));

        if output.status.success() {
            context_updates.insert("tool.output".to_string(), Value::Str(stdout.clone()));
            // SRV-BUG-005: DOT edge conditions use `context.tool_stdout` (e.g.
            // `condition="context.tool_stdout=done"`) but the handler only stored
            // stdout as `tool.output`.  Store it under `tool_stdout` as well so
            // all existing dotpowers.dot conditions resolve correctly.
            context_updates.insert("tool_stdout".to_string(), Value::Str(stdout.clone()));
            Ok(Outcome {
                notes: format!("Tool completed: {command}"),
                context_updates,
                ..Outcome::success()
            })
        } else {
            // Non-zero exit: still expose stdout/stderr for condition checks.
            context_updates.insert("tool.output".to_string(), Value::Str(stdout.clone()));
            context_updates.insert("tool_stdout".to_string(), Value::Str(stdout.clone()));
            Ok(Outcome {
                failure_reason: format!("Tool exited with code {exit_code}: {stderr}"),
                context_updates,
                ..Outcome::fail(String::new())
            })
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::Graph;
    use crate::state::context::StageStatus;
    use std::time::Duration;

    fn make_node_with_command(cmd: &str) -> Node {
        let mut n = Node::default();
        n.id = "tool_node".to_string();
        n.extra
            .insert("tool_command".to_string(), Value::Str(cmd.to_string()));
        n
    }

    #[tokio::test]
    async fn echo_command_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let handler = ToolHandler;
        let node = make_node_with_command("echo hello");
        let ctx = Context::new();
        let graph = Graph::new("t".into());
        let outcome = handler
            .execute(&node, &ctx, &graph, dir.path())
            .await
            .unwrap();
        assert_eq!(outcome.status, StageStatus::Success);
        assert_eq!(
            outcome.context_updates.get("tool.output"),
            Some(&Value::Str("hello\n".to_string()))
        );
        assert_eq!(
            outcome.context_updates.get("tool.exit_code"),
            Some(&Value::Int(0))
        );
    }

    /// SRV-BUG-005: DOT edge conditions use `context.tool_stdout` but
    /// ToolHandler only stored stdout as `tool.output`.  The handler must
    /// ALSO store stdout under the `tool_stdout` key so conditions like
    /// `context.tool_stdout=done` can resolve correctly.
    #[tokio::test]
    async fn stdout_stored_as_tool_stdout_key_for_conditions() {
        let dir = tempfile::tempdir().unwrap();
        let handler = ToolHandler;
        let node = make_node_with_command("printf 'done'");
        let ctx = Context::new();
        let graph = Graph::new("t".into());
        let outcome = handler
            .execute(&node, &ctx, &graph, dir.path())
            .await
            .unwrap();
        assert_eq!(outcome.status, StageStatus::Success);
        // Must be stored under tool_stdout for DOT conditions like
        // condition="context.tool_stdout=done"
        assert_eq!(
            outcome.context_updates.get("tool_stdout"),
            Some(&Value::Str("done".to_string())),
            "tool_stdout key required for DOT edge conditions"
        );
    }

    #[tokio::test]
    async fn non_zero_exit_fails() {
        let dir = tempfile::tempdir().unwrap();
        let handler = ToolHandler;
        let node = make_node_with_command("exit 1");
        let ctx = Context::new();
        let graph = Graph::new("t".into());
        let outcome = handler
            .execute(&node, &ctx, &graph, dir.path())
            .await
            .unwrap();
        assert_eq!(outcome.status, StageStatus::Fail);
        assert!(outcome.context_updates.get("tool.exit_code").is_some());
    }

    #[tokio::test]
    async fn missing_command_fails() {
        let dir = tempfile::tempdir().unwrap();
        let handler = ToolHandler;
        let node = Node::default(); // no tool_command extra attr
        let ctx = Context::new();
        let graph = Graph::new("t".into());
        let outcome = handler
            .execute(&node, &ctx, &graph, dir.path())
            .await
            .unwrap();
        assert_eq!(outcome.status, StageStatus::Fail);
        assert!(outcome.failure_reason.contains("tool_command"));
    }

    #[tokio::test]
    async fn timeout_fails() {
        let dir = tempfile::tempdir().unwrap();
        let handler = ToolHandler;
        let mut node = make_node_with_command("sleep 10");
        node.timeout = Some(Duration::from_millis(50));
        let ctx = Context::new();
        let graph = Graph::new("t".into());
        let outcome = handler
            .execute(&node, &ctx, &graph, dir.path())
            .await
            .unwrap();
        assert_eq!(outcome.status, StageStatus::Fail);
        assert!(
            outcome.failure_reason.contains("timed out"),
            "expected timeout message, got: {}",
            outcome.failure_reason
        );
    }
}
