//! Wait-for-human handler for `shape=hexagon` / `type="wait.human"` nodes.
//!
//! Derives choices from the node's outgoing edge labels, presents them to the
//! configured [`Interviewer`], and returns the selected label as
//! `preferred_label` in the [`Outcome`].

use async_trait::async_trait;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::fs;

use crate::error::EngineError;
use crate::graph::{Graph, Node, Value};
use crate::handler::Handler;
use crate::interviewer::{AnswerValue, Interviewer, Question, QuestionOption, QuestionType};
use crate::state::context::{Context, Outcome};

// ---------------------------------------------------------------------------
// WaitForHumanHandler
// ---------------------------------------------------------------------------

/// Handler for `shape=hexagon` (`type="wait.human"`) nodes.
///
/// Blocks pipeline execution until the [`Interviewer`] returns an answer,
/// then routes by setting `preferred_label` in the returned [`Outcome`].
pub struct WaitForHumanHandler {
    interviewer: Arc<dyn Interviewer>,
}

impl WaitForHumanHandler {
    /// Create a new handler with the given interviewer.
    pub fn new(interviewer: Arc<dyn Interviewer>) -> Self {
        WaitForHumanHandler { interviewer }
    }
}

#[async_trait]
impl Handler for WaitForHumanHandler {
    async fn execute(
        &self,
        node: &Node,
        _context: &Context,
        graph: &Graph,
        logs_root: &Path,
    ) -> Result<Outcome, EngineError> {
        // 1. Derive choices from outgoing edges.
        let edges = graph.outgoing_edges(&node.id);
        if edges.is_empty() {
            return Ok(Outcome::fail("No outgoing edges for human gate"));
        }

        let mut options: Vec<QuestionOption> = Vec::new();
        for edge in &edges {
            let display_label = if !edge.label.is_empty() {
                edge.label.clone()
            } else {
                edge.to.clone()
            };
            let (key, _stripped) = parse_accelerator(&display_label);
            options.push(QuestionOption {
                key,
                label: display_label,
            });
        }

        // 2. Build question.
        let question_text = if !node.label.is_empty() {
            node.label.clone()
        } else {
            "Select an option:".to_string()
        };

        let question = Question {
            text: question_text,
            question_type: QuestionType::MultipleChoice,
            options: options.clone(),
            default: None,
            timeout: node.timeout,
            stage: node.id.clone(),
            metadata: HashMap::new(),
        };

        // 3. Present to interviewer.
        let answer = self.interviewer.ask(question).await;

        // 4. Handle timeout and skip.
        match &answer.value {
            AnswerValue::Timeout => {
                // Check for a configured default choice.
                if let Some(Value::Str(default_choice)) = node.extra.get("human.default_choice") {
                    let preferred = default_choice.clone();
                    let outcome = build_outcome(&preferred, &options, &edges);
                    write_status(logs_root, &node.id, &outcome).await?;
                    return Ok(outcome);
                }
                return Ok(Outcome::retry("human gate timeout, no default"));
            }
            AnswerValue::Skipped => {
                return Ok(Outcome::fail("human skipped interaction"));
            }
            _ => {}
        }

        // 5. Determine selected label.
        let selected_label = if let Some(opt) = &answer.selected_option {
            opt.label.clone()
        } else if let AnswerValue::Selected(key) = &answer.value {
            // Find the option whose key matches.
            options
                .iter()
                .find(|o| o.key.eq_ignore_ascii_case(key))
                .map(|o| o.label.clone())
                .unwrap_or_else(|| answer.text.clone())
        } else {
            // Fallback to text or first option.
            if !answer.text.is_empty() {
                answer.text.clone()
            } else {
                options.first().map(|o| o.label.clone()).unwrap_or_default()
            }
        };

        // 6. Build outcome.
        let outcome = build_outcome(&selected_label, &options, &edges);
        write_status(logs_root, &node.id, &outcome).await?;
        Ok(outcome)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build the [`Outcome`] for the selected label.
fn build_outcome(
    selected_label: &str,
    options: &[QuestionOption],
    edges: &[&crate::graph::Edge],
) -> Outcome {
    // Find the key for the selected label.
    let selected_key = options
        .iter()
        .find(|o| o.label == selected_label)
        .map(|o| o.key.clone())
        .unwrap_or_default();

    // Find any matching edge targets.
    let suggested: Vec<String> = edges
        .iter()
        .filter(|e| {
            let display = if !e.label.is_empty() {
                e.label.as_str()
            } else {
                e.to.as_str()
            };
            display == selected_label
        })
        .map(|e| e.to.clone())
        .collect();

    let mut context_updates = HashMap::new();
    context_updates.insert(
        "human.gate.selected".to_string(),
        Value::Str(selected_key.clone()),
    );
    context_updates.insert(
        "human.gate.label".to_string(),
        Value::Str(selected_label.to_string()),
    );

    Outcome {
        preferred_label: selected_label.to_string(),
        suggested_next_ids: suggested,
        context_updates,
        ..Outcome::success()
    }
}

/// Write `status.json` to `{logs_root}/{node_id}/status.json`.
async fn write_status(
    logs_root: &Path,
    node_id: &str,
    outcome: &Outcome,
) -> Result<(), EngineError> {
    let stage_dir = logs_root.join(node_id);
    fs::create_dir_all(&stage_dir).await?;
    let json = serde_json::to_string_pretty(outcome).map_err(|e| EngineError::Handler {
        node_id: node_id.to_string(),
        message: format!("failed to serialise outcome: {e}"),
    })?;
    fs::write(stage_dir.join("status.json"), json).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Accelerator key parsing
// ---------------------------------------------------------------------------

/// Parse an accelerator key from an edge label.
///
/// Priority order:
/// 1. `[K] Label` → key = `K` (uppercased)
/// 2. `K) Label`  → key = `K` (uppercased; K is a single non-whitespace char)
/// 3. `K - Label` → key = `K` (uppercased; K is a single non-whitespace char)
/// 4. First non-whitespace character (uppercased)
///
/// Returns `(key, stripped_label)` where `stripped_label` is the label with
/// the accelerator prefix removed and leading whitespace trimmed.
pub fn parse_accelerator(label: &str) -> (String, String) {
    let label = label.trim();

    if label.is_empty() {
        return (String::new(), String::new());
    }

    // Pattern 1: [K] rest
    if label.starts_with('[') {
        let mut chars = label.chars().skip(1);
        if let Some(k) = chars.next() {
            let rest: String = chars.collect();
            if let Some(after_bracket) = rest.strip_prefix(']') {
                let stripped = after_bracket.trim_start().to_string();
                return (k.to_ascii_uppercase().to_string(), stripped);
            }
        }
    }

    let mut chars = label.chars();
    let first = match chars.next() {
        Some(c) => c,
        None => return (String::new(), String::new()),
    };

    // Pattern 2: K) rest   (single char followed by ')')
    {
        let rest: String = chars.clone().collect();
        if let Some(after_paren) = rest.strip_prefix(')') {
            let stripped = after_paren.trim_start().to_string();
            return (first.to_ascii_uppercase().to_string(), stripped);
        }
    }

    // Pattern 3: K - rest  (single char, optional spaces, '-', rest)
    {
        let rest: String = chars.clone().collect();
        let trimmed = rest.trim_start();
        if let Some(after_dash) = trimmed.strip_prefix('-') {
            let stripped = after_dash.trim_start().to_string();
            return (first.to_ascii_uppercase().to_string(), stripped);
        }
    }

    // Fallback: first non-whitespace char is the key; label unchanged.
    let key = first.to_ascii_uppercase().to_string();
    (key, label.to_string())
}

/// Normalise a label for comparison: lowercase + trim whitespace +
/// strip leading accelerator prefix.
pub fn normalize_label(label: &str) -> String {
    let (_key, stripped) = parse_accelerator(label);
    let candidate = if stripped.is_empty() {
        label.trim().to_string()
    } else {
        stripped
    };
    candidate.to_lowercase()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{Edge, Graph, Node};
    use crate::interviewer::{Answer, AutoApproveInterviewer, QueueInterviewer};
    use crate::state::context::StageStatus;
    use std::sync::Arc;

    fn make_graph_with_edges(node_id: &str, labels: &[&str]) -> Graph {
        let mut g = Graph::new("test".into());
        let mut n = Node::default();
        n.id = node_id.to_string();
        n.label = "Choose:".to_string();
        n.shape = "hexagon".to_string();
        g.nodes.insert(node_id.to_string(), n);

        for (i, lbl) in labels.iter().enumerate() {
            let target = format!("target_{i}");
            let mut tn = Node::default();
            tn.id = target.clone();
            g.nodes.insert(target.clone(), tn);
            g.edges.push(Edge {
                from: node_id.to_string(),
                to: target,
                label: lbl.to_string(),
                ..Default::default()
            });
        }
        g
    }

    // -- parse_accelerator --

    #[test]
    fn parse_bracket_pattern() {
        let (key, stripped) = parse_accelerator("[Y] Yes");
        assert_eq!(key, "Y");
        assert_eq!(stripped, "Yes");
    }

    #[test]
    fn parse_paren_pattern() {
        let (key, stripped) = parse_accelerator("Y) Yes");
        assert_eq!(key, "Y");
        assert_eq!(stripped, "Yes");
    }

    #[test]
    fn parse_dash_pattern() {
        let (key, stripped) = parse_accelerator("Y - Yes");
        assert_eq!(key, "Y");
        assert_eq!(stripped, "Yes");
    }

    #[test]
    fn parse_fallback() {
        let (key, stripped) = parse_accelerator("Yes, deploy");
        assert_eq!(key, "Y");
        assert_eq!(stripped, "Yes, deploy");
    }

    #[test]
    fn parse_empty_label() {
        let (key, stripped) = parse_accelerator("");
        assert!(key.is_empty());
        assert!(stripped.is_empty());
    }

    #[test]
    fn parse_lowercase_key_uppercased() {
        let (key, _) = parse_accelerator("[a] approve");
        assert_eq!(key, "A");
    }

    // -- normalize_label --

    #[test]
    fn normalize_bracket_prefix() {
        assert_eq!(normalize_label("[A] Approve"), "approve");
    }

    #[test]
    fn normalize_plain_label() {
        assert_eq!(normalize_label("Yes"), "yes");
    }

    // -- WaitForHumanHandler --

    #[tokio::test]
    async fn auto_approve_selects_first_option() {
        let dir = tempfile::tempdir().unwrap();
        let iv = Arc::new(AutoApproveInterviewer);
        let handler = WaitForHumanHandler::new(iv);
        let graph = make_graph_with_edges("gate", &["[A] Approve", "[R] Reject"]);
        let node = graph.node("gate").unwrap().clone();
        let ctx = Context::new();
        let outcome = handler
            .execute(&node, &ctx, &graph, dir.path())
            .await
            .unwrap();
        assert_eq!(outcome.status, StageStatus::Success);
        assert_eq!(outcome.preferred_label, "[A] Approve");
    }

    #[tokio::test]
    async fn queue_interviewer_routes_correctly() {
        let dir = tempfile::tempdir().unwrap();
        let opt = QuestionOption {
            key: "R".to_string(),
            label: "[R] Reject".to_string(),
        };
        let answer = Answer::selected(opt);
        let iv = Arc::new(QueueInterviewer::new(vec![answer]));
        let handler = WaitForHumanHandler::new(iv);
        let graph = make_graph_with_edges("gate", &["[A] Approve", "[R] Reject"]);
        let node = graph.node("gate").unwrap().clone();
        let ctx = Context::new();
        let outcome = handler
            .execute(&node, &ctx, &graph, dir.path())
            .await
            .unwrap();
        assert_eq!(outcome.status, StageStatus::Success);
        assert_eq!(outcome.preferred_label, "[R] Reject");
    }

    #[tokio::test]
    async fn no_edges_returns_fail() {
        let dir = tempfile::tempdir().unwrap();
        let iv = Arc::new(AutoApproveInterviewer);
        let handler = WaitForHumanHandler::new(iv);
        let graph = make_graph_with_edges("gate", &[]);
        let node = graph.node("gate").unwrap().clone();
        let ctx = Context::new();
        let outcome = handler
            .execute(&node, &ctx, &graph, dir.path())
            .await
            .unwrap();
        assert_eq!(outcome.status, StageStatus::Fail);
    }

    #[tokio::test]
    async fn skipped_answer_returns_fail() {
        let dir = tempfile::tempdir().unwrap();
        let iv = Arc::new(QueueInterviewer::new(vec![Answer::skipped()]));
        let handler = WaitForHumanHandler::new(iv);
        let graph = make_graph_with_edges("gate", &["Approve"]);
        let node = graph.node("gate").unwrap().clone();
        let ctx = Context::new();
        let outcome = handler
            .execute(&node, &ctx, &graph, dir.path())
            .await
            .unwrap();
        assert_eq!(outcome.status, StageStatus::Fail);
    }

    #[tokio::test]
    async fn timeout_without_default_returns_retry() {
        let dir = tempfile::tempdir().unwrap();
        let iv = Arc::new(QueueInterviewer::new(vec![Answer::timeout()]));
        let handler = WaitForHumanHandler::new(iv);
        let graph = make_graph_with_edges("gate", &["Approve"]);
        let node = graph.node("gate").unwrap().clone();
        let ctx = Context::new();
        let outcome = handler
            .execute(&node, &ctx, &graph, dir.path())
            .await
            .unwrap();
        assert_eq!(outcome.status, StageStatus::Retry);
    }

    #[tokio::test]
    async fn status_json_written() {
        let dir = tempfile::tempdir().unwrap();
        let iv = Arc::new(AutoApproveInterviewer);
        let handler = WaitForHumanHandler::new(iv);
        let graph = make_graph_with_edges("gate", &["Approve"]);
        let node = graph.node("gate").unwrap().clone();
        let ctx = Context::new();
        handler
            .execute(&node, &ctx, &graph, dir.path())
            .await
            .unwrap();
        let path = dir.path().join("gate").join("status.json");
        assert!(path.exists());
    }

    #[tokio::test]
    async fn context_updates_set_correctly() {
        let dir = tempfile::tempdir().unwrap();
        let iv = Arc::new(AutoApproveInterviewer);
        let handler = WaitForHumanHandler::new(iv);
        let graph = make_graph_with_edges("gate", &["[A] Approve"]);
        let node = graph.node("gate").unwrap().clone();
        let ctx = Context::new();
        let outcome = handler
            .execute(&node, &ctx, &graph, dir.path())
            .await
            .unwrap();
        assert!(outcome.context_updates.contains_key("human.gate.selected"));
        assert!(outcome.context_updates.contains_key("human.gate.label"));
    }
}
