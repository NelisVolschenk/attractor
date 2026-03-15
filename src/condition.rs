//! Condition expression parser and evaluator (NLSpec §10).
//!
//! The condition language is a minimal boolean DSL for edge guards:
//! ```text
//! ConditionExpr  ::= Clause ( '&&' Clause )*
//! Clause         ::= Key Operator Literal
//! Key            ::= 'outcome' | 'preferred_label' | 'context.' Path
//! Operator       ::= '=' | '!='
//! Literal        ::= anything up to '&&' or end-of-string
//! ```
//!
//! ## Public API
//! - [`parse_condition`] — structural parse into [`ConditionExpr`]
//! - [`evaluate_condition`] — runtime evaluation against outcome + context

use crate::error::{EngineError, ParseError};
use serde_json::Value as JsonValue;
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A parsed condition expression: AND of one or more [`Clause`]s.
///
/// An empty `clauses` vec always evaluates to `true`.
#[derive(Debug, Clone, PartialEq)]
pub struct ConditionExpr {
    pub clauses: Vec<Clause>,
}

/// A single comparison clause: `key op literal`.
#[derive(Debug, Clone, PartialEq)]
pub struct Clause {
    pub key: ConditionKey,
    pub op: ConditionOp,
    /// The right-hand side literal (unquoted string).
    pub value: String,
}

/// The left-hand side key of a condition clause.
#[derive(Debug, Clone, PartialEq)]
pub enum ConditionKey {
    /// The `outcome` variable — resolves to outcome status string.
    Outcome,
    /// The `preferred_label` variable.
    PreferredLabel,
    /// A `context.*` or bare context key lookup.
    Context(String),
}

/// The comparison operator.
#[derive(Debug, Clone, PartialEq)]
pub enum ConditionOp {
    /// `=` — exact equality (case-sensitive).
    Eq,
    /// `!=` — inequality.
    NotEq,
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

/// Parse a condition expression string into a [`ConditionExpr`].
///
/// Returns [`ParseError::ConditionSyntax`] on invalid input.
/// Empty string is valid and returns `ConditionExpr { clauses: vec![] }`.
pub fn parse_condition(expr: &str) -> Result<ConditionExpr, ParseError> {
    let expr = expr.trim();
    if expr.is_empty() {
        return Ok(ConditionExpr { clauses: vec![] });
    }

    // Split on `&&` — handles `&&` with surrounding whitespace.
    let raw_clauses: Vec<&str> = expr.split("&&").collect();
    let mut clauses = Vec::with_capacity(raw_clauses.len());

    for raw in raw_clauses {
        let raw = raw.trim();
        if raw.is_empty() {
            return Err(ParseError::ConditionSyntax(format!(
                "empty clause in expression: {expr:?}"
            )));
        }
        clauses
            .push(parse_clause(raw).map_err(|e| {
                ParseError::ConditionSyntax(format!("invalid clause {raw:?}: {e}"))
            })?);
    }

    Ok(ConditionExpr { clauses })
}

fn parse_clause(raw: &str) -> Result<Clause, String> {
    // Try `!=` first (before `=`) to avoid consuming the `!` as part of a key.
    if let Some(pos) = raw.find("!=") {
        let key_str = raw[..pos].trim();
        let val_str = raw[pos + 2..].trim();
        let key = parse_key(key_str)?;
        return Ok(Clause {
            key,
            op: ConditionOp::NotEq,
            value: val_str.to_string(),
        });
    }
    if let Some(pos) = raw.find('=') {
        let key_str = raw[..pos].trim();
        let val_str = raw[pos + 1..].trim();
        let key = parse_key(key_str)?;
        return Ok(Clause {
            key,
            op: ConditionOp::Eq,
            value: val_str.to_string(),
        });
    }
    Err(format!("no operator found in clause {raw:?}"))
}

fn parse_key(raw: &str) -> Result<ConditionKey, String> {
    match raw {
        "outcome" => Ok(ConditionKey::Outcome),
        "preferred_label" => Ok(ConditionKey::PreferredLabel),
        s if s.starts_with("context.") => {
            let path = s["context.".len()..].to_string();
            if path.is_empty() {
                return Err("context key path is empty".to_string());
            }
            Ok(ConditionKey::Context(path))
        }
        s => {
            // Direct context key (convenience — no `context.` prefix)
            if s.is_empty() {
                return Err("key is empty".to_string());
            }
            // Validate: must be identifier-like
            if s.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
            {
                Ok(ConditionKey::Context(s.to_string()))
            } else {
                Err(format!("invalid key: {s:?}"))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Evaluator
// ---------------------------------------------------------------------------

/// Evaluate a condition string against outcome state and run context.
///
/// - `outcome_status`: the stage status string (`"success"`, `"fail"`, etc.).
/// - `preferred_label`: the outcome's preferred label (may be empty).
/// - `context`: flat key-value map of context values.
///
/// Returns `Ok(true)` if all clauses pass, `Ok(false)` if any clause fails.
/// An empty condition always returns `Ok(true)`.
pub fn evaluate_condition(
    expr: &str,
    outcome_status: &str,
    preferred_label: &str,
    context: &HashMap<String, JsonValue>,
) -> Result<bool, EngineError> {
    let cond = parse_condition(expr).map_err(EngineError::Parse)?;
    if cond.clauses.is_empty() {
        return Ok(true);
    }
    for clause in &cond.clauses {
        let lhs = resolve_key(&clause.key, outcome_status, preferred_label, context);
        let rhs = &clause.value;
        let passes = match clause.op {
            ConditionOp::Eq => lhs == *rhs,
            ConditionOp::NotEq => lhs != *rhs,
        };
        if !passes {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Resolve a condition key to a string value for comparison.
fn resolve_key(
    key: &ConditionKey,
    outcome_status: &str,
    preferred_label: &str,
    context: &HashMap<String, JsonValue>,
) -> String {
    match key {
        ConditionKey::Outcome => outcome_status.to_string(),
        ConditionKey::PreferredLabel => preferred_label.to_string(),
        ConditionKey::Context(path) => {
            // Try `context.<path>` first, then bare `<path>`.
            let full_key = format!("context.{path}");
            if let Some(v) = context.get(&full_key) {
                return json_value_to_string(v);
            }
            if let Some(v) = context.get(path.as_str()) {
                return json_value_to_string(v);
            }
            String::new()
        }
    }
}

fn json_value_to_string(v: &JsonValue) -> String {
    match v {
        JsonValue::String(s) => s.clone(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::Null => String::new(),
        other => other.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ctx(pairs: &[(&str, JsonValue)]) -> HashMap<String, JsonValue> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    // --- parse_condition ---

    #[test]
    fn parse_empty_condition() {
        let c = parse_condition("").unwrap();
        assert!(c.clauses.is_empty());
    }

    #[test]
    fn parse_outcome_eq() {
        let c = parse_condition("outcome=success").unwrap();
        assert_eq!(c.clauses.len(), 1);
        assert_eq!(c.clauses[0].key, ConditionKey::Outcome);
        assert_eq!(c.clauses[0].op, ConditionOp::Eq);
        assert_eq!(c.clauses[0].value, "success");
    }

    #[test]
    fn parse_outcome_neq() {
        let c = parse_condition("outcome!=fail").unwrap();
        assert_eq!(c.clauses[0].op, ConditionOp::NotEq);
        assert_eq!(c.clauses[0].value, "fail");
    }

    #[test]
    fn parse_preferred_label() {
        let c = parse_condition("preferred_label=Fix").unwrap();
        assert_eq!(c.clauses[0].key, ConditionKey::PreferredLabel);
        assert_eq!(c.clauses[0].value, "Fix");
    }

    #[test]
    fn parse_context_key() {
        let c = parse_condition("context.tests_passed=true").unwrap();
        assert_eq!(
            c.clauses[0].key,
            ConditionKey::Context("tests_passed".to_string())
        );
    }

    #[test]
    fn parse_two_clauses() {
        let c = parse_condition("outcome=success && context.tests_passed=true").unwrap();
        assert_eq!(c.clauses.len(), 2);
    }

    #[test]
    fn parse_trailing_and_is_error() {
        assert!(parse_condition("outcome=success &&").is_err());
    }

    // --- evaluate_condition (NLSpec §10.6 examples) ---

    #[test]
    fn eval_empty_always_true() {
        assert!(evaluate_condition("", "success", "", &ctx(&[])).unwrap());
    }

    #[test]
    fn eval_outcome_success_match() {
        assert!(evaluate_condition("outcome=success", "success", "", &ctx(&[])).unwrap());
    }

    #[test]
    fn eval_outcome_success_no_match() {
        assert!(!evaluate_condition("outcome=success", "fail", "", &ctx(&[])).unwrap());
    }

    #[test]
    fn eval_outcome_neq() {
        assert!(evaluate_condition("outcome!=fail", "success", "", &ctx(&[])).unwrap());
        assert!(!evaluate_condition("outcome!=fail", "fail", "", &ctx(&[])).unwrap());
    }

    #[test]
    fn eval_context_key_present() {
        let c = ctx(&[("tests_passed", json!("true"))]);
        assert!(evaluate_condition("context.tests_passed=true", "success", "", &c).unwrap());
    }

    #[test]
    fn eval_context_key_missing_is_empty() {
        // Missing key resolves to ""
        assert!(!evaluate_condition("context.missing_key=something", "s", "", &ctx(&[])).unwrap());
        // Empty-string comparison
        assert!(evaluate_condition("context.missing_key=", "s", "", &ctx(&[])).unwrap());
    }

    #[test]
    fn eval_context_neq() {
        let c = ctx(&[("loop_state", json!("exhausted"))]);
        assert!(!evaluate_condition("context.loop_state!=exhausted", "s", "", &c).unwrap());
        let c2 = ctx(&[("loop_state", json!("active"))]);
        assert!(evaluate_condition("context.loop_state!=exhausted", "s", "", &c2).unwrap());
    }

    #[test]
    fn eval_preferred_label() {
        assert!(evaluate_condition("preferred_label=Fix", "s", "Fix", &ctx(&[])).unwrap());
        assert!(!evaluate_condition("preferred_label=Fix", "s", "Approve", &ctx(&[])).unwrap());
    }

    #[test]
    fn eval_multi_clause_all_pass() {
        let c = ctx(&[("tests_passed", json!("true"))]);
        assert!(
            evaluate_condition(
                "outcome=success && context.tests_passed=true",
                "success",
                "",
                &c
            )
            .unwrap()
        );
    }

    #[test]
    fn eval_multi_clause_one_fails() {
        let c = ctx(&[("tests_passed", json!("false"))]);
        assert!(
            !evaluate_condition(
                "outcome=success && context.tests_passed=true",
                "success",
                "",
                &c
            )
            .unwrap()
        );
    }
}
