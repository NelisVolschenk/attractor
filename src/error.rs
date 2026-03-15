//! Error types for the attractor crate.
//!
//! Three error enums cover the three failure domains:
//! - [`ParseError`] — DOT source parsing and expression parsing failures
//! - [`ValidationError`] — lint-rule validation failures
//! - [`EngineError`] — runtime execution failures

/// Errors produced during DOT parsing, condition parsing, or stylesheet parsing.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ParseError {
    /// A syntax error in the DOT source.
    #[error("DOT syntax error at position {position}: {message}")]
    Syntax { position: usize, message: String },

    /// An attribute value could not be converted to the expected type.
    #[error("invalid attribute value for '{key}': {message}")]
    InvalidAttribute { key: String, message: String },

    /// A condition expression string is syntactically invalid.
    #[error("condition syntax error: {0}")]
    ConditionSyntax(String),

    /// A model stylesheet string is syntactically invalid.
    #[error("stylesheet syntax error: {0}")]
    StylesheetSyntax(String),
}

/// Errors produced during graph validation.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ValidationError {
    /// The graph failed validation with one or more error-severity diagnostics.
    #[error("validation failed with {count} error(s)")]
    Failed { count: usize },
}

/// Errors produced during pipeline execution.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EngineError {
    /// A parse error occurred.
    #[error("parse error: {0}")]
    Parse(#[from] ParseError),

    /// A validation error occurred.
    #[error("validation error: {0}")]
    Validation(#[from] ValidationError),

    /// No start node (shape=Mdiamond) was found in the graph.
    #[error("no start node found")]
    NoStartNode,

    /// No exit node (shape=Msquare) was found in the graph.
    #[error("no exit node found")]
    NoExitNode,

    /// A node handler returned an error.
    #[error("handler error for node '{node_id}': {message}")]
    Handler { node_id: String, message: String },

    /// A goal gate node did not reach SUCCESS before the pipeline attempted to exit.
    #[error("goal gate unsatisfied: node '{0}'")]
    GoalGateUnsatisfied(String),

    /// A failed node has no outgoing fail edge and no retry target.
    #[error("no outgoing fail edge from node '{0}'")]
    NoFailEdge(String),

    /// A configured `retry_target` or `fallback_retry_target` does not exist in the graph.
    #[error("retry target not found: '{0}'")]
    RetryTargetNotFound(String),

    /// An error occurred while saving or loading a checkpoint.
    #[error("checkpoint error: {0}")]
    Checkpoint(String),

    /// An error occurred while accessing the artifact store.
    #[error("artifact error: {0}")]
    Artifact(String),

    /// An I/O error occurred.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// The codergen backend returned an error.
    #[error("backend error: {0}")]
    Backend(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_error_syntax_display() {
        let e = ParseError::Syntax {
            position: 42,
            message: "unexpected token".to_string(),
        };
        assert_eq!(
            format!("{e}"),
            "DOT syntax error at position 42: unexpected token"
        );
    }

    #[test]
    fn parse_error_invalid_attribute_display() {
        let e = ParseError::InvalidAttribute {
            key: "max_retries".to_string(),
            message: "expected integer".to_string(),
        };
        assert_eq!(
            format!("{e}"),
            "invalid attribute value for 'max_retries': expected integer"
        );
    }

    #[test]
    fn parse_error_condition_syntax_display() {
        let e = ParseError::ConditionSyntax("bad expr".to_string());
        assert_eq!(format!("{e}"), "condition syntax error: bad expr");
    }

    #[test]
    fn validation_error_failed_display() {
        let e = ValidationError::Failed { count: 3 };
        assert_eq!(format!("{e}"), "validation failed with 3 error(s)");
    }

    #[test]
    fn engine_error_from_parse_error() {
        let parse_err = ParseError::Syntax {
            position: 0,
            message: "oops".into(),
        };
        let engine_err: EngineError = parse_err.into();
        assert!(matches!(engine_err, EngineError::Parse(_)));
    }

    #[test]
    fn engine_error_from_validation_error() {
        let val_err = ValidationError::Failed { count: 1 };
        let engine_err: EngineError = val_err.into();
        assert!(matches!(engine_err, EngineError::Validation(_)));
    }

    #[test]
    fn engine_error_from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let engine_err: EngineError = io_err.into();
        assert!(matches!(engine_err, EngineError::Io(_)));
    }
}
