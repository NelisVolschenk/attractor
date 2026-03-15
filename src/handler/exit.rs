//! Exit handler — no-op for `shape=Msquare` pipeline exit nodes.

use async_trait::async_trait;
use std::path::Path;

use crate::error::EngineError;
use crate::graph::{Graph, Node};
use crate::handler::Handler;
use crate::state::context::{Context, Outcome};

/// No-op handler for `shape=Msquare` (pipeline exit) nodes.
///
/// Returns [`Outcome::success`] immediately.  Goal gate enforcement is the
/// engine's responsibility, not this handler's.
pub struct ExitHandler;

#[async_trait]
impl Handler for ExitHandler {
    async fn execute(
        &self,
        _node: &Node,
        _context: &Context,
        _graph: &Graph,
        _logs_root: &Path,
    ) -> Result<Outcome, EngineError> {
        Ok(Outcome::success())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::Graph;
    use crate::state::context::StageStatus;

    #[tokio::test]
    async fn returns_success() {
        let handler = ExitHandler;
        let node = Node::default();
        let ctx = Context::new();
        let graph = Graph::new("test".into());
        let result = handler
            .execute(&node, &ctx, &graph, Path::new("/tmp"))
            .await
            .unwrap();
        assert_eq!(result.status, StageStatus::Success);
    }
}
