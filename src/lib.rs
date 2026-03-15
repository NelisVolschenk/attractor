//! DOT-based AI pipeline runner.
//!
//! Executes directed-graph workflows where each node is handled by an AI agent
//! or built-in handler.
//!
//! ## Epoch 8 — Execution Engine
//!
//! - **[`engine`]** — `PipelineRunner`, `RunConfig`, `RunResult`
//! - **[`transform`]** — `Transform` trait, `VariableExpansionTransform`, `StylesheetApplicationTransform`
//! - **[`testing`]** — `MockCodergenBackend` test utility
//!
//! ## Epoch 7 — State Management + Handler Infrastructure
//!
//! - **[`state`]** — `Context`, `Outcome`, `StageStatus`, `Checkpoint`, `ArtifactStore`, `FidelityMode`
//! - **[`handler`]** — `Handler` trait, `HandlerRegistry`, all built-in handlers
//! - **[`interviewer`]** — `Interviewer` trait, all built-in implementations
//! - **[`events`]** — `PipelineEvent` enum + broadcast channel
//!
//! ## Epoch 6 — DOT Parser + Validator
//!
//! - **[`error`]** — `ParseError`, `ValidationError`, `EngineError`
//! - **[`graph`]** — `Graph`, `Node`, `Edge`, `Value`, `GraphAttrs`
//! - **[`parser`]** — `parse_dot()` entry point + lexer + value parsers
//! - **[`condition`]** — condition expression parser + evaluator
//! - **[`stylesheet`]** — model stylesheet parser + applicator
//! - **[`validation`]** — built-in lint rules, `validate()`, `validate_or_raise()`

// Core types
pub mod error;
pub mod graph;

// DOT parser (F-202, F-203, F-204, F-205)
pub mod parser;

// Condition expression language (F-206)
pub mod condition;

// Model stylesheet (F-207)
pub mod stylesheet;

// Validation / linting (F-208)
pub mod validation;

// State management (F-209, F-210, F-211, F-230)
pub mod state;

// Handler trait + registry + built-in handlers (F-212 through F-217, F-225, F-226, F-227, F-228)
pub mod handler;

// Human-in-the-loop (F-216, F-229)
pub mod interviewer;

// Pipeline events (F-221)
pub mod events;

// AST transforms (F-222)
pub mod transform;

// Execution engine (F-218, F-219, F-220, F-223, F-224)
pub mod engine;

// Test utilities (F-231)
pub mod testing;

// ---------------------------------------------------------------------------
// Re-exports — public API surface
// ---------------------------------------------------------------------------

// Error types
pub use error::{EngineError, ParseError, ValidationError};

// Graph model
pub use graph::{Edge, Graph, GraphAttrs, Node, Value};

// Parser entry point
pub use parser::parse_dot;

// Condition expression language
pub use condition::{
    Clause, ConditionExpr, ConditionKey, ConditionOp, evaluate_condition, parse_condition,
};

// Stylesheet
pub use stylesheet::{
    Declaration, Selector, StyleProperty, StyleRule, Stylesheet, apply_stylesheet, parse_stylesheet,
};

// Validation
pub use validation::{Diagnostic, LintRule, Severity, validate, validate_or_raise};

// State management
pub use state::{
    ArtifactInfo, ArtifactStore, Checkpoint, Context, FidelityMode, Outcome, StageStatus,
    resolve_fidelity, resolve_thread_id,
};

// Handler system
pub use handler::{
    BranchResult, CodergenBackend, CodergenHandler, CodergenResult, ConditionalHandler,
    ErrorPolicy, ExitHandler, FanInHandler, Handler, HandlerRegistry, JoinPolicy,
    ManagerLoopHandler, ParallelHandler, StartHandler, ToolHandler, WaitForHumanHandler,
    shape_to_handler_type,
};

// Interviewer
pub use interviewer::{
    Answer, AnswerValue, AutoApproveInterviewer, CallbackInterviewer, Interviewer, Question,
    QuestionOption, QuestionType, QueueInterviewer, Recording, RecordingInterviewer,
};

// Events
pub use events::{EVENT_CHANNEL_CAPACITY, PipelineEvent, event_channel};

// Transforms
pub use transform::{
    StylesheetApplicationTransform, Transform, VariableExpansionTransform, apply_transforms,
};

// Engine
pub use engine::{
    BackoffConfig, PipelineRunner, PipelineRunnerBuilder, RetryPolicy, RunConfig, RunResult,
    check_goal_gates, normalize_label, resolve_gate_retry_target, select_edge,
};

// Test utilities
pub use testing::MockCodergenBackend;
