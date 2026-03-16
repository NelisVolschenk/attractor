//! DOT parser — entry point and grammar combinators.
//!
//! Public API: [`parse_dot`].
//!
//! Internally the parser:
//! 1. Pre-processes the source with [`lexer::strip_comments`].
//! 2. Applies hand-written recursive-descent combinators that build a [`Graph`].
//!
//! Implements the BNF from NLSpec §2.2, including chained edges (F-205),
//! node/edge default blocks (F-205), and subgraph flattening (F-205).

pub mod lexer;
pub mod values;

use crate::error::ParseError;
use crate::graph::{Edge, Graph, GraphAttrs, Node, Value};
use lexer::*;
use nom::{IResult, character::complete::multispace0};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Opt helper — hand-rolled to avoid nom's opt() combinator (impl Parser, non-callable)
// ---------------------------------------------------------------------------

/// Try a parser; return `(input, Some(val))` on success or `(input, None)` on failure.
fn try_parse<'a, T, F>(input: &'a str, parser: F) -> (&'a str, Option<T>)
where
    F: Fn(&'a str) -> IResult<&'a str, T>,
{
    match parser(input) {
        Ok((rest, val)) => (rest, Some(val)),
        Err(_) => (input, None),
    }
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Parse a DOT source string into a [`Graph`].
///
/// Comments are stripped first, then hand-written grammar combinators build the model.
/// Returns [`ParseError::Syntax`] on malformed input.
pub fn parse_dot(source: &str) -> Result<Graph, ParseError> {
    let cleaned = lexer::strip_comments(source);
    let input = cleaned.trim();

    match parse_graph(input) {
        Ok((remaining, graph)) => {
            let remaining = remaining.trim();
            if !remaining.is_empty() {
                Err(ParseError::Syntax {
                    position: cleaned.len().saturating_sub(remaining.len()),
                    message: format!(
                        "unexpected trailing content: '{}'",
                        &remaining[..remaining.len().min(40)]
                    ),
                })
            } else {
                Ok(graph)
            }
        }
        Err(e) => Err(ParseError::Syntax {
            position: 0,
            message: format!("parse error: {e}"),
        }),
    }
}

// ---------------------------------------------------------------------------
// Graph parser
// ---------------------------------------------------------------------------

/// Parse `digraph Name? '{' Statement* '}'`
pub(crate) fn parse_graph(input: &str) -> IResult<&str, Graph> {
    let (input, _) = multispace0(input)?;
    let (input, _) = kw_digraph(input)?;
    let (input, _) = multispace0(input)?;
    // Optional graph name — try parse_identifier_or_string; fall back to None
    let (input, name) = try_parse(input, parse_identifier_or_string);
    let (input, _) = multispace0(input)?;
    let (input, _) = lbrace(input)?;

    let mut graph = Graph::new(name.unwrap_or_default());
    let (input, _) = parse_statements(input, &mut graph)?;

    let (input, _) = multispace0(input)?;
    let (input, _) = rbrace(input)?;
    Ok((input, graph))
}

// ---------------------------------------------------------------------------
// Statement dispatcher
// ---------------------------------------------------------------------------

fn parse_statements<'a>(input: &'a str, graph: &mut Graph) -> IResult<&'a str, ()> {
    let mut input = input;
    loop {
        let (new_input, _) = multispace0(input)?;
        input = new_input;
        if input.starts_with('}') || input.is_empty() {
            break;
        }
        match parse_one_statement(input, graph) {
            Ok((new_input, _)) => input = new_input,
            Err(_) => break,
        }
    }
    Ok((input, ()))
}

fn parse_one_statement<'a>(input: &'a str, graph: &mut Graph) -> IResult<&'a str, ()> {
    // Try: subgraph block (keyword or bare `{`)
    if input.starts_with("subgraph") || input.starts_with('{') {
        let nd = graph.node_defaults.clone();
        let ed = graph.edge_defaults.clone();
        let (rest, result) = parse_subgraph_block(input, &nd, &ed)?;
        for node in result.nodes {
            graph.nodes.insert(node.id.clone(), node);
        }
        for edge in result.edges {
            graph.edges.push(edge);
        }
        return Ok((rest, ()));
    }

    // Try: `graph [ ... ]` attribute block
    if let Some(peek_rest) = input.strip_prefix("graph") {
        let (pr, _) = multispace0(peek_rest)?;
        if pr.starts_with('[') {
            let (rest, attrs) = parse_graph_kw_block(input)?;
            let new_ga = GraphAttrs::from_attrs(attrs);
            let ga = &mut graph.graph_attrs;
            if !new_ga.goal.is_empty() {
                ga.goal = new_ga.goal;
            }
            if !new_ga.label.is_empty() {
                ga.label = new_ga.label;
            }
            if !new_ga.model_stylesheet.is_empty() {
                ga.model_stylesheet = new_ga.model_stylesheet;
            }
            if !new_ga.retry_target.is_empty() {
                ga.retry_target = new_ga.retry_target;
            }
            if !new_ga.fallback_retry_target.is_empty() {
                ga.fallback_retry_target = new_ga.fallback_retry_target;
            }
            if !new_ga.default_fidelity.is_empty() {
                ga.default_fidelity = new_ga.default_fidelity;
            }
            // Always merge default_max_retry so `graph [default_max_retry=0]` works.
            // GraphAttrs::from_attrs initialises this to 50, which is the same as
            // Graph::new()'s default, so merging a 50 is a no-op.
            ga.default_max_retry = new_ga.default_max_retry;
            ga.extra.extend(new_ga.extra);
            return Ok((rest, ()));
        }
    }

    // Try: `node [ ... ]` default block
    if let Some(peek_rest) = input.strip_prefix("node") {
        let (pr, _) = multispace0(peek_rest)?;
        if pr.starts_with('[') {
            let (rest, attrs) = parse_node_defaults_block(input)?;
            graph.node_defaults.extend(attrs);
            return Ok((rest, ()));
        }
    }

    // Try: `edge [ ... ]` default block
    if let Some(peek_rest) = input.strip_prefix("edge") {
        let (pr, _) = multispace0(peek_rest)?;
        if pr.starts_with('[') {
            let (rest, attrs) = parse_edge_defaults_block(input)?;
            graph.edge_defaults.extend(attrs);
            return Ok((rest, ()));
        }
    }

    // Try to parse an identifier or quoted string (node ID or LHS of bare attr)
    if let Ok((after_id, _node_id)) = parse_identifier_or_string(input) {
        let (after_ws, _) = multispace0(after_id)?;

        // Bare `key = value ;?` at graph level
        if after_ws.starts_with('=') {
            let (rest, (key, val)) = parse_bare_graph_attr(input)?;
            let mut attrs = HashMap::new();
            attrs.insert(key, val);
            let new_ga = GraphAttrs::from_attrs(attrs);
            let ga = &mut graph.graph_attrs;
            if !new_ga.goal.is_empty() {
                ga.goal = new_ga.goal;
            }
            if !new_ga.label.is_empty() {
                ga.label = new_ga.label;
            }
            if !new_ga.model_stylesheet.is_empty() {
                ga.model_stylesheet = new_ga.model_stylesheet;
            }
            if !new_ga.retry_target.is_empty() {
                ga.retry_target = new_ga.retry_target;
            }
            if !new_ga.fallback_retry_target.is_empty() {
                ga.fallback_retry_target = new_ga.fallback_retry_target;
            }
            if !new_ga.default_fidelity.is_empty() {
                ga.default_fidelity = new_ga.default_fidelity;
            }
            ga.extra.extend(new_ga.extra);
            return Ok((rest, ()));
        }

        // Check for edge statement: identifier -> ...
        if after_ws.starts_with("->") {
            let ed = graph.edge_defaults.clone();
            let (rest, edges) = parse_edge_stmt(input, &ed)?;
            // Ensure all referenced nodes exist (implicit node creation)
            for edge in &edges {
                if !graph.nodes.contains_key(&edge.from) {
                    let n = build_node_from_defaults(&edge.from, &graph.node_defaults);
                    graph.nodes.insert(edge.from.clone(), n);
                }
                if !graph.nodes.contains_key(&edge.to) {
                    let n = build_node_from_defaults(&edge.to, &graph.node_defaults);
                    graph.nodes.insert(edge.to.clone(), n);
                }
            }
            for edge in edges {
                graph.edges.push(edge);
            }
            return Ok((rest, ()));
        }

        // Otherwise it's a node statement
        let nd = graph.node_defaults.clone();
        let (rest, node) = parse_node_stmt(input, &nd)?;
        graph.nodes.insert(node.id.clone(), node);
        return Ok((rest, ()));
    }

    Err(nom::Err::Error(nom::error::Error::new(
        input,
        nom::error::ErrorKind::Alt,
    )))
}

// ---------------------------------------------------------------------------
// Attribute block parser
// ---------------------------------------------------------------------------

/// Parse `[ attr (, attr)* ]`
pub(crate) fn parse_attr_block(input: &str) -> IResult<&str, HashMap<String, Value>> {
    let (input, _) = multispace0(input)?;
    let (input, _) = lbracket(input)?;
    let (input, _) = multispace0(input)?;
    let (input, attrs) = parse_attr_list(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = rbracket(input)?;
    Ok((input, attrs))
}

fn parse_attr_list(input: &str) -> IResult<&str, HashMap<String, Value>> {
    let mut map = HashMap::new();
    let mut input = input;
    loop {
        let (ni, _) = multispace0(input)?;
        if ni.starts_with(']') || ni.is_empty() {
            return Ok((ni, map));
        }
        let (ni, (key, val)) = parse_attr(ni)?;
        map.insert(key, val);
        let (ni, _) = multispace0(ni)?;
        // Optional comma separator
        if let Some(ni2) = ni.strip_prefix(',') {
            input = ni2;
        } else {
            input = ni;
        }
    }
}

/// Parse a single `key = value` pair.
pub(crate) fn parse_attr(input: &str) -> IResult<&str, (String, Value)> {
    let (input, key) = parse_key(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = eq_sign(input)?;
    let (input, _) = multispace0(input)?;
    let (input, val) = parse_attr_value(input)?;
    Ok((input, (key, val)))
}

/// Parse an attribute key: may be a qualified identifier (with dots) or bare identifier.
fn parse_key(input: &str) -> IResult<&str, String> {
    let (input, first) = parse_raw_identifier(input)?;
    let mut key = first.to_string();
    let mut rest = input;
    loop {
        if rest.starts_with('.') {
            let after_dot = &rest[1..];
            match parse_raw_identifier(after_dot) {
                Ok((new_rest, seg)) => {
                    key.push('.');
                    key.push_str(seg);
                    rest = new_rest;
                }
                Err(_) => break,
            }
        } else {
            break;
        }
    }
    Ok((rest, key))
}

/// Parse an attribute value: quoted string, typed value, or bare identifier.
fn parse_attr_value(input: &str) -> IResult<&str, Value> {
    if input.starts_with('"') {
        return values::parse_value(input);
    }
    if let Ok(r) = values::parse_value(input) {
        return Ok(r);
    }
    parse_bare_value(input)
}

/// Parse a bare (unquoted) value — anything up to `,`, `]`, `;`, whitespace, `{`, `}`.
fn parse_bare_value(input: &str) -> IResult<&str, Value> {
    let end = input
        .find(|c: char| matches!(c, ',' | ']' | ';' | '{' | '}') || c.is_whitespace())
        .unwrap_or(input.len());
    if end == 0 {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Tag,
        )));
    }
    Ok((&input[end..], Value::Str(input[..end].to_string())))
}

// ---------------------------------------------------------------------------
// Specific block parsers
// ---------------------------------------------------------------------------

fn parse_graph_kw_block(input: &str) -> IResult<&str, HashMap<String, Value>> {
    let (input, _) = kw_graph(input)?;
    let (input, _) = multispace0(input)?;
    let (input, attrs) = parse_attr_block(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = opt_semi(input)?;
    Ok((input, attrs))
}

fn parse_node_defaults_block(input: &str) -> IResult<&str, HashMap<String, Value>> {
    let (input, _) = kw_node(input)?;
    let (input, _) = multispace0(input)?;
    let (input, attrs) = parse_attr_block(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = opt_semi(input)?;
    Ok((input, attrs))
}

fn parse_edge_defaults_block(input: &str) -> IResult<&str, HashMap<String, Value>> {
    let (input, _) = kw_edge(input)?;
    let (input, _) = multispace0(input)?;
    let (input, attrs) = parse_attr_block(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = opt_semi(input)?;
    Ok((input, attrs))
}

fn parse_bare_graph_attr(input: &str) -> IResult<&str, (String, Value)> {
    let (input, key) = parse_key(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = eq_sign(input)?;
    let (input, _) = multispace0(input)?;
    let (input, val) = parse_attr_value(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = opt_semi(input)?;
    Ok((input, (key, val)))
}

// ---------------------------------------------------------------------------
// Node statement
// ---------------------------------------------------------------------------

/// Parse `Identifier AttrBlock? ;?`
pub(crate) fn parse_node_stmt<'a>(
    input: &'a str,
    defaults: &HashMap<String, Value>,
) -> IResult<&'a str, Node> {
    let (input, id) = parse_identifier_or_string(input)?;
    let (input, _) = multispace0(input)?;
    let (input, attrs) = if input.starts_with('[') {
        parse_attr_block(input)?
    } else {
        (input, HashMap::new())
    };
    let (input, _) = multispace0(input)?;
    let (input, _) = opt_semi(input)?;

    let mut node = build_node_from_defaults(&id, defaults);
    node.apply_attrs(attrs);
    if node.label.is_empty() {
        node.label = id.clone();
    }
    Ok((input, node))
}

pub(crate) fn build_node_from_defaults(id: &str, defaults: &HashMap<String, Value>) -> Node {
    let mut node = Node {
        id: id.to_string(),
        label: id.to_string(),
        ..Default::default()
    };
    node.apply_attrs(defaults.clone());
    node
}

// ---------------------------------------------------------------------------
// Edge statement (F-204 + F-205: chained edges)
// ---------------------------------------------------------------------------

/// Parse `Identifier ( '->' Identifier )+ AttrBlock? ;?`
///
/// Chained edges like `A -> B -> C [label="x"]` expand to two edges, each
/// sharing the attribute block (F-205 NLSpec §2.9).
pub(crate) fn parse_edge_stmt<'a>(
    input: &'a str,
    defaults: &HashMap<String, Value>,
) -> IResult<&'a str, Vec<Edge>> {
    let (input, first_id) = parse_identifier_or_string(input)?;
    let (input, _) = multispace0(input)?;

    // Collect all node IDs in the chain
    let mut ids = vec![first_id];
    let mut input = input;
    loop {
        let (ni, _) = multispace0(input)?;
        if !ni.starts_with("->") {
            input = ni;
            break;
        }
        let ni = &ni[2..]; // consume "->"
        let (ni, _) = multispace0(ni)?;
        let (ni, next_id) = parse_identifier_or_string(ni)?;
        ids.push(next_id);
        input = ni;
    }

    if ids.len() < 2 {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Tag,
        )));
    }

    // Optional attribute block
    let (input, attrs) = if input.starts_with('[') {
        parse_attr_block(input)?
    } else {
        (input, HashMap::new())
    };
    let (input, _) = multispace0(input)?;
    let (input, _) = opt_semi(input)?;

    // Expand chain: A->B->C becomes (A->B) and (B->C), each with shared attrs
    let mut edges = Vec::new();
    for window in ids.windows(2) {
        let from = window[0].clone();
        let to = window[1].clone();
        let mut edge = Edge::default();
        edge.apply_attrs(defaults);
        edge.apply_attrs(&attrs);
        edge.from = from;
        edge.to = to;
        edges.push(edge);
    }
    Ok((input, edges))
}

// ---------------------------------------------------------------------------
// Subgraph statement (F-205)
// ---------------------------------------------------------------------------

pub(crate) struct SubgraphResult {
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
}

/// Derive a CSS class name from a subgraph label.
///
/// Lowercases, replaces spaces with hyphens, strips non-alphanumeric-non-hyphen.
pub(crate) fn derive_class(label: &str) -> String {
    label
        .to_lowercase()
        .chars()
        .filter_map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                Some(c)
            } else if c == ' ' {
                Some('-')
            } else {
                None
            }
        })
        .collect()
}

pub(crate) fn parse_subgraph_block<'a>(
    input: &'a str,
    parent_node_defaults: &HashMap<String, Value>,
    parent_edge_defaults: &HashMap<String, Value>,
) -> IResult<&'a str, SubgraphResult> {
    let (input, _) = multispace0(input)?;

    // Optional `subgraph` keyword
    let (input, has_kw) = if let Some(after) = input.strip_prefix("subgraph") {
        // Verify word boundary
        if after
            .chars()
            .next()
            .is_none_or(|c| !c.is_ascii_alphanumeric() && c != '_')
        {
            (after, true)
        } else {
            (input, false)
        }
    } else {
        (input, false)
    };

    let (input, _) = multispace0(input)?;

    // Optional subgraph name (only if the `subgraph` keyword was present)
    let (input, _sg_name) = if has_kw {
        try_parse(input, parse_identifier_or_string)
    } else {
        (input, None)
    };

    let (input, _) = multispace0(input)?;
    let (input, _) = lbrace(input)?;

    // Inner graph to accumulate subgraph contents
    let mut inner = Graph::new("_subgraph".to_string());
    inner.node_defaults = parent_node_defaults.clone();
    inner.edge_defaults = parent_edge_defaults.clone();

    let (input, _) = parse_statements(input, &mut inner)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = rbrace(input)?;

    // Derive class from subgraph label
    let class = if !inner.graph_attrs.label.is_empty() {
        let c = derive_class(&inner.graph_attrs.label);
        if c.is_empty() { None } else { Some(c) }
    } else {
        None
    };

    // Apply derived class to all nodes in subgraph
    let mut nodes: Vec<Node> = inner.nodes.into_values().collect();
    if let Some(ref cls) = class {
        for node in &mut nodes {
            if node.class.is_empty() {
                node.class = cls.clone();
            } else if !node.class.split(',').any(|c| c.trim() == cls.as_str()) {
                node.class.push(',');
                node.class.push_str(cls);
            }
        }
    }

    Ok((
        input,
        SubgraphResult {
            nodes,
            edges: inner.edges,
        },
    ))
}

// ---------------------------------------------------------------------------
// Identifier helpers
// ---------------------------------------------------------------------------

/// Parse a bare identifier or a quoted string (returns owned String).
pub(crate) fn parse_identifier_or_string(input: &str) -> IResult<&str, String> {
    if input.starts_with('"') {
        let (rest, v) = values::parse_string(input)?;
        if let Value::Str(s) = v {
            return Ok((rest, s));
        }
    }
    let (rest, id) = parse_raw_identifier(input)?;
    Ok((rest, id.to_string()))
}

/// Match a raw identifier as a string slice.
pub(crate) fn parse_raw_identifier(input: &str) -> IResult<&str, &str> {
    if input.is_empty() {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Alpha,
        )));
    }
    let first = input.chars().next().unwrap();
    if !first.is_ascii_alphabetic() && first != '_' {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Alpha,
        )));
    }
    let n = input
        .find(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .unwrap_or(input.len());
    Ok((&input[n..], &input[..n]))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- NLSpec §2.13 examples ---

    #[test]
    fn parse_simple_linear_workflow() {
        let dot = r#"
digraph Simple {
    graph [goal="Run tests and report"]
    rankdir=LR

    start [shape=Mdiamond, label="Start"]
    exit  [shape=Msquare, label="Exit"]

    run_tests [label="Run Tests", prompt="Run the test suite and report results"]
    report    [label="Report", prompt="Summarize the test results"]

    start -> run_tests -> report -> exit
}
"#;
        let g = parse_dot(dot).expect("parse should succeed");
        assert_eq!(g.graph_attrs.goal, "Run tests and report");
        assert!(g.nodes.contains_key("start"));
        assert!(g.nodes.contains_key("exit"));
        assert!(g.nodes.contains_key("run_tests"));
        assert!(g.nodes.contains_key("report"));
        assert_eq!(g.nodes["start"].shape, "Mdiamond");
        assert_eq!(g.nodes["exit"].shape, "Msquare");
        // 3 edges: start->run_tests, run_tests->report, report->exit
        assert_eq!(g.edges.len(), 3);
    }

    #[test]
    fn parse_branching_workflow() {
        let dot = r#"
digraph Branch {
    graph [goal="Implement and validate a feature"]
    rankdir=LR
    node [shape=box, timeout="900s"]

    start     [shape=Mdiamond, label="Start"]
    exit      [shape=Msquare, label="Exit"]
    plan      [label="Plan", prompt="Plan the implementation"]
    implement [label="Implement", prompt="Implement the plan"]
    validate  [label="Validate", prompt="Run tests"]
    gate      [shape=diamond, label="Tests passing?"]

    start -> plan -> implement -> validate -> gate
    gate -> exit      [label="Yes", condition="outcome=success"]
    gate -> implement [label="No", condition="outcome!=success"]
}
"#;
        let g = parse_dot(dot).expect("parse should succeed");
        assert_eq!(g.nodes.len(), 6);
        // 4 edges from chain + 2 explicit = 6
        assert_eq!(g.edges.len(), 6);
        let yes_edge = g.edges.iter().find(|e| e.label == "Yes").unwrap();
        assert_eq!(yes_edge.condition, "outcome=success");
        // node defaults: shape=box applied
        assert_eq!(g.nodes["plan"].shape, "box");
        // start/exit/gate override shape explicitly
        assert_eq!(g.nodes["start"].shape, "Mdiamond");
        assert_eq!(g.nodes["gate"].shape, "diamond");
    }

    #[test]
    fn parse_human_gate_workflow() {
        let dot = r#"
digraph Review {
    rankdir=LR

    start [shape=Mdiamond, label="Start"]
    exit  [shape=Msquare, label="Exit"]

    review_gate [
        shape=hexagon,
        label="Review Changes",
        type="wait.human"
    ]

    start -> review_gate
    review_gate -> ship_it [label="[A] Approve"]
    review_gate -> fixes   [label="[F] Fix"]
    ship_it -> exit
    fixes -> review_gate
}
"#;
        let g = parse_dot(dot).expect("parse should succeed");
        assert!(g.nodes.contains_key("review_gate"));
        assert_eq!(g.nodes["review_gate"].shape, "hexagon");
        assert_eq!(g.nodes["review_gate"].node_type, "wait.human");
        assert_eq!(g.edges.len(), 5);
    }

    #[test]
    fn parse_empty_graph() {
        let g = parse_dot("digraph {}").expect("empty graph");
        assert_eq!(g.name, "");
        assert!(g.nodes.is_empty());
        assert!(g.edges.is_empty());
    }

    #[test]
    fn parse_named_graph() {
        let g = parse_dot("digraph MyGraph {}").expect("named graph");
        assert_eq!(g.name, "MyGraph");
    }

    #[test]
    fn parse_chained_edges() {
        let dot = r#"digraph {
            start [shape=Mdiamond]
            A B C
            exit [shape=Msquare]
            start -> A -> B -> C -> exit [label="next"]
        }"#;
        let g = parse_dot(dot).expect("chained edges");
        assert_eq!(g.edges.len(), 4);
        assert!(g.edges.iter().all(|e| e.label == "next"));
    }

    #[test]
    fn parse_node_defaults_applied() {
        let dot = r#"
digraph {
    node [shape=box, reasoning_effort="low"]
    start [shape=Mdiamond]
    A
    exit [shape=Msquare]
    start -> A -> exit
}
"#;
        let g = parse_dot(dot).expect("node defaults");
        assert_eq!(g.nodes["A"].shape, "box");
        assert_eq!(g.nodes["A"].reasoning_effort, "low");
        assert_eq!(g.nodes["start"].shape, "Mdiamond");
    }

    #[test]
    fn parse_graph_attrs() {
        let dot = r#"
digraph {
    graph [goal="test goal", default_max_retry=10]
}
"#;
        let g = parse_dot(dot).expect("graph attrs");
        assert_eq!(g.graph_attrs.goal, "test goal");
    }

    #[test]
    fn parse_comments_stripped() {
        let dot = r#"
// This is a comment
digraph {
    /* block comment */
    start [shape=Mdiamond] // inline comment
    exit  [shape=Msquare]
    start -> exit
}
"#;
        let g = parse_dot(dot).expect("comments");
        assert_eq!(g.nodes.len(), 2);
    }

    #[test]
    fn parse_subgraph_flattened() {
        let dot = r#"
digraph {
    start [shape=Mdiamond]
    exit  [shape=Msquare]
    subgraph cluster_loop {
        label = "Loop A"
        node [thread_id="loop-a"]
        Plan [label="Plan next step"]
        Implement [label="Implement"]
    }
    start -> Plan -> Implement -> exit
}
"#;
        let g = parse_dot(dot).expect("subgraph");
        assert!(g.nodes.contains_key("Plan"));
        assert!(g.nodes.contains_key("Implement"));
    }

    #[test]
    fn derive_class_from_label() {
        assert_eq!(derive_class("Loop A"), "loop-a");
        assert_eq!(derive_class("My Pipeline"), "my-pipeline");
        assert_eq!(derive_class("Loop #1"), "loop-1");
        assert_eq!(derive_class(""), "");
    }

    #[test]
    fn parse_node_unknown_attrs_in_extra() {
        let dot = r#"digraph { A [shape=box, custom_attr="hello"] }"#;
        let g = parse_dot(dot).expect("extra attrs");
        let node = &g.nodes["A"];
        assert!(node.extra.contains_key("custom_attr"));
    }

    #[test]
    fn parse_edge_weight() {
        let dot = r#"digraph { A [shape=Mdiamond] B [shape=Msquare] A -> B [weight=5] }"#;
        let g = parse_dot(dot).expect("edge weight");
        assert_eq!(g.edges[0].weight, 5);
    }

    // --- GAP-ATR-001 / GAP-ATR-017: multi-line attribute blocks ---

    #[test]
    fn parse_multiline_attribute_block() {
        // NLSpec §11.1 + §11.12 parity matrix: attributes may span multiple lines
        // inside a `[...]` block — the parser must handle embedded newlines.
        let dot = r#"
digraph {
    start [shape=Mdiamond]
    exit  [shape=Msquare]
    review [
        shape=hexagon,
        label="Review Changes",
        type="wait.human",
        prompt="Please review the code changes"
    ]
    start -> review -> exit
}
"#;
        let g = parse_dot(dot).expect("multi-line attr block should parse");
        let node = &g.nodes["review"];
        assert_eq!(node.shape, "hexagon");
        assert_eq!(node.label, "Review Changes");
        assert_eq!(node.node_type, "wait.human");
        assert_eq!(node.prompt, "Please review the code changes");
    }
}
