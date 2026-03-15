//! DOT lexer: comment stripping and low-level token parsers.
//!
//! `strip_comments` performs a pre-pass over the source to remove `//` and
//! `/* */` comments while preserving quoted strings intact. The remaining
//! functions are direct parser functions (no nom combinators) used by the
//! grammar layer.

use nom::{IResult, character::complete::multispace0};

// ---------------------------------------------------------------------------
// Comment stripping
// ---------------------------------------------------------------------------

/// Strip `//` line comments and `/* */` block comments from `input`.
///
/// Characters inside double-quoted strings are never treated as comment
/// starters. Block comments are not nested.
pub fn strip_comments(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let n = bytes.len();
    let mut i = 0;
    let mut in_string = false;

    while i < n {
        if in_string {
            // Inside a quoted string: pass characters through verbatim,
            // handling backslash escapes so \" doesn't end the string.
            if bytes[i] == b'\\' && i + 1 < n {
                out.push(bytes[i] as char);
                out.push(bytes[i + 1] as char);
                i += 2;
            } else if bytes[i] == b'"' {
                out.push('"');
                in_string = false;
                i += 1;
            } else {
                out.push(bytes[i] as char);
                i += 1;
            }
        } else {
            // Outside a string: detect comment starters.
            if i + 1 < n && bytes[i] == b'/' && bytes[i + 1] == b'/' {
                // Line comment: skip to end of line.
                i += 2;
                while i < n && bytes[i] != b'\n' {
                    i += 1;
                }
                // The newline itself is kept so line numbers are preserved.
            } else if i + 1 < n && bytes[i] == b'/' && bytes[i + 1] == b'*' {
                // Block comment: skip to first `*/`.
                i += 2;
                while i + 1 < n && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    // Preserve newlines for line-number tracking.
                    if bytes[i] == b'\n' {
                        out.push('\n');
                    }
                    i += 1;
                }
                if i + 1 < n {
                    i += 2; // consume `*/`
                }
            } else if bytes[i] == b'"' {
                out.push('"');
                in_string = true;
                i += 1;
            } else {
                out.push(bytes[i] as char);
                i += 1;
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Whitespace helpers
// ---------------------------------------------------------------------------

/// Consume zero or more whitespace characters. Always succeeds.
pub fn ws(input: &str) -> IResult<&str, ()> {
    let (rest, _) = multispace0(input)?;
    Ok((rest, ()))
}

// ---------------------------------------------------------------------------
// Token recognizers — all written as direct string-operating functions
// ---------------------------------------------------------------------------

/// Match a bare DOT identifier: `[A-Za-z_][A-Za-z0-9_]*`.
pub fn identifier(input: &str) -> IResult<&str, &str> {
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

/// Match the `digraph` keyword (case-sensitive, word-boundary enforced).
pub fn kw_digraph(input: &str) -> IResult<&str, &str> {
    keyword("digraph")(input)
}

/// Match the `graph` keyword.
pub fn kw_graph(input: &str) -> IResult<&str, &str> {
    keyword("graph")(input)
}

/// Match the `node` keyword.
pub fn kw_node(input: &str) -> IResult<&str, &str> {
    keyword("node")(input)
}

/// Match the `edge` keyword.
pub fn kw_edge(input: &str) -> IResult<&str, &str> {
    keyword("edge")(input)
}

/// Match the `subgraph` keyword.
pub fn kw_subgraph(input: &str) -> IResult<&str, &str> {
    keyword("subgraph")(input)
}

/// Match the `->` edge operator.
pub fn arrow(input: &str) -> IResult<&str, &str> {
    if let Some(rest) = input.strip_prefix("->") {
        Ok((rest, "->"))
    } else {
        Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Tag,
        )))
    }
}

/// Match a single `{`.
pub fn lbrace(input: &str) -> IResult<&str, char> {
    match_char(input, '{')
}

/// Match a single `}`.
pub fn rbrace(input: &str) -> IResult<&str, char> {
    match_char(input, '}')
}

/// Match a single `[`.
pub fn lbracket(input: &str) -> IResult<&str, char> {
    match_char(input, '[')
}

/// Match a single `]`.
pub fn rbracket(input: &str) -> IResult<&str, char> {
    match_char(input, ']')
}

/// Match a single `=`.
pub fn eq_sign(input: &str) -> IResult<&str, char> {
    match_char(input, '=')
}

/// Match a single `,`.
pub fn comma(input: &str) -> IResult<&str, char> {
    match_char(input, ',')
}

/// Match an optional `;`.
pub fn opt_semi(input: &str) -> IResult<&str, Option<char>> {
    if let Some(rest) = input.strip_prefix(';') {
        Ok((rest, Some(';')))
    } else {
        Ok((input, None))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn match_char(input: &str, c: char) -> IResult<&str, char> {
    if input.starts_with(c) {
        let n = c.len_utf8();
        Ok((&input[n..], c))
    } else {
        Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Char,
        )))
    }
}

/// Build a keyword parser: matches `kw` exactly and requires that the
/// next character is NOT alphanumeric or `_` (word-boundary check).
fn keyword(kw: &'static str) -> impl Fn(&str) -> IResult<&str, &str> {
    move |input: &str| {
        if !input.starts_with(kw) {
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
        }
        let rest = &input[kw.len()..];
        // Word-boundary: next char must not be alphanumeric or `_`.
        if let Some(c) = rest.chars().next() {
            if c.is_ascii_alphanumeric() || c == '_' {
                return Err(nom::Err::Error(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Tag,
                )));
            }
        }
        Ok((rest, &input[..kw.len()]))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_line_comments() {
        let src = "a = 1; // this is a comment\nb = 2;";
        let out = strip_comments(src);
        assert!(!out.contains("this is a comment"));
        assert!(out.contains("a = 1;"));
        assert!(out.contains("b = 2;"));
    }

    #[test]
    fn strip_block_comments() {
        let src = "before /* block comment */ after";
        let out = strip_comments(src);
        assert!(!out.contains("block comment"));
        assert!(out.contains("before"));
        assert!(out.contains("after"));
    }

    #[test]
    fn strip_preserves_string_content() {
        let src = r#"label = "value // not a comment""#;
        let out = strip_comments(src);
        assert!(out.contains("// not a comment"));
    }

    #[test]
    fn strip_preserves_string_with_block_comment_syntax() {
        let src = r#"label = "a /* b */ c""#;
        let out = strip_comments(src);
        assert!(out.contains("/* b */"));
    }

    #[test]
    fn identifier_basic() {
        let (rest, id) = identifier("hello world").unwrap();
        assert_eq!(id, "hello");
        assert_eq!(rest, " world");
    }

    #[test]
    fn identifier_with_underscores() {
        let (rest, id) = identifier("my_node_1 rest").unwrap();
        assert_eq!(id, "my_node_1");
        assert_eq!(rest, " rest");
    }

    #[test]
    fn identifier_rejects_leading_digit() {
        assert!(identifier("1bad").is_err());
    }

    #[test]
    fn arrow_parser() {
        let (rest, _) = arrow("-> foo").unwrap();
        assert_eq!(rest, " foo");
    }

    #[test]
    fn arrow_rejects_non_arrow() {
        assert!(arrow("- foo").is_err());
    }

    #[test]
    fn kw_digraph_accepts_keyword() {
        let (rest, kw) = kw_digraph("digraph G {").unwrap();
        assert_eq!(kw, "digraph");
        assert_eq!(rest, " G {");
    }

    #[test]
    fn kw_graph_rejects_graphattr() {
        // "graphattr" starts with "graph" but is not the keyword alone.
        assert!(kw_graph("graphattr").is_err());
    }

    #[test]
    fn opt_semi_present() {
        let (rest, s) = opt_semi(";rest").unwrap();
        assert_eq!(s, Some(';'));
        assert_eq!(rest, "rest");
    }

    #[test]
    fn opt_semi_absent() {
        let (rest, s) = opt_semi("rest").unwrap();
        assert_eq!(s, None);
        assert_eq!(rest, "rest");
    }

    #[test]
    fn ws_consumes_whitespace() {
        let (rest, _) = ws("  \t\n hello").unwrap();
        assert_eq!(rest, "hello");
    }
}
