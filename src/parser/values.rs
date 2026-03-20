//! Value type parsers for the DOT DSL.
//!
//! Parses the five value types without using nom combinator functions
//! (which return non-callable `impl Parser` in nom 8 stable).
//! All parsers are plain `fn(&str) -> IResult<&str, Value>` functions.

use crate::graph::Value;
use nom::{IResult, character::complete::digit1};
use std::time::Duration;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Parse any `Value` variant.
///
/// Dispatch order (to avoid prefix ambiguity):
/// 1. String (starts with `"`)
/// 2. Boolean (`true`/`false` keyword)
/// 3. Duration (integer followed by unit suffix)
/// 4. Float (contains `.`)
/// 5. Integer
pub fn parse_value(input: &str) -> IResult<&str, Value> {
    if input.starts_with('"') {
        return parse_string(input);
    }
    if let Ok(r) = parse_boolean(input) {
        return Ok(r);
    }
    if let Ok(r) = parse_duration(input) {
        return Ok(r);
    }
    if let Ok(r) = parse_float(input) {
        return Ok(r);
    }
    parse_integer(input)
}

/// Parse a double-quoted string with escape sequences.
///
/// Handles: `\"`, `\n`, `\t`, `\\`. Other `\X` passes through `X`.
pub fn parse_string(input: &str) -> IResult<&str, Value> {
    if !input.starts_with('"') {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Char,
        )));
    }
    let src = &input[1..]; // skip opening quote
    let mut result = String::new();
    let mut chars = src.char_indices();
    loop {
        match chars.next() {
            None => {
                return Err(nom::Err::Error(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Char,
                )));
            }
            Some((i, '"')) => {
                // End of string — remaining slice starts after closing quote
                let rest = &src[i + 1..];
                return Ok((rest, Value::Str(result)));
            }
            Some((_, '\\')) => match chars.next() {
                Some((_, '"')) => result.push('"'),
                Some((_, 'n')) => result.push('\n'),
                Some((_, 't')) => result.push('\t'),
                Some((_, '\\')) => result.push('\\'),
                Some((_, c)) => result.push(c),
                None => {
                    return Err(nom::Err::Error(nom::error::Error::new(
                        input,
                        nom::error::ErrorKind::Char,
                    )));
                }
            },
            Some((_, c)) => result.push(c),
        }
    }
}

/// Parse `true` or `false` (case-sensitive), followed by a non-identifier char.
pub fn parse_boolean(input: &str) -> IResult<&str, Value> {
    for (kw, val) in [("true", true), ("false", false)] {
        if let Some(rest) = input.strip_prefix(kw) {
            // Word-boundary: next char must not be alphanumeric or `_`
            if rest
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
            {
                continue;
            }
            return Ok((rest, Value::Bool(val)));
        }
    }
    Err(nom::Err::Error(nom::error::Error::new(
        input,
        nom::error::ErrorKind::Tag,
    )))
}

/// Parse a duration literal: non-negative integer + unit suffix.
///
/// Units (tried longest first to avoid `m` matching `ms`): `ms`, `s`, `m`, `h`, `d`.
pub fn parse_duration(input: &str) -> IResult<&str, Value> {
    // Only non-negative integers for durations
    let (after_digits, n_str) = digit1(input)?;
    let n: u64 = n_str.parse().map_err(|_| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Digit))
    })?;

    // Try units longest-first so "ms" is tried before "m"
    for (suffix, unit) in [("ms", "ms"), ("s", "s"), ("m", "m"), ("h", "h"), ("d", "d")] {
        if let Some(rest) = after_digits.strip_prefix(suffix) {
            // Word boundary: must not be followed by more identifier chars
            if rest
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
            {
                continue;
            }
            let d = match unit {
                "ms" => Duration::from_millis(n),
                "s" => Duration::from_secs(n),
                "m" => Duration::from_secs(n * 60),
                "h" => Duration::from_secs(n * 3600),
                "d" => Duration::from_secs(n * 86400),
                _ => unreachable!(),
            };
            return Ok((rest, Value::Duration(d)));
        }
    }
    Err(nom::Err::Error(nom::error::Error::new(
        input,
        nom::error::ErrorKind::Tag,
    )))
}

/// Parse a floating-point literal. Requires a decimal point.
///
/// Grammar: `'-'? [0-9]* '.' [0-9]+`
pub fn parse_float(input: &str) -> IResult<&str, Value> {
    let bytes = input.as_bytes();
    let mut pos = 0;
    // Optional minus
    if pos < bytes.len() && bytes[pos] == b'-' {
        pos += 1;
    }
    // Digits before decimal
    while pos < bytes.len() && bytes[pos].is_ascii_digit() {
        pos += 1;
    }
    // Require decimal point
    if pos >= bytes.len() || bytes[pos] != b'.' {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Float,
        )));
    }
    pos += 1; // consume '.'
    let decimal_start = pos;
    // Digits after decimal (at least one required)
    while pos < bytes.len() && bytes[pos].is_ascii_digit() {
        pos += 1;
    }
    if pos == decimal_start {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Float,
        )));
    }
    let raw = &input[..pos];
    let f: f64 = raw.parse().map_err(|_| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Float))
    })?;
    Ok((&input[pos..], Value::Float(f)))
}

/// Parse an optional-sign integer literal.
///
/// Grammar: `'-'? [0-9]+`
pub fn parse_integer(input: &str) -> IResult<&str, Value> {
    let bytes = input.as_bytes();
    let mut pos = 0;
    // Optional minus
    if pos < bytes.len() && bytes[pos] == b'-' {
        pos += 1;
    }
    let digit_start = pos;
    while pos < bytes.len() && bytes[pos].is_ascii_digit() {
        pos += 1;
    }
    if pos == digit_start {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Digit,
        )));
    }
    let raw = &input[..pos];
    let n: i64 = raw.parse().map_err(|_| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Digit))
    })?;
    Ok((&input[pos..], Value::Int(n)))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- String ---

    #[test]
    fn parse_simple_string() {
        let (rest, v) = parse_string(r#""hello" rest"#).unwrap();
        assert_eq!(v, Value::Str("hello".to_string()));
        assert_eq!(rest, " rest");
    }

    #[test]
    fn parse_string_escaped_quote() {
        let (_, v) = parse_string(r#""he said \"hi\"""#).unwrap();
        assert_eq!(v, Value::Str(r#"he said "hi""#.to_string()));
    }

    #[test]
    fn parse_string_escaped_newline() {
        let (_, v) = parse_string("\"line1\\nline2\"").unwrap();
        assert_eq!(v, Value::Str("line1\nline2".to_string()));
    }

    #[test]
    fn parse_string_escaped_tab() {
        let (_, v) = parse_string("\"a\\tb\"").unwrap();
        assert_eq!(v, Value::Str("a\tb".to_string()));
    }

    #[test]
    fn parse_string_escaped_backslash() {
        let (_, v) = parse_string(r#""a\\b""#).unwrap();
        assert_eq!(v, Value::Str("a\\b".to_string()));
    }

    // --- Boolean ---

    #[test]
    fn parse_true() {
        let (rest, v) = parse_boolean("true rest").unwrap();
        assert_eq!(v, Value::Bool(true));
        assert_eq!(rest, " rest");
    }

    #[test]
    fn parse_false() {
        let (_, v) = parse_boolean("false]").unwrap();
        assert_eq!(v, Value::Bool(false));
    }

    #[test]
    fn parse_boolean_rejects_trueish() {
        assert!(parse_boolean("trueish").is_err());
    }

    // --- Duration ---

    #[test]
    fn parse_duration_seconds() {
        let (_, v) = parse_duration("900s").unwrap();
        assert_eq!(v, Value::Duration(Duration::from_secs(900)));
    }

    #[test]
    fn parse_duration_minutes() {
        let (_, v) = parse_duration("15m").unwrap();
        assert_eq!(v, Value::Duration(Duration::from_secs(900)));
    }

    #[test]
    fn parse_duration_hours() {
        let (_, v) = parse_duration("2h").unwrap();
        assert_eq!(v, Value::Duration(Duration::from_secs(7200)));
    }

    #[test]
    fn parse_duration_millis() {
        let (_, v) = parse_duration("250ms").unwrap();
        assert_eq!(v, Value::Duration(Duration::from_millis(250)));
    }

    #[test]
    fn parse_duration_days() {
        let (_, v) = parse_duration("1d").unwrap();
        assert_eq!(v, Value::Duration(Duration::from_secs(86400)));
    }

    #[test]
    fn parse_duration_ms_before_m() {
        // "5ms" should parse as 5 milliseconds, not 5 minutes + "s" leftover
        let (rest, v) = parse_duration("5ms").unwrap();
        assert_eq!(v, Value::Duration(Duration::from_millis(5)));
        assert_eq!(rest, "");
    }

    // --- Float ---

    #[test]
    fn parse_float_basic() {
        let (_, v) = parse_float("3.15").unwrap();
        assert_eq!(v, Value::Float(3.15));
    }

    #[test]
    fn parse_float_negative() {
        let (_, v) = parse_float("-3.15").unwrap();
        assert_eq!(v, Value::Float(-3.15));
    }

    #[test]
    fn parse_float_zero_decimal() {
        let (_, v) = parse_float("0.5").unwrap();
        assert_eq!(v, Value::Float(0.5));
    }

    #[test]
    fn parse_float_rejects_integer() {
        assert!(parse_float("42").is_err());
    }

    // --- Integer ---

    #[test]
    fn parse_integer_positive() {
        let (_, v) = parse_integer("42 rest").unwrap();
        assert_eq!(v, Value::Int(42));
    }

    #[test]
    fn parse_integer_negative() {
        let (_, v) = parse_integer("-1").unwrap();
        assert_eq!(v, Value::Int(-1));
    }

    #[test]
    fn parse_integer_zero() {
        let (_, v) = parse_integer("0").unwrap();
        assert_eq!(v, Value::Int(0));
    }

    // --- parse_value dispatch ---

    #[test]
    fn parse_value_string() {
        let (_, v) = parse_value(r#""hello""#).unwrap();
        assert!(matches!(v, Value::Str(_)));
    }

    #[test]
    fn parse_value_bool() {
        let (_, v) = parse_value("true").unwrap();
        assert_eq!(v, Value::Bool(true));
    }

    #[test]
    fn parse_value_duration() {
        let (_, v) = parse_value("900s").unwrap();
        assert!(matches!(v, Value::Duration(_)));
    }

    #[test]
    fn parse_value_float() {
        let (_, v) = parse_value("0.5").unwrap();
        assert!(matches!(v, Value::Float(_)));
    }

    #[test]
    fn parse_value_int() {
        let (_, v) = parse_value("42").unwrap();
        assert_eq!(v, Value::Int(42));
    }
}
