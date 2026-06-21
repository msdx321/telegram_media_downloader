//! Recursive-descent parser for the Telegram Media Downloader filter language.
//!
//! Grammar:
//!   expr     = or_expr
//!   or_expr  = and_expr ("||" | "or" | "OR") and_expr
//!   and_expr = comp ("&&" | "and" | "AND") comp
//!   comp     = add (("==" | "!=" | ">" | "<" | ">=" | "<=") add)*
//!   add      = mul (("+" | "-") mul)*
//!   mul      = unary (("*" | "/") unary)*
//!   unary    = "-" unary | primary
//!   primary  = NUMBER | STRING | RESTRING | TIME | NAME | "(" expr ")"

use crate::format::parse_byte_str;
use chrono::NaiveDateTime;
use regex::Regex;
use std::collections::HashMap;
use std::fmt;
use std::str::Chars;
use std::sync::LazyLock;

// Date-like prefixes used by the lexer to decide whether a run of digits
// starts a datetime literal instead of a plain number.
static RE_DATETIME_FULL: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^\d{4}[-/.]\d{1,2}[-/.]\d{1,2}\s+\d{1,2}:\d{1,2}:\d{1,2}").unwrap()
});
static RE_DATETIME_DATE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\d{4}[-/.]\d{1,2}[-/.]\d{1,2}").unwrap());

// ── AST ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum Value {
    Int(i64),
    Float(f64),
    Str(String),
    ReStr(Regex),
    DateTime(NaiveDateTime),
    Bool(bool),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Int(v) => write!(f, "{v}"),
            Value::Float(v) => write!(f, "{v}"),
            Value::Str(v) => write!(f, "{v}"),
            Value::ReStr(v) => write!(f, "r'{v}'"),
            Value::DateTime(v) => write!(f, "{}", v.format("%Y-%m-%d %H:%M:%S")),
            Value::Bool(v) => write!(f, "{v}"),
        }
    }
}

// ── Tokenizer ────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
enum Token {
    Num(i64),
    Str(String),
    ReStr(Regex),
    Time(NaiveDateTime),
    Name(String),
    And,
    Or,
    Eq,
    Ne,
    Ge,
    Le,
    Gt,
    Lt,
    Plus,
    Minus,
    Star,
    Slash,
    LParen,
    RParen,
    Eof,
}

struct Lexer<'a> {
    chars: Chars<'a>,
    peeked: Option<char>,
}

impl<'a> Lexer<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            chars: input.chars(),
            peeked: None,
        }
    }

    fn next_char(&mut self) -> Option<char> {
        self.peeked.take().or_else(|| self.chars.next())
    }

    fn peek_char(&mut self) -> Option<char> {
        if self.peeked.is_none() {
            self.peeked = self.chars.next();
        }
        self.peeked
    }

    fn skip_ws(&mut self) {
        while let Some(c) = self.peek_char() {
            if !c.is_ascii_whitespace() {
                break;
            }
            self.next_char();
        }
    }

    fn read_string(&mut self, quote: char) -> Token {
        let mut s = String::new();
        loop {
            match self.next_char() {
                Some(c) if c == quote => break,
                Some(c) => s.push(c),
                None => break, // unterminated string — treat EOL as end
            }
        }
        Token::Str(s)
    }

    fn read_re_string(&mut self) -> Token {
        // We've seen `r'` — read until closing quote.
        let mut s = String::new();
        loop {
            match self.next_char() {
                Some('\'') => break,
                Some(c) => s.push(c),
                None => break,
            }
        }
        match Regex::new(&s) {
            Ok(re) => Token::ReStr(re),
            Err(_) => Token::Str(s), // fallback: treat as plain string
        }
    }

    fn read_number(&mut self, first: char) -> Token {
        let mut s = String::from(first);
        while let Some(c) = self.peek_char() {
            if c.is_ascii_digit() {
                s.push(c);
                self.next_char();
            } else {
                break;
            }
        }
        // Check for byte suffix: 10MB, 1GB, etc. `c` is the first letter
        // (held in `peeked`); `self.chars` sits just past it, so we clone
        // from there to read the remaining letters without consuming them.
        if let Some(c) = self.peek_char() {
            if c.is_ascii_alphabetic() {
                let tail: String = self
                    .chars
                    .clone()
                    .take_while(|ch| ch.is_ascii_alphabetic())
                    .collect();
                let candidate = format!("{s}{c}{tail}");
                if let Some(bytes) = parse_byte_str(&candidate) {
                    self.next_char(); // consume the peeked first letter
                    for _ in 0..tail.len() {
                        self.next_char();
                    }
                    return Token::Num(bytes as i64);
                }
            }
        }
        Token::Num(s.parse().unwrap())
    }

    fn read_name(&mut self, first: char) -> Token {
        let mut s = String::from(first);
        while let Some(c) = self.peek_char() {
            if c.is_alphanumeric() || c == '_' {
                s.push(c);
                self.next_char();
            } else {
                break;
            }
        }
        match s.to_uppercase().as_str() {
            "AND" => Token::And,
            "OR" => Token::Or,
            _ => Token::Name(s),
        }
    }

    fn read_datetime(&mut self) -> Token {
        let mut s = String::new();
        // Read digits, hyphens, colons, spaces
        while let Some(c) = self.peek_char() {
            if c.is_ascii_digit() || c == '-' || c == '.' || c == '/' || c == ' ' || c == ':' {
                s.push(c);
                self.next_char();
            } else {
                break;
            }
        }
        let normalized = s.replace(['/', '.'], "-");
        if let Ok(dt) = NaiveDateTime::parse_from_str(&normalized, "%Y-%m-%d %H:%M:%S") {
            return Token::Time(dt);
        }
        // Try just date
        if let Ok(dt) = chrono::NaiveDate::parse_from_str(normalized.trim(), "%Y-%m-%d") {
            return Token::Time(dt.and_hms_opt(0, 0, 0).unwrap());
        }
        // Fallback: treat as string
        Token::Str(s)
    }
}

impl<'a> Iterator for Lexer<'a> {
    type Item = Token;

    fn next(&mut self) -> Option<Token> {
        self.skip_ws();
        let c = self.next_char()?;

        Some(match c {
            '\'' => self.read_string('\''),
            '\"' => self.read_string('\"'),
            'r' if self.peek_char() == Some('\'') => {
                self.next_char(); // consume the '
                self.read_re_string()
            }
            '(' => Token::LParen,
            ')' => Token::RParen,
            '+' => Token::Plus,
            '-' => Token::Minus,
            '*' => Token::Star,
            '/' => Token::Slash,
            '=' => {
                if self.peek_char() == Some('=') {
                    self.next_char();
                    Token::Eq
                } else {
                    Token::Name("=".into())
                }
            }
            '!' => {
                if self.peek_char() == Some('=') {
                    self.next_char();
                    Token::Ne
                } else {
                    Token::Name("!".into())
                }
            }
            '>' => {
                if self.peek_char() == Some('=') {
                    self.next_char();
                    Token::Ge
                } else {
                    Token::Gt
                }
            }
            '<' => {
                if self.peek_char() == Some('=') {
                    self.next_char();
                    Token::Le
                } else {
                    Token::Lt
                }
            }
            '&' => {
                if self.peek_char() == Some('&') {
                    self.next_char();
                    Token::And
                } else {
                    Token::Name("&".into())
                }
            }
            '|' => {
                if self.peek_char() == Some('|') {
                    self.next_char();
                    Token::Or
                } else {
                    Token::Name("|".into())
                }
            }
            d if d.is_ascii_digit() => {
                // Peek ahead: if date-like pattern, parse as datetime
                let rest: String = self.chars.clone().take(20).collect();
                let candidate = format!("{d}{rest}");
                if RE_DATETIME_FULL.is_match(&candidate) || RE_DATETIME_DATE.is_match(&candidate) {
                    self.read_datetime()
                } else {
                    self.read_number(d)
                }
            }
            a if a.is_alphabetic() || a == '_' => self.read_name(a),
            _ => Token::Name(c.to_string()),
        })
    }
}

// ── Parser ───────────────────────────────────────────────────────────────

pub struct Parser<'a> {
    tokens: Vec<Token>,
    pos: usize,
    _input: &'a str,
}

impl<'a> Parser<'a> {
    pub fn new(input: &'a str) -> Self {
        let lexer = Lexer::new(input);
        Self {
            tokens: lexer.collect(),
            pos: 0,
            _input: input,
        }
    }

    fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    fn advance(&mut self) -> &Token {
        self.pos += 1;
        self.tokens.get(self.pos - 1).unwrap_or(&Token::Eof)
    }

    fn expect(&mut self, expected: fn(&Token) -> bool, label: &str) -> Result<Token, String> {
        let t = self.advance();
        if expected(t) {
            Ok(t.clone())
        } else {
            Err(format!("expected {label}, got {t:?}"))
        }
    }

    // ── expression parsing (Pratt-style) ─────────────────────────────────

    pub fn parse(&mut self, vars: &HashMap<String, Value>) -> Result<Value, String> {
        let val = self.or_expr(vars)?;
        if !matches!(self.peek(), Token::Eof) {
            return Err(format!(
                "unexpected token after expression: {:?}",
                self.peek()
            ));
        }
        Ok(val)
    }

    fn or_expr(&mut self, vars: &HashMap<String, Value>) -> Result<Value, String> {
        let mut left = self.and_expr(vars)?;
        while matches!(self.peek(), Token::Or) {
            self.advance();
            let right = self.and_expr(vars)?;
            left = truthy_or(left, right);
        }
        Ok(left)
    }

    fn and_expr(&mut self, vars: &HashMap<String, Value>) -> Result<Value, String> {
        let mut left = self.comp(vars)?;
        while matches!(self.peek(), Token::And) {
            self.advance();
            let right = self.comp(vars)?;
            left = truthy_and(left, right);
        }
        Ok(left)
    }

    fn comp(&mut self, vars: &HashMap<String, Value>) -> Result<Value, String> {
        let left = self.add(vars)?;
        match self.peek() {
            Token::Eq | Token::Ne | Token::Gt | Token::Lt | Token::Ge | Token::Le => {
                let op = self.advance().clone();
                let right = self.add(vars)?;
                Ok(compare(&left, &op, &right))
            }
            _ => Ok(left),
        }
    }

    fn add(&mut self, vars: &HashMap<String, Value>) -> Result<Value, String> {
        let mut left = self.mul(vars)?;
        loop {
            match self.peek() {
                Token::Plus => {
                    self.advance();
                    left = arithmetic(&left, "+", &self.mul(vars)?);
                }
                Token::Minus => {
                    self.advance();
                    left = arithmetic(&left, "-", &self.mul(vars)?);
                }
                _ => break,
            }
        }
        Ok(left)
    }

    fn mul(&mut self, vars: &HashMap<String, Value>) -> Result<Value, String> {
        let mut left = self.unary(vars)?;
        loop {
            match self.peek() {
                Token::Star => {
                    self.advance();
                    left = arithmetic(&left, "*", &self.unary(vars)?);
                }
                Token::Slash => {
                    self.advance();
                    left = arithmetic(&left, "/", &self.unary(vars)?);
                }
                _ => break,
            }
        }
        Ok(left)
    }

    fn unary(&mut self, vars: &HashMap<String, Value>) -> Result<Value, String> {
        if matches!(self.peek(), Token::Minus) {
            self.advance();
            let val = self.unary(vars)?;
            return match val {
                Value::Int(v) => Ok(Value::Int(-v)),
                Value::Float(v) => Ok(Value::Float(-v)),
                _ => Err("cannot negate non-numeric value".into()),
            };
        }
        self.primary(vars)
    }

    fn primary(&mut self, vars: &HashMap<String, Value>) -> Result<Value, String> {
        match self.peek() {
            Token::Num(n) => {
                let v = *n;
                self.advance();
                Ok(Value::Int(v))
            }
            Token::Str(_) => {
                if let Token::Str(s) = self.advance().clone() {
                    Ok(Value::Str(s))
                } else {
                    unreachable!()
                }
            }
            Token::ReStr(_) => {
                if let Token::ReStr(r) = self.advance().clone() {
                    Ok(Value::ReStr(r))
                } else {
                    unreachable!()
                }
            }
            Token::Time(dt) => {
                let v = *dt;
                self.advance();
                Ok(Value::DateTime(v))
            }
            Token::Name(name) => {
                let n = name.clone();
                self.advance();
                vars.get(&n)
                    .cloned()
                    .ok_or_else(|| format!("undefined variable: {n}"))
            }
            Token::LParen => {
                self.advance();
                let v = self.or_expr(vars)?;
                self.expect(|t| matches!(t, Token::RParen), ")")?;
                Ok(v)
            }
            _ => Err(format!("unexpected token: {:?}", self.peek())),
        }
    }
}

// ── Value operations ─────────────────────────────────────────────────────

fn truthy_and(a: Value, b: Value) -> Value {
    match (a, b) {
        (Value::Bool(a), Value::Bool(b)) => Value::Bool(a && b),
        _ => Value::Bool(false),
    }
}

fn truthy_or(a: Value, b: Value) -> Value {
    match (a, b) {
        (Value::Bool(a), Value::Bool(b)) => Value::Bool(a || b),
        _ => Value::Bool(false),
    }
}

fn compare(left: &Value, op: &Token, right: &Value) -> Value {
    let result = match (left, right) {
        (Value::Int(l), Value::Int(r)) => cmp_num(*l as f64, *r as f64, op),
        (Value::Float(l), Value::Float(r)) => cmp_num(*l, *r, op),
        (Value::Int(l), Value::Float(r)) => cmp_num(*l as f64, *r, op),
        (Value::Float(l), Value::Int(r)) => cmp_num(*l, *r as f64, op),
        (Value::Str(l), Value::Str(r)) => cmp_str(l, r, op),
        (Value::Str(l), Value::ReStr(r)) => match op {
            Token::Eq => r.is_match(l),
            Token::Ne => !r.is_match(l),
            _ => false,
        },
        (Value::ReStr(l), Value::Str(r)) => match op {
            Token::Eq => l.is_match(r),
            Token::Ne => !l.is_match(r),
            _ => false,
        },
        (Value::DateTime(l), Value::DateTime(r)) => cmp_num(
            l.and_utc().timestamp() as f64,
            r.and_utc().timestamp() as f64,
            op,
        ),
        _ => false,
    };
    Value::Bool(result)
}

fn cmp_num(l: f64, r: f64, op: &Token) -> bool {
    match op {
        Token::Eq => (l - r).abs() < f64::EPSILON,
        Token::Ne => (l - r).abs() >= f64::EPSILON,
        Token::Gt => l > r,
        Token::Lt => l < r,
        Token::Ge => l >= r,
        Token::Le => l <= r,
        _ => false,
    }
}

fn cmp_str(l: &str, r: &str, op: &Token) -> bool {
    match op {
        Token::Eq => l == r,
        Token::Ne => l != r,
        Token::Gt => l > r,
        Token::Lt => l < r,
        Token::Ge => l >= r,
        Token::Le => l <= r,
        _ => false,
    }
}

fn arithmetic(left: &Value, op: &str, right: &Value) -> Value {
    let l = as_f64(left);
    let r = as_f64(right);
    let result = match op {
        "+" => l + r,
        "-" => l - r,
        "*" => l * r,
        "/" => {
            if r == 0.0 {
                return Value::Float(f64::NAN);
            }
            l / r
        }
        _ => return Value::Int(0),
    };
    if result.fract() == 0.0 && result.is_finite() {
        Value::Int(result as i64)
    } else {
        Value::Float(result)
    }
}

fn as_f64(v: &Value) -> f64 {
    match v {
        Value::Int(n) => *n as f64,
        Value::Float(n) => *n,
        _ => 0.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn var(name: &str, v: i64) -> HashMap<String, Value> {
        let mut m = HashMap::new();
        m.insert(name.into(), Value::Int(v));
        m
    }

    #[test]
    fn byte_suffix_lexes_intact() {
        // Regression: the first letter of a byte suffix used to be dropped,
        // so "10MB" was read as Num(10) plus a dangling Name("B") token and
        // the parser rejected the trailing token.
        let mut p = Parser::new("10MB");
        assert!(matches!(
            p.parse(&HashMap::new()).unwrap(),
            Value::Int(n) if n == 10 * 1024 * 1024
        ));
    }

    #[test]
    fn byte_suffix_in_comparison() {
        let mut p = Parser::new("file_size >= 10MB");
        assert!(matches!(
            p.parse(&var("file_size", 10 * 1024 * 1024)).unwrap(),
            Value::Bool(true)
        ));
        let mut p = Parser::new("file_size >= 10MB");
        assert!(matches!(
            p.parse(&var("file_size", 10)).unwrap(),
            Value::Bool(false)
        ));
    }
}
