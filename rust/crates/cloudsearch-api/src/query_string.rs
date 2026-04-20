//! Query string parser for cloudSearch.
//!
//! Converts query strings like `"field:value AND (tag:foo OR tag:bar)"`
//! into the existing `SearchQuery` AST.

use cloudsearch_common::{
    BoolQuery, CloudSearchError, RangeQuery, SearchQuery, TermQuery, WildcardQuery,
};

/// Parse a query string into a `SearchQuery`.
pub fn parse_query_string(input: &str) -> Result<SearchQuery, CloudSearchError> {
    let mut parser = Parser::new(input);
    let query = parser.parse_query()?;
    parser.ensure_exhausted()?;
    Ok(query)
}

/// Token produced by the tokenizer.
#[derive(Debug, Clone, PartialEq)]
enum Token {
    /// The colon separator between field and value
    Colon,
    /// Parentheses
    Lparen,
    Rparen,
}

struct Parser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn parse_query(&mut self) -> Result<SearchQuery, CloudSearchError> {
        self.parse_or_expr()
    }

    /// `OR_EXPR` ::= `AND_EXPR` ( "OR" `AND_EXPR` )*
    fn parse_or_expr(&mut self) -> Result<SearchQuery, CloudSearchError> {
        let mut left = self.parse_and_expr()?;

        loop {
            self.skip_whitespace();
            let rest = &self.input[self.pos..];

            // Handle NOT: combine with left using must_not
            if let Some(stripped) = rest.strip_prefix("NOT") {
                let after = stripped.chars().next();
                if after.is_none_or(|c| !c.is_alphanumeric() && c != '_' && c != '-') {
                    self.pos += 3;
                    let right = self.parse_and_expr()?;
                    left = self.make_bool_must_not_and(left, right);
                    continue;
                }
            }

            if self.skip_word("OR") {
                let right = self.parse_and_expr()?;
                left = self.make_bool_should(left, right);
                continue;
            }

            break;
        }

        Ok(left)
    }

    /// `AND_EXPR` ::= `NOT_EXPR` ( "AND" `NOT_EXPR` )*
    fn parse_and_expr(&mut self) -> Result<SearchQuery, CloudSearchError> {
        let mut left = self.parse_not_expr()?;

        loop {
            self.skip_whitespace();
            let rest = &self.input[self.pos..];

            // Stop at ) (let caller handle it)
            if rest.starts_with(')') {
                break;
            }
            // Stop at end
            if self.pos >= self.input.len() {
                break;
            }

            // Handle AND
            if let Some(stripped) = rest.strip_prefix("AND") {
                let after = stripped.chars().next();
                if after.is_none_or(|c| !c.is_alphanumeric() && c != '_' && c != '-') {
                    self.pos += 3;
                    let right = self.parse_not_expr()?;
                    left = self.make_bool_must(left, right);
                    continue;
                }
            }

            // Stop at OR or NOT (let parse_or_expr handle it)
            if let Some(stripped) = rest.strip_prefix("OR") {
                let after = stripped.chars().next();
                if after.is_none_or(|c| !c.is_alphanumeric() && c != '_' && c != '-') {
                    break;
                }
            }
            if let Some(stripped) = rest.strip_prefix("NOT") {
                let after = stripped.chars().next();
                if after.is_none_or(|c| !c.is_alphanumeric() && c != '_' && c != '-') {
                    break;
                }
            }

            // Implicit AND: bare clause following
            if !rest.starts_with(')') && !self.is_at_operator() {
                let right = self.parse_not_expr()?;
                left = self.make_bool_must(left, right);
                continue;
            }

            break;
        }

        Ok(left)
    }

    /// `NOT_EXPR` ::= PRIMARY (handles NOT in `parse_and_expr`)
    fn parse_not_expr(&mut self) -> Result<SearchQuery, CloudSearchError> {
        self.parse_primary()
    }

    /// PRIMARY ::= "(" QUERY ")" | CLAUSE
    fn parse_primary(&mut self) -> Result<SearchQuery, CloudSearchError> {
        if self.skip(Token::Lparen) {
            let query = self.parse_query()?;
            self.expect(Token::Rparen, "closing parenthesis")?;
            Ok(query)
        } else {
            self.parse_clause()
        }
    }

    /// CLAUSE ::= FIELD ":" VALUE | VALUE (bare word = tag:field)
    fn parse_clause(&mut self) -> Result<SearchQuery, CloudSearchError> {
        // Check for a field:value pattern
        if let Some((field, value)) = self.try_parse_field_value()? {
            self.classify_and_build_query(&field, &value)
        } else {
            // Bare word: treat as tag:word
            let word = self.read_word();
            if word.is_empty() {
                return Err(CloudSearchError::InvalidSearchRequest(
                    "unexpected end of query".to_string(),
                ));
            }
            Ok(SearchQuery::Term(TermQuery {
                field: "tag".to_string(),
                value: serde_json::Value::String(word.to_string()),
            }))
        }
    }

    /// Try to parse "field:value". Returns None if no colon found.
    fn try_parse_field_value(&mut self) -> Result<Option<(String, String)>, CloudSearchError> {
        let start = self.pos;
        let field = self.read_word().to_string();
        if field.is_empty() {
            self.pos = start;
            return Ok(None);
        }

        if !self.skip(Token::Colon) {
            self.pos = start;
            return Ok(None);
        }

        let value = self.read_value()?;
        if value.is_empty() {
            self.pos = start;
            return Err(CloudSearchError::InvalidSearchRequest(format!(
                "missing value for field '{field}'"
            )));
        }

        Ok(Some((field, value.to_string())))
    }

    /// Classify a field:value pair and build the appropriate `SearchQuery`.
    fn classify_and_build_query(
        &self,
        field: &str,
        value: &str,
    ) -> Result<SearchQuery, CloudSearchError> {
        // Range operators: check >=, <= first before >, <
        if let Some(stripped) = value.strip_prefix(">=") {
            let num = self.parse_numeric(stripped)?;
            return Ok(SearchQuery::Range(RangeQuery {
                field: field.to_string(),
                gte: Some(num.clone()),
                gt: None,
                lte: None,
                lt: None,
            }));
        }
        if let Some(stripped) = value.strip_prefix("<=") {
            let num = self.parse_numeric(stripped)?;
            return Ok(SearchQuery::Range(RangeQuery {
                field: field.to_string(),
                gte: None,
                gt: None,
                lte: Some(num.clone()),
                lt: None,
            }));
        }
        if let Some(stripped) = value.strip_prefix('>') {
            let num = self.parse_numeric(stripped)?;
            return Ok(SearchQuery::Range(RangeQuery {
                field: field.to_string(),
                gte: None,
                gt: Some(num.clone()),
                lte: None,
                lt: None,
            }));
        }
        if let Some(stripped) = value.strip_prefix('<') {
            let num = self.parse_numeric(stripped)?;
            return Ok(SearchQuery::Range(RangeQuery {
                field: field.to_string(),
                gte: None,
                gt: None,
                lte: None,
                lt: Some(num.clone()),
            }));
        }

        // Range syntax: A..B
        if let Some((lo, hi)) = value.split_once("..")
            && (!lo.is_empty() || !hi.is_empty())
        {
            let lo_num = self.parse_numeric(lo)?;
            let hi_num = self.parse_numeric(hi)?;
            return Ok(SearchQuery::Range(RangeQuery {
                field: field.to_string(),
                gte: Some(lo_num.clone()),
                gt: None,
                lte: Some(hi_num.clone()),
                lt: None,
            }));
        }

        // Wildcard detection: contains * or ?
        if value.contains('*') || value.contains('?') {
            return Ok(SearchQuery::Wildcard(WildcardQuery {
                field: field.to_string(),
                value: value.to_string(),
            }));
        }

        // Default: term query
        let json_value = self.parse_value(value);
        Ok(SearchQuery::Term(TermQuery {
            field: field.to_string(),
            value: json_value,
        }))
    }

    fn parse_numeric(&self, s: &str) -> Result<serde_json::Value, CloudSearchError> {
        // Try to parse as integer first
        if let Ok(i) = s.parse::<i64>() {
            return Ok(serde_json::json!(i));
        }
        // Fall back to float
        s.parse::<f64>()
            .map(|n| serde_json::json!(n))
            .map_err(|_| CloudSearchError::InvalidSearchRequest(format!("invalid number '{s}'")))
    }

    fn parse_value(&self, s: &str) -> serde_json::Value {
        // Try to parse as JSON first (numbers, booleans)
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
            return v;
        }
        // Fall back to string
        serde_json::Value::String(s.to_string())
    }

    /// Read a word (alphanumeric + underscore, non-empty)
    fn read_word(&mut self) -> &str {
        let start = self.pos;
        while self.pos < self.input.len() {
            let c = self.input[self.pos..].chars().next().unwrap();
            if c.is_alphanumeric() || c == '_' || c == '-' {
                self.pos += c.len_utf8();
            } else {
                break;
            }
        }
        &self.input[start..self.pos]
    }

    /// Read a value: word, or quoted string
    fn read_value(&mut self) -> Result<&str, CloudSearchError> {
        self.skip_whitespace();

        if self.pos >= self.input.len() {
            return Ok("");
        }

        if self.input[self.pos..].starts_with('"') {
            self.read_quoted_string()
        } else {
            // Read until whitespace or operator
            let start = self.pos;
            while self.pos < self.input.len() {
                let c = self.input[self.pos..].chars().next().unwrap();
                if c.is_whitespace() || self.is_operator_start() {
                    break;
                }
                self.pos += c.len_utf8();
            }
            Ok(&self.input[start..self.pos])
        }
    }

    fn read_quoted_string(&mut self) -> Result<&str, CloudSearchError> {
        // Opening quote
        self.pos += 1;
        let start = self.pos;

        while self.pos < self.input.len() {
            let c = self.input[self.pos..].chars().next().unwrap();
            if c == '\\' && self.pos + 1 < self.input.len() {
                // Skip escaped character
                self.pos += 2;
            } else if c == '"' {
                let result = &self.input[start..self.pos];
                self.pos += 1; // consume closing quote
                return Ok(result);
            } else {
                self.pos += c.len_utf8();
            }
        }

        Err(CloudSearchError::InvalidSearchRequest(
            "unclosed quoted string".to_string(),
        ))
    }

    fn skip_whitespace(&mut self) {
        while self.pos < self.input.len()
            && self.input[self.pos..]
                .chars()
                .next().is_some_and(char::is_whitespace)
        {
            self.pos += 1;
        }
    }

    fn is_operator_start(&self) -> bool {
        let rest = &self.input[self.pos..];
        rest.starts_with("AND")
            || rest.starts_with("OR")
            || rest.starts_with("NOT")
            || rest.starts_with('(')
            || rest.starts_with(')')
    }

    fn skip_word(&mut self, word: &str) -> bool {
        self.skip_whitespace();
        let rest = &self.input[self.pos..];
        if let Some(after) = rest.strip_prefix(word) {
            // Ensure it's a whole word (next char not alphanumeric/underscore/hyphen)
            if after.is_empty()
                || !after.starts_with(|c: char| c.is_alphanumeric() || c == '_' || c == '-')
            {
                self.pos += word.len();
                return true;
            }
        }
        false
    }

    /// Check if current position is at an operator keyword (after skipping whitespace)
    fn is_at_operator(&self) -> bool {
        let rest = &self.input[self.pos..];
        // AND
        if let Some(stripped) = rest.strip_prefix("AND") {
            let after = stripped.chars().next();
            return after.is_none_or(|c| !c.is_alphanumeric() && c != '_' && c != '-');
        }
        // OR
        if let Some(stripped) = rest.strip_prefix("OR") {
            let after = stripped.chars().next();
            return after.is_none_or(|c| !c.is_alphanumeric() && c != '_' && c != '-');
        }
        // NOT
        if let Some(stripped) = rest.strip_prefix("NOT") {
            let after = stripped.chars().next();
            return after.is_none_or(|c| !c.is_alphanumeric() && c != '_' && c != '-');
        }
        false
    }

    fn skip(&mut self, token: Token) -> bool {
        self.skip_whitespace();
        match token {
            Token::Colon => {
                if self.input[self.pos..].starts_with(':') {
                    self.pos += 1;
                    true
                } else {
                    false
                }
            }
            Token::Lparen => {
                if self.input[self.pos..].starts_with('(') {
                    self.pos += 1;
                    true
                } else {
                    false
                }
            }
            Token::Rparen => {
                if self.input[self.pos..].starts_with(')') {
                    self.pos += 1;
                    true
                } else {
                    false
                }
            }
        }
    }

    fn expect(&mut self, token: Token, description: &str) -> Result<(), CloudSearchError> {
        if self.skip(token) {
            Ok(())
        } else {
            Err(CloudSearchError::InvalidSearchRequest(format!(
                "expected {description}"
            )))
        }
    }

    fn ensure_exhausted(&self) -> Result<(), CloudSearchError> {
        let mut pos = self.pos;
        while pos < self.input.len()
            && self.input[pos..].chars().next().is_some_and(char::is_whitespace)
        {
            pos += 1;
        }
        if pos < self.input.len() {
            Err(CloudSearchError::InvalidSearchRequest(format!(
                "unexpected characters at end of query: '{}'",
                &self.input[pos..]
            )))
        } else {
            Ok(())
        }
    }

    /// Combine two queries into a `BoolQuery` with must.
    fn make_bool_must(&self, left: SearchQuery, right: SearchQuery) -> SearchQuery {
        let mut must = Vec::new();
        let mut must_not = Vec::new();

        match left {
            SearchQuery::Bool(BoolQuery {
                must: m,
                must_not: mn,
                should,
                filter,
                ..
            }) => {
                if !m.is_empty() || !filter.is_empty() {
                    // Wrap in must preserving should
                    must.push(SearchQuery::Bool(BoolQuery {
                        must: m,
                        should,
                        filter,
                        must_not: vec![],
                    }));
                } else if !should.is_empty() {
                    // Bool with only should (e.g., OR group) - preserve as a unit in must
                    must.push(SearchQuery::Bool(BoolQuery {
                        must: vec![],
                        should,
                        filter: vec![],
                        must_not: vec![],
                    }));
                }
                must_not.extend(mn);
            }
            other => must.push(other),
        }
        match right {
            SearchQuery::Bool(BoolQuery {
                must: m,
                must_not: mn,
                should,
                filter,
                ..
            }) => {
                if !m.is_empty() || !filter.is_empty() {
                    must.push(SearchQuery::Bool(BoolQuery {
                        must: m,
                        should,
                        filter,
                        must_not: vec![],
                    }));
                } else if !should.is_empty() {
                    must.push(SearchQuery::Bool(BoolQuery {
                        must: vec![],
                        should,
                        filter: vec![],
                        must_not: vec![],
                    }));
                }
                must_not.extend(mn);
            }
            other => must.push(other),
        }

        SearchQuery::Bool(BoolQuery {
            must,
            should: vec![],
            filter: vec![],
            must_not,
        })
    }

    /// Combine two queries into a `BoolQuery` with should.
    fn make_bool_should(&self, left: SearchQuery, right: SearchQuery) -> SearchQuery {
        let mut should = Vec::new();
        let mut must = Vec::new();

        match left {
            SearchQuery::Bool(BoolQuery {
                must: m, should: s, ..
            }) => {
                if !m.is_empty() {
                    must.extend(m);
                }
                should.extend(s);
            }
            other => should.push(other),
        }
        match right {
            SearchQuery::Bool(BoolQuery {
                must: m, should: s, ..
            }) => {
                if !m.is_empty() {
                    must.extend(m);
                }
                should.extend(s);
            }
            other => should.push(other),
        }

        SearchQuery::Bool(BoolQuery {
            must,
            should,
            filter: vec![],
            must_not: vec![],
        })
    }

    /// Combine left AND (NOT right) → must: [left's must + should], `must_not`: [right]
    fn make_bool_must_not_and(&self, left: SearchQuery, right: SearchQuery) -> SearchQuery {
        let (must, should, filter) = match left {
            SearchQuery::Bool(BoolQuery {
                must: m,
                should,
                filter,
                ..
            }) => (m, should, filter),
            other => (vec![other], vec![], vec![]),
        };
        SearchQuery::Bool(BoolQuery {
            must,
            should,
            filter,
            must_not: vec![right],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cloudsearch_common::RangeQuery;

    #[test]
    fn test_simple_term() {
        let result = parse_query_string("status:active").unwrap();
        assert_eq!(
            result,
            SearchQuery::Term(TermQuery {
                field: "status".to_string(),
                value: serde_json::json!("active")
            })
        );
    }

    #[test]
    fn test_term_with_numeric_value() {
        let result = parse_query_string("count:42").unwrap();
        assert_eq!(
            result,
            SearchQuery::Term(TermQuery {
                field: "count".to_string(),
                value: serde_json::json!(42)
            })
        );
    }

    #[test]
    fn test_implicit_and() {
        let result = parse_query_string("status:active type:post").unwrap();
        let bool_q = match result {
            SearchQuery::Bool(b) => b,
            other => panic!("expected Bool, got {other:?}"),
        };
        assert_eq!(bool_q.must.len(), 2);
    }

    #[test]
    fn test_explicit_and() {
        let result = parse_query_string("status:active AND type:post").unwrap();
        let bool_q = match result {
            SearchQuery::Bool(b) => b,
            other => panic!("expected Bool, got {other:?}"),
        };
        assert_eq!(bool_q.must.len(), 2);
    }

    #[test]
    fn test_or() {
        let result = parse_query_string("tag:foo OR tag:bar").unwrap();
        let bool_q = match result {
            SearchQuery::Bool(b) => b,
            other => panic!("expected Bool, got {other:?}"),
        };
        assert_eq!(bool_q.should.len(), 2);
    }

    #[test]
    fn test_not() {
        let result = parse_query_string("status:active NOT deleted:true").unwrap();
        let bool_q = match result {
            SearchQuery::Bool(b) => b,
            other => panic!("expected Bool, got {other:?}"),
        };
        assert_eq!(bool_q.must.len(), 1);
        assert_eq!(bool_q.must_not.len(), 1);
    }

    #[test]
    fn test_parens_grouping() {
        let result = parse_query_string("(tag:foo OR tag:bar) AND status:active").unwrap();
        let bool_q = match result {
            SearchQuery::Bool(b) => b,
            other => panic!("expected Bool, got {other:?}"),
        };
        assert_eq!(bool_q.must.len(), 2);
    }

    #[test]
    fn test_quoted_string() {
        let result = parse_query_string("message:\"hello world\"").unwrap();
        assert_eq!(
            result,
            SearchQuery::Term(TermQuery {
                field: "message".to_string(),
                value: serde_json::json!("hello world")
            })
        );
    }

    #[test]
    fn test_wildcard() {
        let result = parse_query_string("service:auth-*").unwrap();
        assert_eq!(
            result,
            SearchQuery::Wildcard(WildcardQuery {
                field: "service".to_string(),
                value: "auth-*".to_string()
            })
        );
    }

    #[test]
    fn test_prefix() {
        let result = parse_query_string("service:auth*").unwrap();
        assert_eq!(
            result,
            SearchQuery::Wildcard(WildcardQuery {
                field: "service".to_string(),
                value: "auth*".to_string()
            })
        );
    }

    #[test]
    fn test_range_gt() {
        let result = parse_query_string("price:>10").unwrap();
        assert_eq!(
            result,
            SearchQuery::Range(RangeQuery {
                field: "price".to_string(),
                gte: None,
                gt: Some(serde_json::json!(10)),
                lte: None,
                lt: None,
            })
        );
    }

    #[test]
    fn test_range_gte() {
        let result = parse_query_string("price:>=10").unwrap();
        assert_eq!(
            result,
            SearchQuery::Range(RangeQuery {
                field: "price".to_string(),
                gte: Some(serde_json::json!(10)),
                gt: None,
                lte: None,
                lt: None,
            })
        );
    }

    #[test]
    fn test_range_lt() {
        let result = parse_query_string("price:<100").unwrap();
        assert_eq!(
            result,
            SearchQuery::Range(RangeQuery {
                field: "price".to_string(),
                gte: None,
                gt: None,
                lte: None,
                lt: Some(serde_json::json!(100)),
            })
        );
    }

    #[test]
    fn test_range_lte() {
        let result = parse_query_string("price:<=100").unwrap();
        assert_eq!(
            result,
            SearchQuery::Range(RangeQuery {
                field: "price".to_string(),
                gte: None,
                gt: None,
                lte: Some(serde_json::json!(100)),
                lt: None,
            })
        );
    }

    #[test]
    fn test_range_between() {
        let result = parse_query_string("price:10..100").unwrap();
        assert_eq!(
            result,
            SearchQuery::Range(RangeQuery {
                field: "price".to_string(),
                gte: Some(serde_json::json!(10)),
                gt: None,
                lte: Some(serde_json::json!(100)),
                lt: None,
            })
        );
    }

    #[test]
    fn test_complex_expression() {
        let result =
            parse_query_string("status:active AND (tag:featured OR tag:promoted) NOT deleted:true")
                .unwrap();
        let bool_q = match result {
            SearchQuery::Bool(b) => b,
            other => panic!("expected Bool, got {other:?}"),
        };
        assert_eq!(bool_q.must.len(), 2); // status:active + the OR group
        assert_eq!(bool_q.must_not.len(), 1);
    }
}
