// src/pushdown.rs
//! Pure translation of DataFusion filter [`Expr`]s into PostgreSQL
//! `WHERE`-clause fragments, so [`crate::postgres_table::PostgresTable`] can
//! push predicates down to the source instead of fetching every row and
//! filtering locally.
//!
//! # Correctness model: everything is `Inexact`
//!
//! [`PostgresTable`](crate::postgres_table::PostgresTable) classifies every
//! translatable filter as
//! [`Inexact`](datafusion::logical_expr::TableProviderFilterPushDown::Inexact), never
//! `Exact`. Under `Inexact`, DataFusion keeps a local `Filter` above the scan
//! and re-applies the predicate to whatever rows the source returns. The
//! pushdown is therefore a pure **row-reduction optimization**: the source is
//! only required to return a *superset* of the matching rows, and correctness
//! never depends on the SQL we generate being a perfectly faithful rendering
//! of the DataFusion semantics. If in doubt about a construct we simply
//! classify it `Unsupported` and DataFusion does all the work.
//!
//! # Security
//!
//! No untrusted value is ever concatenated raw into SQL. Column names go
//! through the shared `quote_ident` helper (double-quote
//! escaping); string literals are wrapped in a single-quoted literal with any
//! embedded single quote doubled, and any string containing a NUL byte is
//! *rejected* (translated to `None`) rather than escaped, because PostgreSQL
//! cannot represent NUL in a text value. Non-finite floats (NaN/inf) are
//! likewise rejected. The result is that a hostile predicate value can only
//! ever land inside one quoted literal — it can never alter the structure of
//! the generated statement. This is exercised by the hostile-input tests in
//! this module and in `tests/pushdown.rs`.
//!
//! # Supported grammar (v1)
//!
//! ```text
//! predicate   := column cmp literal
//!              | literal cmp column          (operands flipped)
//!              | column IS NULL
//!              | column IS NOT NULL
//!              | column [NOT] IN (literal, ...)
//!              | predicate AND predicate
//! cmp         := = | <>                      (any supported literal)
//!              | < | <= | > | >=             (non-string literals only)
//! literal     := bool | intN | floatN(finite) | utf8(no NUL)
//! column      := a name present in the table's Arrow schema
//! ```
//!
//! String ORDERING comparisons are deliberately excluded: PostgreSQL orders
//! text by collation while Arrow compares bytes, and the two disagree in both
//! directions, so a pushed string ordering can return *less* than a superset
//! of the matching rows — breaking the `Inexact` model above (see
//! [`is_string_literal`]). String equality/`IN` remain pushable under
//! PostgreSQL's default deterministic collations.
//!
//! Everything else (`OR`, `LIKE`, `BETWEEN`, `CAST`, function calls, dates,
//! timestamps, decimals, `NULL` literals, subqueries, nested non-column/literal
//! operands, unknown columns) translates to `None` → `Unsupported`.

use arrow::datatypes::Schema;
use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};

use crate::postgres_table::quote_ident;

/// Translate a single DataFusion filter [`Expr`] into a PostgreSQL
/// `WHERE`-clause fragment, or `None` if the expression is outside the
/// supported v1 grammar (see the module docs).
///
/// The returned fragment is a complete boolean SQL expression (already
/// parenthesised where needed) safe to splice into a `WHERE`. `schema` is the
/// table's Arrow schema; only columns present in it are translatable.
///
/// This function is pure and side-effect free, which is what lets both
/// `supports_filters_pushdown` (classification) and `scan` (SQL generation)
/// share exactly one definition of "what can be pushed".
pub fn translate_filter(expr: &Expr, schema: &Schema) -> Option<String> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::And => {
                // DataFusion normally splits conjunctions before calling
                // supports_filters_pushdown, but handle an explicit AND too:
                // both sides must translate or the whole thing is unsupported.
                let l = translate_filter(left, schema)?;
                let r = translate_filter(right, schema)?;
                Some(format!("({} AND {})", l, r))
            }
            _ => translate_comparison(left, *op, right, schema),
        },
        Expr::IsNull(inner) => {
            let col = column_sql(as_column(inner)?, schema)?;
            Some(format!("{} IS NULL", col))
        }
        Expr::IsNotNull(inner) => {
            let col = column_sql(as_column(inner)?, schema)?;
            Some(format!("{} IS NOT NULL", col))
        }
        Expr::InList(InList {
            expr,
            list,
            negated,
        }) => translate_in_list(expr, list, *negated, schema),
        _ => None,
    }
}

/// Translate `column cmp literal` or `literal cmp column`.
fn translate_comparison(
    left: &Expr,
    op: Operator,
    right: &Expr,
    schema: &Schema,
) -> Option<String> {
    let sql_op = comparison_op_sql(op)?;
    // column <op> literal
    if let (Some(col), Some(lit)) = (as_column(left), as_literal(right)) {
        if is_ordering(op) && is_string_literal(lit) {
            return None;
        }
        let col = column_sql(col, schema)?;
        let lit = literal_sql(lit)?;
        return Some(format!("{} {} {}", col, sql_op, lit));
    }
    // literal <op> column  → flip the operator so the column stays on the left
    if let (Some(lit), Some(col)) = (as_literal(left), as_column(right)) {
        if is_ordering(op) && is_string_literal(lit) {
            return None;
        }
        let flipped = comparison_op_sql(flip_operator(op))?;
        let col = column_sql(col, schema)?;
        let lit = literal_sql(lit)?;
        return Some(format!("{} {} {}", col, flipped, lit));
    }
    None
}

/// Whether `op` is an ordering comparison (`<`, `<=`, `>`, `>=`).
fn is_ordering(op: Operator) -> bool {
    matches!(
        op,
        Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
    )
}

/// Whether the literal is a string. String ORDERING comparisons must not be
/// pushed down: PostgreSQL orders text by the column's collation (commonly
/// `en_US.UTF-8`-style dictionary order), while DataFusion/Arrow compares
/// bytes — and the two disagree in BOTH directions (e.g. `'B' < 'a'` byte-wise
/// but `'B' > 'a'` under dictionary collations). A pushed string ordering can
/// therefore UNDER-fetch, returning less than the superset of matching rows
/// that the `Inexact` re-filter model requires — the local re-filter cannot
/// resurrect rows the source never returned. String EQUALITY (`=`, `<>`, `IN`)
/// stays pushable: under PostgreSQL's default deterministic collations, text
/// equality is byte equality, matching Arrow exactly.
fn is_string_literal(v: &ScalarValue) -> bool {
    matches!(
        v,
        ScalarValue::Utf8(Some(_))
            | ScalarValue::LargeUtf8(Some(_))
            | ScalarValue::Utf8View(Some(_))
    )
}

/// Translate `column [NOT] IN (literal, ...)`. Requires a non-empty list of
/// pure literals; any non-literal element makes the whole predicate
/// unsupported.
fn translate_in_list(expr: &Expr, list: &[Expr], negated: bool, schema: &Schema) -> Option<String> {
    let col = column_sql(as_column(expr)?, schema)?;
    if list.is_empty() {
        return None;
    }
    let mut items = Vec::with_capacity(list.len());
    for item in list {
        items.push(literal_sql(as_literal(item)?)?);
    }
    let keyword = if negated { "NOT IN" } else { "IN" };
    Some(format!("{} {} ({})", col, keyword, items.join(", ")))
}

/// The SQL spelling of a comparison operator, or `None` for anything that
/// isn't one of the six supported comparisons.
fn comparison_op_sql(op: Operator) -> Option<&'static str> {
    match op {
        Operator::Eq => Some("="),
        Operator::NotEq => Some("<>"),
        Operator::Lt => Some("<"),
        Operator::LtEq => Some("<="),
        Operator::Gt => Some(">"),
        Operator::GtEq => Some(">="),
        _ => None,
    }
}

/// Flip a comparison operator so `literal <op> column` can be rewritten as
/// `column <flipped> literal`. Only the six comparison operators occur here.
fn flip_operator(op: Operator) -> Operator {
    match op {
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        // = and <> are symmetric.
        other => other,
    }
}

/// Extract a [`Column`] from an expression, or `None` if it isn't a bare
/// column reference.
fn as_column(expr: &Expr) -> Option<&Column> {
    match expr {
        Expr::Column(c) => Some(c),
        _ => None,
    }
}

/// Extract a [`ScalarValue`] literal from an expression, or `None`.
fn as_literal(expr: &Expr) -> Option<&ScalarValue> {
    match expr {
        Expr::Literal(v) => Some(v),
        _ => None,
    }
}

/// Render a column reference as quoted SQL, but only if a column of that name
/// exists in the table schema; unknown columns are not translatable.
fn column_sql(col: &Column, schema: &Schema) -> Option<String> {
    if schema.fields().iter().any(|f| f.name() == &col.name) {
        Some(quote_ident(&col.name))
    } else {
        None
    }
}

/// Render a scalar literal as a SQL literal, or `None` for any value outside
/// the supported set (NULLs, dates, timestamps, decimals, non-finite floats,
/// strings containing NUL, ...).
fn literal_sql(v: &ScalarValue) -> Option<String> {
    match v {
        ScalarValue::Boolean(Some(b)) => Some(if *b { "TRUE" } else { "FALSE" }.to_string()),
        ScalarValue::Int8(Some(i)) => Some(i.to_string()),
        ScalarValue::Int16(Some(i)) => Some(i.to_string()),
        ScalarValue::Int32(Some(i)) => Some(i.to_string()),
        ScalarValue::Int64(Some(i)) => Some(i.to_string()),
        ScalarValue::UInt8(Some(i)) => Some(i.to_string()),
        ScalarValue::UInt16(Some(i)) => Some(i.to_string()),
        ScalarValue::UInt32(Some(i)) => Some(i.to_string()),
        ScalarValue::UInt64(Some(i)) => Some(i.to_string()),
        // Rust's Display for floats emits the shortest decimal that round-trips
        // to the same IEEE value, so a float8 (double precision) column compares
        // exactly. Reject NaN/inf, which have no SQL literal spelling.
        ScalarValue::Float32(Some(f)) => finite_float_sql(*f as f64),
        ScalarValue::Float64(Some(f)) => finite_float_sql(*f),
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => escape_string_literal(s),
        _ => None,
    }
}

/// Render a finite float as a SQL numeric literal; `None` for NaN/inf.
fn finite_float_sql(f: f64) -> Option<String> {
    if f.is_finite() {
        Some(format!("{}", f))
    } else {
        None
    }
}

/// Escape a string into a single-quoted PostgreSQL string literal, doubling
/// any embedded single quote. Returns `None` if the string contains a NUL
/// byte (which PostgreSQL text cannot hold), rather than emitting anything
/// unsafe.
///
/// With `standard_conforming_strings` on (the PostgreSQL default since 9.1) a
/// backslash inside a regular `'...'` literal is an ordinary character, so no
/// backslash escaping is required or wanted. Newlines, double quotes, `;` and
/// `--` are all ordinary characters inside a quoted literal and cannot end it.
pub fn escape_string_literal(s: &str) -> Option<String> {
    if s.contains('\0') {
        return None;
    }
    Some(format!("'{}'", s.replace('\'', "''")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{binary_expr, col, lit, Expr};
    use datafusion::scalar::ScalarValue;

    fn schema() -> Schema {
        use arrow::datatypes::{DataType, Field};
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
            Field::new("active", DataType::Boolean, true),
        ])
    }

    fn t(expr: Expr) -> Option<String> {
        translate_filter(&expr, &schema())
    }

    // --- comparison operators, column <op> literal ---------------------------

    #[test]
    fn eq_int() {
        assert_eq!(t(col("id").eq(lit(42i64))).as_deref(), Some("\"id\" = 42"));
    }

    #[test]
    fn all_comparison_ops() {
        assert_eq!(t(col("id").eq(lit(1i64))).as_deref(), Some("\"id\" = 1"));
        assert_eq!(
            t(col("id").not_eq(lit(1i64))).as_deref(),
            Some("\"id\" <> 1")
        );
        assert_eq!(t(col("id").lt(lit(1i64))).as_deref(), Some("\"id\" < 1"));
        assert_eq!(
            t(col("id").lt_eq(lit(1i64))).as_deref(),
            Some("\"id\" <= 1")
        );
        assert_eq!(t(col("id").gt(lit(1i64))).as_deref(), Some("\"id\" > 1"));
        assert_eq!(
            t(col("id").gt_eq(lit(1i64))).as_deref(),
            Some("\"id\" >= 1")
        );
    }

    #[test]
    fn string_ordering_is_rejected() {
        // Collation order (PostgreSQL) and byte order (Arrow) disagree in both
        // directions, so a pushed string ordering could under-fetch. Must not
        // translate — in either operand order.
        assert_eq!(t(col("name").gt(lit("a"))), None);
        assert_eq!(t(col("name").lt(lit("a"))), None);
        assert_eq!(t(col("name").gt_eq(lit("a"))), None);
        assert_eq!(t(col("name").lt_eq(lit("a"))), None);
        assert_eq!(
            t(binary_expr(lit("a"), Operator::Lt, col("name"))),
            None,
            "flipped operand order must be rejected too"
        );
    }

    #[test]
    fn string_equality_still_pushed() {
        assert_eq!(
            t(col("name").eq(lit("a"))).as_deref(),
            Some("\"name\" = 'a'")
        );
        assert_eq!(
            t(col("name").not_eq(lit("a"))).as_deref(),
            Some("\"name\" <> 'a'")
        );
    }

    #[test]
    fn literal_on_left_flips_operator() {
        // 5 < id  ==>  "id" > 5
        let expr = binary_expr(lit(5i64), Operator::Lt, col("id"));
        assert_eq!(t(expr).as_deref(), Some("\"id\" > 5"));
        // 5 >= id ==> "id" <= 5
        let expr = binary_expr(lit(5i64), Operator::GtEq, col("id"));
        assert_eq!(t(expr).as_deref(), Some("\"id\" <= 5"));
        // symmetric ops keep their spelling
        let expr = binary_expr(lit(5i64), Operator::Eq, col("id"));
        assert_eq!(t(expr).as_deref(), Some("\"id\" = 5"));
    }

    // --- literal types -------------------------------------------------------

    #[test]
    fn int_widths() {
        assert_eq!(
            translate_filter(
                &col("id").eq(Expr::Literal(ScalarValue::Int8(Some(1)))),
                &schema()
            )
            .as_deref(),
            Some("\"id\" = 1")
        );
        assert_eq!(
            translate_filter(
                &col("id").eq(Expr::Literal(ScalarValue::Int16(Some(2)))),
                &schema()
            )
            .as_deref(),
            Some("\"id\" = 2")
        );
        assert_eq!(
            translate_filter(
                &col("id").eq(Expr::Literal(ScalarValue::Int32(Some(3)))),
                &schema()
            )
            .as_deref(),
            Some("\"id\" = 3")
        );
        assert_eq!(
            translate_filter(
                &col("id").eq(Expr::Literal(ScalarValue::UInt64(Some(4)))),
                &schema()
            )
            .as_deref(),
            Some("\"id\" = 4")
        );
    }

    #[test]
    fn bool_literal() {
        assert_eq!(
            t(col("active").eq(lit(true))).as_deref(),
            Some("\"active\" = TRUE")
        );
        assert_eq!(
            t(col("active").eq(lit(false))).as_deref(),
            Some("\"active\" = FALSE")
        );
    }

    #[test]
    fn finite_float_literals() {
        assert_eq!(
            t(col("score").gt(lit(4.5f64))).as_deref(),
            Some("\"score\" > 4.5")
        );
        // integral-valued float still renders as a valid numeric literal
        assert_eq!(
            t(col("score").eq(lit(6.0f64))).as_deref(),
            Some("\"score\" = 6")
        );
    }

    #[test]
    fn non_finite_float_is_rejected() {
        assert_eq!(t(col("score").gt(lit(f64::NAN))), None);
        assert_eq!(t(col("score").gt(lit(f64::INFINITY))), None);
        assert_eq!(t(col("score").gt(lit(f64::NEG_INFINITY))), None);
    }

    #[test]
    fn null_literal_is_unsupported() {
        // A typed NULL literal is not translatable; IS NULL is the supported form.
        assert_eq!(
            translate_filter(
                &col("id").eq(Expr::Literal(ScalarValue::Int64(None))),
                &schema()
            ),
            None
        );
    }

    // --- string escaping (security-critical) ---------------------------------

    #[test]
    fn plain_string() {
        assert_eq!(
            t(col("name").eq(lit("vip"))).as_deref(),
            Some("\"name\" = 'vip'")
        );
    }

    #[test]
    fn string_with_single_quote_is_doubled() {
        assert_eq!(
            t(col("name").eq(lit("O'Brien"))).as_deref(),
            Some("\"name\" = 'O''Brien'")
        );
    }

    #[test]
    fn escape_string_literal_doubles_quotes() {
        assert_eq!(escape_string_literal("a'b").as_deref(), Some("'a''b'"));
    }

    #[test]
    fn escape_string_literal_rejects_nul() {
        assert_eq!(escape_string_literal("a\0b"), None);
    }

    /// Property-style loop over a hostile corpus: every input must either be
    /// rejected (`None`) or rendered as exactly one single-quoted literal whose
    /// only unescaped single quotes are the enclosing pair, so nothing can
    /// alter the SQL structure.
    #[test]
    fn hostile_strings_never_break_out_of_the_literal() {
        let corpus = [
            "'",
            "''",
            "'''",
            "; DROP TABLE x; --",
            "x'; DROP TABLE users; --",
            "\\",
            "back\\slash",
            "line\nbreak",
            "tab\there",
            "double\"quote",
            "mixed '\" \\ ; -- end",
            "100% \u{1f600} unicode",
            "quote at end'",
            "'quote at start",
        ];
        for input in corpus {
            let expr = col("name").eq(lit(input));
            let Some(sql) = translate_filter(&expr, &schema()) else {
                continue; // rejected — trivially safe
            };
            let prefix = "\"name\" = ";
            assert!(sql.starts_with(prefix), "unexpected prefix in {:?}", sql);
            let literal = &sql[prefix.len()..];
            assert_structurally_safe_literal(literal, input);
        }
    }

    /// A well-formed single-quoted literal: starts and ends with `'`, and every
    /// interior single quote is part of a doubled `''` pair (so it never ends
    /// the literal early). Also verifies it decodes back to the original input.
    fn assert_structurally_safe_literal(literal: &str, original: &str) {
        assert!(
            literal.starts_with('\'') && literal.ends_with('\'') && literal.len() >= 2,
            "literal {:?} is not single-quote wrapped",
            literal
        );
        let inner = &literal[1..literal.len() - 1];
        // Every single quote in the interior must be doubled. Walk it and
        // reconstruct the decoded value; also assert no lone quote exists.
        let mut decoded = String::new();
        let mut chars = inner.chars().peekable();
        while let Some(c) = chars.next() {
            if c == '\'' {
                assert_eq!(
                    chars.next(),
                    Some('\''),
                    "lone single quote inside literal {:?} would end it early",
                    literal
                );
                decoded.push('\'');
            } else {
                decoded.push(c);
            }
        }
        assert_eq!(
            decoded, original,
            "literal {:?} does not decode back to input {:?}",
            literal, original
        );
    }

    // --- IS NULL / IS NOT NULL ----------------------------------------------

    #[test]
    fn is_null_and_is_not_null() {
        assert_eq!(
            t(col("name").is_null()).as_deref(),
            Some("\"name\" IS NULL")
        );
        assert_eq!(
            t(col("name").is_not_null()).as_deref(),
            Some("\"name\" IS NOT NULL")
        );
    }

    // --- IN / NOT IN ---------------------------------------------------------

    #[test]
    fn in_list_of_literals() {
        let expr = col("id").in_list(vec![lit(1i64), lit(2i64), lit(3i64)], false);
        assert_eq!(t(expr).as_deref(), Some("\"id\" IN (1, 2, 3)"));
    }

    #[test]
    fn not_in_list_of_literals() {
        let expr = col("id").in_list(vec![lit(1i64), lit(2i64)], true);
        assert_eq!(t(expr).as_deref(), Some("\"id\" NOT IN (1, 2)"));
    }

    #[test]
    fn in_list_of_strings_is_escaped() {
        let expr = col("name").in_list(vec![lit("a'b"), lit("c")], false);
        assert_eq!(t(expr).as_deref(), Some("\"name\" IN ('a''b', 'c')"));
    }

    #[test]
    fn in_list_with_non_literal_is_unsupported() {
        // id IN (name)  — a column in the list is not a pure literal
        let expr = col("id").in_list(vec![col("name")], false);
        assert_eq!(t(expr), None);
    }

    // --- AND -----------------------------------------------------------------

    #[test]
    fn and_of_supported_is_combined() {
        let expr = col("id").gt(lit(5i64)).and(col("name").eq(lit("x")));
        assert_eq!(t(expr).as_deref(), Some("(\"id\" > 5 AND \"name\" = 'x')"));
    }

    #[test]
    fn and_with_unsupported_side_is_unsupported() {
        // Right side is OR (unsupported), so the whole AND is unsupported.
        let unsupported = col("id").eq(lit(1i64)).or(col("id").eq(lit(2i64)));
        let expr = col("id").gt(lit(5i64)).and(unsupported);
        assert_eq!(t(expr), None);
    }

    // --- unsupported shapes --------------------------------------------------

    #[test]
    fn or_is_unsupported() {
        let expr = col("id").eq(lit(1i64)).or(col("id").eq(lit(2i64)));
        assert_eq!(t(expr), None);
    }

    #[test]
    fn unknown_column_is_unsupported() {
        let expr = col("nonexistent").eq(lit(1i64));
        assert_eq!(t(expr), None);
    }

    #[test]
    fn like_is_unsupported() {
        let expr = col("name").like(lit("a%"));
        assert_eq!(t(expr), None);
    }

    #[test]
    fn between_is_unsupported() {
        let expr = col("id").between(lit(1i64), lit(10i64));
        assert_eq!(t(expr), None);
    }

    #[test]
    fn column_op_column_is_unsupported() {
        // id = id is neither column-vs-literal nor literal-vs-column.
        let expr = col("id").eq(col("id"));
        assert_eq!(t(expr), None);
    }
}
