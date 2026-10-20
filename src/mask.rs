//! Masking files (`template --mask`): which columns to replace and which rows to keep, and the SQL
//! that does it. Every masking strategy is idempotent, so a layered build can mask again in place.

use crate::error::{ErrorCode, MitosisError, Result};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::Path;

/// How one column is replaced.
#[derive(Clone, Debug, PartialEq)]
pub enum Strategy {
    Email,
    Name,
    Hash,
    Null,
    Redact,
    Const(String),
    Keep,
}

/// Which rows of a table a subset keeps.
#[derive(Clone, Debug, PartialEq)]
pub enum SubsetRule {
    /// A SQL condition; rows for which it is false are deleted.
    Where(String),
    /// A percent (0 < p <= 100) of the table's pages, by `TABLESAMPLE SYSTEM`.
    Sample(f64),
}

/// A parsed masking file. Table keys are `table` (schema `public`) or `schema.table`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MaskFile {
    pub tables: BTreeMap<String, BTreeMap<String, Strategy>>,
    pub subset: BTreeMap<String, SubsetRule>,
}

/// One column of a user table, from the catalog.
#[derive(Clone, Debug, Deserialize)]
pub struct ColumnInfo {
    pub schema: String,
    pub table: String,
    pub column: String,
    /// `format_type` output, like `character varying(40)`.
    pub data_type: String,
    pub not_null: bool,
}

/// One foreign key, from the catalog: tables as quoted, schema-qualified SQL (`"s"."t"`),
/// columns as quoted identifiers, in key order.
#[derive(Clone, Debug, Deserialize)]
pub struct ForeignKey {
    pub child: String,
    pub child_cols: Vec<String>,
    pub parent: String,
    pub parent_cols: Vec<String>,
}

/// The temp table that holds one sampled table's row ids while its other rows are deleted.
const SAMPLE_TABLE: &str = "mitosis_sample";

/// Foreign-key cleanup passes after a subset before the build gives up.
const MAX_PASSES: u32 = 100;

fn invalid(msg: impl Into<String>) -> MitosisError {
    MitosisError::new(ErrorCode::InvalidRequest, msg)
}

fn parse_strategy(s: &str) -> Option<Strategy> {
    Some(match s {
        "email" => Strategy::Email,
        "name" => Strategy::Name,
        "hash" => Strategy::Hash,
        "null" => Strategy::Null,
        "redact" => Strategy::Redact,
        "keep" => Strategy::Keep,
        _ => Strategy::Const(s.strip_prefix("const:")?.to_string()),
    })
}

fn parse_subset(key: &str, s: &str) -> Result<SubsetRule> {
    let t = s.trim();
    if let Some(p) = t
        .strip_suffix('%')
        .and_then(|p| p.trim().parse::<f64>().ok())
    {
        if p.is_nan() || p <= 0.0 || p > 100.0 {
            return Err(invalid(format!(
                "subset.{key}: the sample percent must be above 0 and at most 100, not {p}"
            )));
        }
        return Ok(SubsetRule::Sample(p));
    }
    if t.is_empty() {
        return Err(invalid(format!(
            "subset.{key} needs a filter (a SQL condition) or a percent like \"10%\""
        )));
    }
    Ok(SubsetRule::Where(t.to_string()))
}

impl MaskFile {
    /// Parses masking-file TOML. Errors name the line (syntax) or the key (meaning).
    pub fn parse(text: &str) -> Result<MaskFile> {
        let root: toml::Table = toml::from_str(text).map_err(|e| invalid(e.to_string()))?;
        let mut m = MaskFile::default();
        for (key, value) in root {
            match key.as_str() {
                "tables" => {
                    let tables = value
                        .as_table()
                        .ok_or_else(|| invalid("tables must be a table, like [tables.users]"))?;
                    for (t, cols) in tables {
                        let cols = cols.as_table().ok_or_else(|| {
                            invalid(format!(
                                "tables.{t} must be a table of column = \"strategy\""
                            ))
                        })?;
                        let mut out = BTreeMap::new();
                        for (c, s) in cols {
                            let s = s.as_str().ok_or_else(|| {
                                if s.is_table() {
                                    invalid(format!(
                                        "tables.{t}.{c} must be a string; for a schema-qualified \
                                         table, quote the key: [tables.\"{t}.{c}\"]"
                                    ))
                                } else {
                                    invalid(format!("tables.{t}.{c} must be a string"))
                                }
                            })?;
                            let strategy = parse_strategy(s).ok_or_else(|| {
                                invalid(format!(
                                    "tables.{t}.{c}: unknown strategy {s:?} (use email, name, hash, \
                                     null, redact, const:<value> or keep)"
                                ))
                            })?;
                            out.insert(c.clone(), strategy);
                        }
                        m.tables.insert(t.clone(), out);
                    }
                }
                "subset" => {
                    let rules = value.as_table().ok_or_else(|| {
                        invalid("subset must be a table of table = \"filter or percent\"")
                    })?;
                    for (t, s) in rules {
                        let s = s.as_str().ok_or_else(|| {
                            if s.is_table() {
                                invalid(format!(
                                    "subset.{t} must be a string; for a schema-qualified table, \
                                     quote the key: \"{t}.<table>\" = \"...\""
                                ))
                            } else {
                                invalid(format!("subset.{t} must be a string"))
                            }
                        })?;
                        m.subset.insert(t.clone(), parse_subset(t, s)?);
                    }
                }
                other => {
                    return Err(invalid(format!(
                        "unknown key {other} (a masking file has [tables.<table>] and [subset])"
                    )))
                }
            }
        }
        Ok(m)
    }

    /// Reads and parses the masking file at `path`; errors name the file.
    pub fn load(path: &Path) -> Result<MaskFile> {
        let named = |msg: String| invalid(format!("masking file {}: {msg}", path.display()));
        let text = std::fs::read_to_string(path).map_err(|e| named(e.to_string()))?;
        MaskFile::parse(&text).map_err(|e| named(e.message))
    }

    /// FNV-1a 64 of the file's bytes, as hex (like the migrations folder hash).
    pub fn hash(path: &Path) -> Result<String> {
        let data = std::fs::read(path)
            .map_err(|e| invalid(format!("masking file {}: {e}", path.display())))?;
        Ok(hash_bytes(&data))
    }

    /// `load` and `hash` from one read, so the parsed file is the one the hash names.
    pub fn load_hashed(path: &Path) -> Result<(MaskFile, String)> {
        let named = |msg: String| invalid(format!("masking file {}: {msg}", path.display()));
        let data = std::fs::read(path).map_err(|e| named(e.to_string()))?;
        let hash = hash_bytes(&data);
        let text = String::from_utf8(data).map_err(|e| named(e.to_string()))?;
        let file = MaskFile::parse(&text).map_err(|e| named(e.message))?;
        Ok((file, hash))
    }
}

fn hash_bytes(data: &[u8]) -> String {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    crate::templates::fnv1a(&mut h, data);
    format!("{h:016x}")
}

fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// `users` -> (`public`, `users`); `billing.cards` -> (`billing`, `cards`).
fn split_table(name: &str) -> (&str, &str) {
    name.split_once('.').unwrap_or(("public", name))
}

fn qualified(name: &str) -> String {
    let (schema, table) = split_table(name);
    format!("{}.{}", quote_ident(schema), quote_ident(table))
}

/// text, varchar, char and citext (also schema-qualified); not arrays or domains.
fn is_text(data_type: &str) -> bool {
    if data_type.ends_with(']') {
        return false;
    }
    let base = data_type.split('(').next().unwrap_or("").trim();
    let base = base.rsplit('.').next().unwrap_or(base);
    matches!(
        base,
        "text" | "character varying" | "varchar" | "character" | "char" | "bpchar" | "citext"
    )
}

fn strategy_name(s: &Strategy) -> &'static str {
    match s {
        Strategy::Email => "email",
        Strategy::Name => "name",
        Strategy::Hash => "hash",
        Strategy::Null => "null",
        Strategy::Redact => "redact",
        Strategy::Const(_) => "const",
        Strategy::Keep => "keep",
    }
}

/// For one masked column: the condition under which a row still needs masking (NULL for a NULL
/// value, so NULLs stay NULL) and the new value. `None` for `keep`. The checks compare `col::text`,
/// which drops a `char(n)` column's padding, so a masked value is recognised there too.
fn column_sql(
    table: &str,
    info: &ColumnInfo,
    strategy: &Strategy,
) -> Result<Option<(String, String)>> {
    let c = quote_ident(&info.column);
    let name = format!("{table}.{}", info.column);
    let text_only = matches!(
        strategy,
        Strategy::Email | Strategy::Name | Strategy::Hash | Strategy::Redact
    );
    if text_only && !is_text(&info.data_type) {
        return Err(invalid(format!(
            "column {name} is {}, and {} works only on text, varchar, char and citext columns",
            info.data_type,
            strategy_name(strategy)
        )));
    }
    // The salt comes from a setting, so it never appears in the statement text.
    let h = format!(
        "encode(sha256(decode(current_setting('{SALT_SETTING}'),'hex') || convert_to({c}::text,'UTF8')),'hex')"
    );
    Ok(Some(match strategy {
        Strategy::Keep => return Ok(None),
        Strategy::Email => (
            format!("{c}::text NOT LIKE '%@masked.invalid'"),
            format!("'u_' || left({h},12) || '@masked.invalid'"),
        ),
        Strategy::Name => (
            format!("{c}::text NOT LIKE 'Masked %'"),
            format!("'Masked ' || left({h},8)"),
        ),
        Strategy::Hash => (
            format!("{c}::text NOT LIKE 'mh\\_%'"),
            format!("'mh_' || {h}"),
        ),
        Strategy::Redact => (
            format!("{c}::text <> repeat('x', length({c}))"),
            format!("repeat('x', length({c}))"),
        ),
        Strategy::Null => {
            if info.not_null {
                return Err(invalid(format!(
                    "column {name} is NOT NULL, so it cannot be masked with null"
                )));
            }
            (format!("{c} IS NOT NULL"), "NULL".into())
        }
        Strategy::Const(v) => {
            let lit = format!("{}::{}", quote_literal(v), info.data_type);
            (format!("{c}::text <> ({lit})::text"), lit)
        }
    }))
}

/// The setting the masking statements read the salt from.
const SALT_SETTING: &str = "mitosis.salt";

/// `SET LOCAL mitosis.salt`, then one `UPDATE` per masked table, setting all its masked columns
/// (empty when nothing is masked). Only the `SET` holds the salt; it must run in the same
/// transaction as the updates. Rows already masked are left alone, so running the statements
/// twice changes nothing. Unknown tables or columns, and strategies that do not fit the column,
/// are errors naming the column.
pub fn mask_statements(
    m: &MaskFile,
    salt_hex: &str,
    columns: &[ColumnInfo],
) -> Result<Vec<String>> {
    if salt_hex.is_empty() || !salt_hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(MitosisError::new(
            ErrorCode::Internal,
            "the masking salt is not hex",
        ));
    }
    let mut out = Vec::new();
    for (table, cols) in &m.tables {
        let (schema, name) = split_table(table);
        let known: Vec<&ColumnInfo> = columns
            .iter()
            .filter(|c| c.schema == schema && c.table == name)
            .collect();
        if known.is_empty() {
            return Err(invalid(format!("masked table {table} does not exist")));
        }
        let (mut sets, mut needs) = (Vec::new(), Vec::new());
        for (col, strategy) in cols {
            let info = known
                .iter()
                .find(|c| c.column == *col)
                .ok_or_else(|| invalid(format!("masked column {table}.{col} does not exist")))?;
            let Some((need, value)) = column_sql(table, info, strategy)? else {
                continue;
            };
            let c = quote_ident(col);
            sets.push(format!("{c} = CASE WHEN {need} THEN {value} ELSE {c} END"));
            needs.push(format!("({need})"));
        }
        if !sets.is_empty() {
            out.push(format!(
                "UPDATE {} SET {} WHERE {}",
                qualified(table),
                sets.join(", "),
                needs.join(" OR ")
            ));
        }
    }
    if !out.is_empty() {
        out.insert(0, format!("SET LOCAL {SALT_SETTING} = '{salt_hex}'"));
    }
    Ok(out)
}

/// The subset deletes, then a loop that deletes rows whose foreign-key parent is gone until a
/// full pass deletes nothing (at most `MAX_PASSES`). Empty without a subset. Run with
/// `session_replication_role = replica`, so foreign keys do not block the deletes meanwhile.
pub fn subset_statements(m: &MaskFile, fks: &[ForeignKey]) -> Result<Vec<String>> {
    let mut out = Vec::new();
    if m.subset.is_empty() {
        return Ok(Vec::new());
    }
    for (table, rule) in &m.subset {
        let t = qualified(table);
        out.push(match rule {
            SubsetRule::Where(w) => vec![format!("DELETE FROM {t} WHERE ({w}) IS NOT TRUE")],
            // The sampled row ids go into a temp table and the delete is an anti-join against
            // it, which hashes or merges (and spills to disk) instead of rescanning per row.
            // `tableoid` keeps partitions apart: their ctids repeat.
            SubsetRule::Sample(p) => vec![
                format!(
                    "CREATE TEMP TABLE {SAMPLE_TABLE} AS \
                     SELECT tableoid AS o, ctid AS k FROM {t} TABLESAMPLE SYSTEM ({p})"
                ),
                format!("ANALYZE {SAMPLE_TABLE}"),
                format!(
                    "DELETE FROM {t} AS x WHERE NOT EXISTS \
                     (SELECT 1 FROM {SAMPLE_TABLE} s WHERE s.o = x.tableoid AND s.k = x.ctid)"
                ),
                format!("DROP TABLE {SAMPLE_TABLE}"),
            ],
        });
    }
    let mut out: Vec<String> = out.into_iter().flatten().collect();
    if fks.is_empty() {
        return Ok(out);
    }
    let mut body = String::new();
    for fk in fks {
        if fk.child_cols.is_empty() || fk.child_cols.len() != fk.parent_cols.len() {
            return Err(MitosisError::new(
                ErrorCode::Internal,
                format!(
                    "foreign key from {} to {} has unmatched columns",
                    fk.child, fk.parent
                ),
            ));
        }
        let cols: Vec<String> = fk.child_cols.iter().map(|c| format!("c.{c}")).collect();
        let on: Vec<String> = fk
            .parent_cols
            .iter()
            .zip(&fk.child_cols)
            .map(|(p, c)| format!("p.{p} = c.{c}"))
            .collect();
        body.push_str(&format!(
            "    DELETE FROM {} AS c WHERE ({}) IS NOT NULL AND NOT EXISTS \
             (SELECT 1 FROM {} AS p WHERE {});\n    GET DIAGNOSTICS n = ROW_COUNT;\n    total := total + n;\n",
            fk.child,
            cols.join(", "),
            fk.parent,
            on.join(" AND ")
        ));
    }
    out.push(format!(
        "DO $mitosis$\nDECLARE n bigint; total bigint; passes int := 0;\nBEGIN\n  LOOP\n    total := 0;\n{body}    \
         EXIT WHEN total = 0;\n    passes := passes + 1;\n    IF passes >= {MAX_PASSES} THEN\n      \
         RAISE EXCEPTION 'subset: rows referencing deleted rows were still being deleted after {MAX_PASSES} passes';\n    \
         END IF;\n  END LOOP;\nEND\n$mitosis$"
    ));
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXAMPLE: &str = r#"
[tables.users]
email = "email"        # deterministic fake email
full_name = "name"     # deterministic fake name
phone = "null"
ssn = "hash"           # keyed hash, hex
notes = "redact"       # same-length run of 'x'
plan = "const:free"

[subset]
orders = "created_at > now() - interval '90 days'"
events = "10%"          # TABLESAMPLE SYSTEM
"#;

    fn col(table: &str, column: &str, data_type: &str, not_null: bool) -> ColumnInfo {
        let (schema, table) = split_table(table);
        ColumnInfo {
            schema: schema.into(),
            table: table.into(),
            column: column.into(),
            data_type: data_type.into(),
            not_null,
        }
    }

    fn users() -> Vec<ColumnInfo> {
        vec![
            col("users", "id", "integer", true),
            col("users", "email", "text", true),
            col("users", "full_name", "character varying(80)", false),
            col("users", "phone", "text", false),
            col("users", "ssn", "citext", false),
            col("users", "notes", "character(20)", false),
            col("users", "plan", "text", false),
        ]
    }

    #[test]
    fn parse_full_example() {
        let m = MaskFile::parse(EXAMPLE).unwrap();
        let u = &m.tables["users"];
        assert_eq!(u["email"], Strategy::Email);
        assert_eq!(u["full_name"], Strategy::Name);
        assert_eq!(u["phone"], Strategy::Null);
        assert_eq!(u["ssn"], Strategy::Hash);
        assert_eq!(u["notes"], Strategy::Redact);
        assert_eq!(u["plan"], Strategy::Const("free".into()));
        assert_eq!(
            m.subset["orders"],
            SubsetRule::Where("created_at > now() - interval '90 days'".into())
        );
        assert_eq!(m.subset["events"], SubsetRule::Sample(10.0));
        let q = MaskFile::parse("[tables.\"billing.cards\"]\nnumber = \"keep\"").unwrap();
        assert_eq!(q.tables["billing.cards"]["number"], Strategy::Keep);
    }

    #[test]
    fn parse_errors_name_key() {
        let msg = |text: &str| MaskFile::parse(text).unwrap_err().message;
        assert!(msg("[tables.users]\nemail = \"fake\"").contains("tables.users.email"));
        assert!(msg("[tables.users]\nemail = 3").contains("tables.users.email"));
        assert!(msg("[tables]\nusers = \"email\"").contains("tables.users"));
        assert!(msg("[subset]\norders = \"\"").contains("subset.orders"));
        assert!(msg("[subset]\norders = 1").contains("subset.orders"));
        assert!(msg("[mask]\nx = 1").contains("unknown key mask"));
        // An unquoted dotted table key gets a hint to quote it.
        assert!(
            msg("[tables.billing.cards]\nnumber = \"keep\"").contains("[tables.\"billing.cards\"]")
        );
        assert!(msg("[subset]\nbilling.cards = \"1%\"").contains("quote the key"));
        // A syntax error names the line.
        assert!(
            msg("[tables.users]\nemail = \n").contains("line 2"),
            "{}",
            msg("[tables.users]\nemail = \n")
        );
    }

    #[test]
    fn sample_percent_bounds() {
        let rule = |s: &str| parse_subset("t", s);
        assert_eq!(rule("100%").unwrap(), SubsetRule::Sample(100.0));
        assert_eq!(rule("0.5 %").unwrap(), SubsetRule::Sample(0.5));
        for bad in ["0%", "-1%", "100.1%", "NaN%", "inf%"] {
            let e = rule(bad).unwrap_err();
            assert!(e.message.contains("subset.t"), "{bad}: {}", e.message);
        }
        // Not a number before the % sign: a filter.
        assert_eq!(
            rule("name LIKE 'a%'").unwrap(),
            SubsetRule::Where("name LIKE 'a%'".into())
        );
    }

    #[test]
    fn statements_skip_already_masked() {
        let m = MaskFile::parse(EXAMPLE).unwrap();
        let sql = mask_statements(&m, "00ff", &users()).unwrap();
        assert_eq!(sql.len(), 2, "the salt, then one UPDATE per table");
        assert_eq!(sql[0], "SET LOCAL mitosis.salt = '00ff'");
        let s = &sql[1];
        assert!(!s.contains("00ff"), "the salt stays out of the UPDATE: {s}");
        assert!(s.starts_with("UPDATE \"public\".\"users\" SET "), "{s}");
        assert!(
            s.contains("\"email\"::text NOT LIKE '%@masked.invalid'"),
            "{s}"
        );
        assert!(s.contains("'u_' || left(encode(sha256(decode(current_setting('mitosis.salt'),'hex') || convert_to(\"email\"::text,'UTF8')),'hex'),12) || '@masked.invalid'"), "{s}");
        assert!(s.contains("\"full_name\"::text NOT LIKE 'Masked %'"), "{s}");
        assert!(s.contains("'Masked ' || left(encode("), "{s}");
        assert!(s.contains("\"ssn\"::text NOT LIKE 'mh\\_%'"), "{s}");
        assert!(s.contains("'mh_' || encode("), "{s}");
        assert!(
            s.contains("\"notes\"::text <> repeat('x', length(\"notes\"))"),
            "{s}"
        );
        assert!(
            s.contains("\"phone\" = CASE WHEN \"phone\" IS NOT NULL THEN NULL"),
            "{s}"
        );
        assert!(s.contains("'free'::text"), "{s}");
        assert!(s.contains(" WHERE (\"email\"::text NOT LIKE"), "{s}");

        let keep = MaskFile::parse("[tables.users]\nemail = \"keep\"").unwrap();
        assert!(mask_statements(&keep, "00", &users()).unwrap().is_empty());
    }

    #[test]
    fn email_rerun_on_char_column_matches_the_unpadded_value() {
        // A char(40) value is padded with spaces; LIKE on the column itself would see them and
        // mask an already-masked value again on every layered build.
        let cols = vec![col("users", "alt", "character(40)", false)];
        let m = MaskFile::parse("[tables.users]\nalt = \"email\"").unwrap();
        let sql = mask_statements(&m, "00", &cols).unwrap();
        assert!(
            sql[1].contains("WHEN \"alt\"::text NOT LIKE '%@masked.invalid' THEN"),
            "{}",
            sql[1]
        );
        assert!(sql[1].contains("WHERE (\"alt\"::text NOT LIKE '%@masked.invalid')"));
    }

    #[test]
    fn statements_check_columns_and_types() {
        let err = |text: &str| {
            mask_statements(&MaskFile::parse(text).unwrap(), "00", &users())
                .unwrap_err()
                .message
        };
        assert!(err("[tables.users]\nid = \"email\"").contains("users.id is integer"));
        assert!(err("[tables.users]\nemail = \"null\"").contains("users.email is NOT NULL"));
        assert!(err("[tables.users]\nnope = \"hash\"").contains("users.nope does not exist"));
        assert!(err("[tables.people]\nemail = \"hash\"").contains("table people does not exist"));
        assert!(!is_text("text[]") && !is_text("character varying(4)[]") && !is_text("\"char\""));
        assert!(is_text("public.citext") && is_text("character(3)"));
    }

    #[test]
    fn subset_statements_delete_orphans() {
        let m = MaskFile::parse(EXAMPLE).unwrap();
        let fks = vec![ForeignKey {
            child: "\"public\".\"order_items\"".into(),
            child_cols: vec!["\"order_id\"".into()],
            parent: "\"public\".\"orders\"".into(),
            parent_cols: vec!["\"id\"".into()],
        }];
        let sql = subset_statements(&m, &fks).unwrap();
        assert_eq!(sql[0], "CREATE TEMP TABLE mitosis_sample AS SELECT tableoid AS o, ctid AS k FROM \"public\".\"events\" TABLESAMPLE SYSTEM (10)");
        assert_eq!(sql[1], "ANALYZE mitosis_sample");
        assert_eq!(sql[2], "DELETE FROM \"public\".\"events\" AS x WHERE NOT EXISTS (SELECT 1 FROM mitosis_sample s WHERE s.o = x.tableoid AND s.k = x.ctid)");
        assert_eq!(sql[3], "DROP TABLE mitosis_sample");
        assert_eq!(
            sql[4],
            "DELETE FROM \"public\".\"orders\" WHERE (created_at > now() - interval '90 days') IS NOT TRUE"
        );
        assert!(sql[5].contains("DELETE FROM \"public\".\"order_items\" AS c WHERE (c.\"order_id\") IS NOT NULL AND NOT EXISTS (SELECT 1 FROM \"public\".\"orders\" AS p WHERE p.\"id\" = c.\"order_id\")"), "{}", sql[5]);
        assert!(sql[5].contains("passes >= 100"));
        assert!(subset_statements(&MaskFile::default(), &fks)
            .unwrap()
            .is_empty());
    }
}
