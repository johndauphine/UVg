pub mod mssql;
pub mod mysql;
pub mod pg;
pub mod sqlite;

use crate::dialect::Dialect;
use crate::schema::ColumnInfo;

/// Canonical type representation for cross-dialect type translation.
#[derive(Debug, Clone, PartialEq)]
pub enum CanonicalType {
    Boolean,
    SmallInt,
    Integer,
    BigInt,
    Float,
    Double,
    Decimal {
        precision: Option<i32>,
        scale: Option<i32>,
    },
    Varchar {
        length: Option<i32>,
    },
    Char {
        length: Option<i32>,
    },
    Text,
    Bytes {
        length: Option<i32>,
    },
    Date,
    Time {
        with_tz: bool,
        /// Sub-second precision in digits (0–6). Currently meaningful only
        /// on MySQL (TIME(N)). PG/MSSQL/SQLite ignore it on emission.
        /// `None` means unspecified — emit without a precision suffix.
        precision: Option<u8>,
    },
    Timestamp {
        with_tz: bool,
        /// Sub-second precision in digits. Same semantics as Time.precision.
        /// MySQL DATETIME(N) and TIMESTAMP(N) preserve this; other dialects
        /// drop it on emission. Round-tripping mysql→mysql with a precision
        /// default like `CURRENT_TIMESTAMP(6)` requires the column type to
        /// carry the same N. See #36.
        precision: Option<u8>,
    },
    Interval,
    Uuid,
    Json,
    Jsonb,
    Enum {
        values: Vec<String>,
    },
    /// MySQL `SET('a','b','c')` — multi-value column. Native to MySQL only.
    /// Other dialects fall back to a VARCHAR sized to fit the
    /// comma-joined values; the SET semantic (zero or more values from
    /// the list) is lost in the fallback. See #38.
    Set {
        values: Vec<String>,
    },
    Array {
        element: Box<CanonicalType>,
    },
    /// Non-portable type passed through as-is.
    Raw {
        type_name: String,
    },
}

/// Result of mapping a canonical type to a target DDL type string.
#[derive(Debug, Clone)]
pub struct DdlType {
    /// The SQL type string, e.g. "INTEGER", "VARCHAR(100)", "TIMESTAMP WITH TIME ZONE".
    pub sql_type: String,
    /// Whether this was a lossy translation (no exact equivalent in target).
    pub is_approximate: bool,
    /// Optional warning about the translation.
    pub warning: Option<String>,
}

impl DdlType {
    fn exact(sql_type: &str) -> Self {
        DdlType {
            sql_type: sql_type.to_string(),
            is_approximate: false,
            warning: None,
        }
    }

    fn approx(sql_type: &str, warning: &str) -> Self {
        DdlType {
            sql_type: sql_type.to_string(),
            is_approximate: true,
            warning: Some(warning.to_string()),
        }
    }
}

/// Normalize a source column to a canonical type.
pub fn to_canonical(col: &ColumnInfo, dialect: Dialect) -> CanonicalType {
    match dialect {
        Dialect::Postgres => pg::to_canonical(col),
        Dialect::Mysql => mysql::to_canonical(col),
        Dialect::Mssql => mssql::to_canonical(col),
        Dialect::Sqlite => sqlite::to_canonical(col),
    }
}

/// Emit a canonical type as a DDL string for the target dialect.
pub fn from_canonical(ct: &CanonicalType, target: Dialect) -> DdlType {
    match target {
        Dialect::Postgres => pg::from_canonical(ct),
        Dialect::Mysql => mysql::from_canonical(ct),
        Dialect::Mssql => mssql::from_canonical(ct),
        Dialect::Sqlite => sqlite::from_canonical(ct),
    }
}

/// Map a source column type to a target DDL type string.
pub fn map_ddl_type(col: &ColumnInfo, source: Dialect, target: Dialect) -> DdlType {
    let canonical = to_canonical(col, source);
    from_canonical(&canonical, target)
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
