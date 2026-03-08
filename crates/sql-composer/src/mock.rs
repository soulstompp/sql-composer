//! Mock table system for generating test data as `SELECT ... UNION ALL SELECT ...`.
//!
//! When the composer encounters a table name that has a mock registered,
//! it can substitute a generated SELECT statement that produces the mock data.

use std::collections::BTreeMap;

/// A mock table definition with column data for test substitution.
///
/// Mock tables generate SQL of the form:
/// ```sql
/// SELECT 'val1' AS col1, 'val2' AS col2
/// UNION ALL
/// SELECT 'val3', 'val4'
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct MockTable {
    /// The name of the table being mocked.
    pub name: String,
    /// Rows of column name → value mappings.
    pub rows: Vec<BTreeMap<String, String>>,
}

impl MockTable {
    /// Create a new mock table with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            rows: Vec::new(),
        }
    }

    /// Add a row to this mock table.
    pub fn add_row(&mut self, row: BTreeMap<String, String>) {
        self.rows.push(row);
    }

    /// Generate the mock SQL for this table.
    ///
    /// The first row includes `AS column_name` aliases, subsequent rows omit them.
    /// Values are quoted as SQL string literals. Use `NULL` (without quotes) for null.
    pub fn to_sql(&self) -> String {
        if self.rows.is_empty() {
            return format!("SELECT NULL WHERE 1=0 /* empty mock: {} */", self.name);
        }

        // Use the first row's keys to determine column order
        let columns: Vec<&String> = self.rows[0].keys().collect();

        let mut parts = Vec::new();

        for (i, row) in self.rows.iter().enumerate() {
            let values: Vec<String> = columns
                .iter()
                .map(|col| {
                    let val = row.get(*col).map(|s| s.as_str()).unwrap_or("NULL");
                    if val == "NULL" {
                        if i == 0 {
                            format!("NULL AS {col}")
                        } else {
                            "NULL".to_string()
                        }
                    } else if i == 0 {
                        format!("'{}' AS {col}", val.replace('\'', "''"))
                    } else {
                        format!("'{}'", val.replace('\'', "''"))
                    }
                })
                .collect();
            parts.push(format!("SELECT {}", values.join(", ")));
        }

        parts.join("\nUNION ALL\n")
    }
}

/// Convenience macro for building mock table rows.
///
/// # Example
/// ```
/// use sql_composer::mock_rows;
/// let rows = mock_rows![
///     {"id" => "1", "name" => "Alice"},
///     {"id" => "2", "name" => "Bob"},
/// ];
/// ```
#[macro_export]
macro_rules! mock_rows {
    [$(  {$($key:literal => $val:literal),* $(,)?}  ),* $(,)?] => {
        vec![
            $(
                {
                    let mut row = ::std::collections::BTreeMap::new();
                    $( row.insert($key.to_string(), $val.to_string()); )*
                    row
                }
            ),*
        ]
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_table_single_row() {
        let mut mock = MockTable::new("users");
        let mut row = BTreeMap::new();
        row.insert("id".to_string(), "1".to_string());
        row.insert("name".to_string(), "Alice".to_string());
        mock.add_row(row);

        let sql = mock.to_sql();
        assert_eq!(sql, "SELECT '1' AS id, 'Alice' AS name");
    }

    #[test]
    fn test_mock_table_multiple_rows() {
        let mut mock = MockTable::new("users");

        let mut row1 = BTreeMap::new();
        row1.insert("id".to_string(), "1".to_string());
        row1.insert("name".to_string(), "Alice".to_string());
        mock.add_row(row1);

        let mut row2 = BTreeMap::new();
        row2.insert("id".to_string(), "2".to_string());
        row2.insert("name".to_string(), "Bob".to_string());
        mock.add_row(row2);

        let sql = mock.to_sql();
        assert_eq!(
            sql,
            "SELECT '1' AS id, 'Alice' AS name\nUNION ALL\nSELECT '2', 'Bob'"
        );
    }

    #[test]
    fn test_mock_table_with_null() {
        let mut mock = MockTable::new("users");
        let mut row = BTreeMap::new();
        row.insert("id".to_string(), "1".to_string());
        row.insert("email".to_string(), "NULL".to_string());
        mock.add_row(row);

        let sql = mock.to_sql();
        assert!(sql.contains("NULL AS email"));
        assert!(!sql.contains("'NULL'"));
    }

    #[test]
    fn test_mock_table_empty() {
        let mock = MockTable::new("users");
        let sql = mock.to_sql();
        assert!(sql.contains("WHERE 1=0"));
    }

    #[test]
    fn test_mock_table_sql_injection_safe() {
        let mut mock = MockTable::new("users");
        let mut row = BTreeMap::new();
        row.insert("name".to_string(), "O'Brien".to_string());
        mock.add_row(row);

        let sql = mock.to_sql();
        assert!(sql.contains("O''Brien"));
    }

    #[test]
    fn test_mock_rows_macro() {
        let rows = mock_rows![
            {"id" => "1", "name" => "Alice"},
            {"id" => "2", "name" => "Bob"},
        ];
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("id").unwrap(), "1");
        assert_eq!(rows[1].get("name").unwrap(), "Bob");
    }
}
