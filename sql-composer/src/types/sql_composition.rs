pub struct SqlComposition<T: Sized> {
    pub sql:    String,
    pub values: Vec<T>,
}

impl<T: Sized> SqlComposition<T> {
    pub fn new() -> Self {
        SqlComposition {
            sql:    String::new(),
            values: vec![],
        }
    }

    pub fn sql(&self) -> String {
        self.sql.to_string()
    }

    pub fn sql_len(&self) -> usize {
        self.sql.len()
    }

    pub fn values(&self) -> &Vec<T> {
        &self.values
    }

    pub fn values_len(&self) -> usize {
        self.values.len()
    }

    pub fn append(&mut self, sc: SqlComposition<T>) {
        self.push(&sc.sql, sc.values);
    }

    pub fn push(&mut self, sql: &str, values: Vec<T>) {
        self.sql.push_str(sql);
        self.values.extend(values);
    }

    pub fn push_sql(&mut self, sql: &str) {
        self.sql.push_str(sql);
    }

    pub fn push_value(&mut self, v: T) {
        self.values.push(v);
    }
}
