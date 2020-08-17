error_chain! {
    errors {
        CompositionAliasConflict(t: String) {
            description("a new alias would conflicts with an already defined alias")
                display("alias used more than once: '{}'", t)
        }
        CompositionAliasUnknown(a: String) {
            description("an alias was provided that couldn't be found")
                display("unknown alias: {}", a)
        }
        CompositionCommandArgInvalid(c: String, e: String){
            description("an invalid argument has been provided to a composition command")
                display("invalid arguments for {}: {}", c, e)
        }
        CompositionCommandUnknown(c: String) {
            description("a command that doesn't exist was called as :command() in templated")
                display("Unable to identify command: '{}'", c)
        }
        CompositionBindingValueCount(n: String, e: String) {
            description("an invalid number of values has been provided for a binding")
                display("Binding {} has invalid number of values: {}", n, e)
        }
        CompositionBindingValueInvalid(n: String, e: String) {
            description("an invalid value has been provided for a binding value")
                display("Binding {} has invalid value: {}", n, e)
        }
        CompositionIncomplete(t: String) {
            description("no terminating character found")
                display("expected termination character, none found")
        }
        MockCompositionArgsInvalid(e: String) {
            description("invalid args passed to mock_compose()")
                display("invalid mock_compose args: {}", e)
        }
        MockCompositionBindingValueCount(n: String, e: String) {
            description("an invalid number of values has been provided for a binding")
                display("Binding {} has invalid number of values: {}", n, e)
        }
        MockCompositionBindingValueInvalid(n: String, e: String) {
            description("an invalid value has been provided for a binding mock value")
                display("Binding {} has invalid mock value: {}", n, e)
        }
        MockCompositionColumnCountInvalid(r: u8, c: u8, ec: u8) {
            description("unexpected number of columns for a row of mocked values")
                display("Row {} of provided mock values has {} columns but {} were expected", r, c, ec)
        }
        ParsedSqlStatementIntoParsedSqlCompositionInvalidSqlLength(e: String) {
            description("the length of SqlStatment.sql must be exactly 1")
                display("the length of SqlStatment.sql must be exactly 1")
        }
        ParsedSqlStatementIntoParsedSqlCompositionInvalidVariant(e: String) {
            description("first item of SqlStatment.sql must be a Sql::Composition variant of the Sql type")
                display("first item of SqlStatment.sql must be a Sql::Composition variant of the Sql type")
        }
    }

    foreign_links {
        Utf8(std::str::Utf8Error);
        StringUtf8(std::string::FromUtf8Error);
        Io(std::io::Error);
        Mysql(mysql::error::Error) #[cfg(feature = "mysql")];
        Postgres(postgres::error::Error) #[cfg(feature = "postgres")];
        Sqlite(rusqlite::Error) #[cfg(feature = "rusqlite")];
    }
}
