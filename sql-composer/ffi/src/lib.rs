use sql-composer;



#[no_mangle]
pub extern "C" fn sqllite_composer_initialize() ->  {
    RusqliteComposer::new()
}

// parse_template(...)
// handle = sqllite_composer_initialize(...)
// bind_values(handle, values...)
// bound_sql, bindings) = compose(handle, stmt.item)
