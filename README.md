# sql-composer

Sql Composer is a SQL pre-processor that helps to manage the intricate relationships between the many queries needed throughout complex data driven applications. 

The compositions sql-composer build allow you to efficiently interact with your database by providing the following advantages :

* Starts with SQL, which is often the point where an engineer has to start from anyway. The most common case is a DBA suggesting a complex generated query be replaced by a hand crafted that is dramatically more performant.
* Prevents a rigid relationship between the database design and the code. As tables change the queries themselves will have to change but will often not require changes to the code itself. 
* Encourages reuse of complete queries, rather than fragments of text/logic. 
* Simplifies management of bind placeholders and bind values when preparing/executing statements. 
* Permits the use of different database drivers even for the same database engine and language.
* Composer queries will produce the same SQL across multiple languages, allowing companies with heterogeneous development environments to easily share crucial business logic in a consistent manner.
* Allow non-engineers to work on improving the SQL and composer macros. If the query writer were to use the CLI, they could easily write and test out new complex compositions without ever writing a line of code.

## Macros

All macros in sql-composer are called using the syntax  . These calls are processed as commands to the `Composer` and effect the resulting SQL passed along to the DB driver and the shape of the bind variables passed along for the driver’s query execution. 

The `Composer` only supports two type of macros, one which produces complete statements and the other which provides query bindings. Outside of these two types of compositions the SQL provided will not be altered by the `Composer`.

### Composition Macros

This type of macro produces a complete statement composition, and may compose together several other statement compositions.

`:command([all] [distinct] [column1, column2… of] [path, path...])` 

The most commonly used command is the `compose` command which reads in another composition and expands it in place. The composer handles these calls in a way that nesting calls to commands several layers deep works without an issue.
 NOTE: recursive calls are not currently caught, so care should be taken until this is cleaned up.

Other commands expand on the concept of calls to `compose` but wrap one or more compositions into a larger summary query. A prime example would be the `union` command, which will compose two compositions between a `UNION` operator. These additional commands are simply helpers to cut down on the number of compositions the query writer must create.

### Binding Macros

Binding macros have a simpler syntax, since this type of macro doesn’t vary much in it’s behavior by design. Gives the query writer strict control of how a user binds values to the template. 

* :bind(name) - bind a single, not providing a value for the `name` param is an error
* :bind_opt(name) - (TODO) bind a single value, none are provided then binds as NULL
* :bind_m - (TODO) bind m (where m >= 1) 
* :bind_opt_m - (TODO) bind m (where m >= 1), if no values are provided to bind then interpreted as NULL
* :bind_m_n - (TODO) bind m to n (where m >= 1 and n > m), bind from m to n values without exploding
 
When the `Composer` encounters a bind macro it will create one or more comma separated driver-specific placeholders or a NULL depending on the rules of the particular bind macro. For each placeholder that is added a corresponding value is added to the bind list. Since this is all managed automatcially calls to `SqlComposition.compose()` only need to provide a Hashmap of named values to bind. 
Project Status

This project is under active development and its API is still likely to change. 
