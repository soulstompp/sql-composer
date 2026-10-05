//! The composer transforms parsed templates into final SQL with dialect-specific
//! placeholders and resolved compose references.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Component, Path, PathBuf};

use crate::error::{Error, Result};
use crate::mock::MockTable;
use crate::parser;
use crate::types::{
    Command, CommandKind, ComposeRef, ComposeTarget, Dialect, Element, Template, TemplateSource,
};

/// The result of composing a template: final SQL and ordered bind parameter names.
#[derive(Debug, Clone, PartialEq)]
pub struct ComposedSql {
    /// The final SQL string with dialect-specific placeholders.
    pub sql: String,
    /// Ordered list of bind parameter names corresponding to placeholders.
    ///
    /// For numbered dialects (Postgres, SQLite), names are in alphabetical order
    /// with duplicates removed. For positional dialects (MySQL), names are in
    /// document order.
    pub bind_params: Vec<String>,
}

/// Composes parsed templates into final SQL.
///
/// Handles dialect-specific placeholder generation, compose reference resolution,
/// and mock table substitution.
#[derive(Clone)]
pub struct Composer {
    /// The target database dialect for placeholder syntax.
    pub dialect: Dialect,
    /// Directories to search for template files referenced by `:compose()`.
    pub search_paths: Vec<PathBuf>,
    /// Mock tables for test data substitution.
    pub mock_tables: HashMap<String, MockTable>,
    /// Templates registered as views, by canonical path: wherever one is composed, the composed
    /// SQL names the view instead of inlining the template.
    pub views: HashMap<PathBuf, String>,
    /// The registry template that defines the views. Its own composes inline one level, since
    /// they are the views' definitions.
    pub view_registry: Option<PathBuf>,
    /// Write every relation a statement reads once, named, at the head of that statement: one
    /// `WITH`, each relation after the relations it reads, and the statement last. A relation is
    /// named after its file and directories (`layers/figures.sqlc` is `layers_figures`), and every
    /// place that composes it says `SELECT * FROM` that name. A template that binds a parameter,
    /// or is composed with its slots filled, is written where it is composed, as without this.
    pub name_relations: bool,
}

/// What a statement reads, gathered in the order its `WITH` names it.
#[derive(Default)]
struct Gathered {
    /// Each relation by canonical path, with its name, every relation after those it reads.
    order: Vec<(PathBuf, String)>,
    /// The relations already in `order`.
    named: HashSet<PathBuf>,
    /// The relations being gathered now, to catch a template that reads itself.
    open: HashSet<PathBuf>,
}

impl Composer {
    /// Create a new composer with the given dialect.
    pub fn new(dialect: Dialect) -> Self {
        Self {
            dialect,
            search_paths: vec![],
            mock_tables: HashMap::new(),
            views: HashMap::new(),
            view_registry: None,
            name_relations: false,
        }
    }

    /// Register the template at `path` as the view `name`: wherever it is composed, the composed
    /// SQL is `SELECT * FROM name` instead of the template's body.
    pub fn add_view(&mut self, path: &Path, name: impl Into<String>) -> Result<()> {
        let resolved = std::fs::canonicalize(self.find_template(path)?)?;
        self.views.insert(resolved, name.into());
        Ok(())
    }

    /// Register every `CREATE VIEW <name> AS :define(<path>)` in the registry template at
    /// `registry`. `:define` is what puts the body there rather than the name, so the registry
    /// needs no exemption of its own; everywhere else a registered template is referenced by its
    /// view. A registered template that binds a parameter or leaves a slot open is an error: a
    /// view takes no argument.
    pub fn load_view_registry(&mut self, registry: &Path) -> Result<()> {
        let resolved = std::fs::canonicalize(self.find_template(registry)?)?;
        let template = parser::parse_template_file(&resolved)?;
        let mut preceding = String::new();
        for element in &template.elements {
            match element {
                Element::Sql(text) => preceding.push_str(text),
                Element::Compose(compose_ref) => {
                    if let (ComposeTarget::Definition(path), Some(name)) =
                        (&compose_ref.target, view_name_before(&preceding))
                    {
                        self.add_view(path, name)?;
                    }
                    preceding.clear();
                }
                _ => preceding.clear(),
            }
        }
        self.view_registry = Some(resolved);

        let registered: Vec<(PathBuf, String)> = self
            .views
            .iter()
            .map(|(p, n)| (p.clone(), n.clone()))
            .collect();
        for (path, view) in registered {
            let template = parser::parse_template_file(&path)?;
            match self.compose(&template) {
                Ok(composed) if composed.bind_params.is_empty() => {}
                Ok(composed) => {
                    return Err(Error::ViewTakesArguments {
                        path,
                        view,
                        reason: format!("binds {}", composed.bind_params.join(", ")),
                    })
                }
                Err(Error::MissingSlot { name }) => {
                    return Err(Error::ViewTakesArguments {
                        path,
                        view,
                        reason: format!("leaves the slot @{name} open"),
                    })
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// The name a composed template is written as, when it has one.
    fn view_reference(&self, path: &Path) -> Result<Option<String>> {
        if self.views.is_empty() {
            return Ok(None);
        }
        let resolved = std::fs::canonicalize(self.find_template(path)?)?;
        Ok(self.views.get(&resolved).cloned())
    }

    /// The name a compose reference is written as, or `None` where the body is spliced in.
    /// `:define` always splices: it is the definition, and a definition that referred to the
    /// name being defined would define nothing.
    fn reference(&self, compose_ref: &ComposeRef, path: &Path) -> Result<Option<String>> {
        match compose_ref.target {
            ComposeTarget::Definition(_) => Ok(None),
            _ => self.view_reference(path),
        }
    }

    /// Add a search path for resolving compose references.
    pub fn add_search_path(&mut self, path: PathBuf) {
        self.search_paths.push(path);
    }

    /// Register a mock table for test data substitution.
    pub fn add_mock_table(&mut self, mock: MockTable) {
        self.mock_tables.insert(mock.name.clone(), mock);
    }

    /// Compose a template into final SQL with placeholders.
    /// Compose a template. Any name the template `:define`s stands for that relation everywhere
    /// underneath it, so a statement can put its vocabulary in a `WITH` clause once and have
    /// every reference to it below say the name instead of repeating the body.
    pub fn compose(&self, template: &Template) -> Result<ComposedSql> {
        if self.name_relations {
            return self.compose_named(template, |composer, statement| {
                composer.compose_root(statement)
            });
        }
        let declared = self.declared_names(template)?;
        if declared.is_empty() {
            return self.compose_root(template);
        }
        let mut scoped = self.clone();
        scoped.views.extend(declared);
        scoped.compose_root(template)
    }

    /// The names a template declares for itself, by canonical path: one per `:define` that has a
    /// name in front of it. A `:define` with no name still splices its body; it simply names
    /// nothing, which is what the registry's own composition does.
    fn declared_names(&self, template: &Template) -> Result<HashMap<PathBuf, String>> {
        let mut names = HashMap::new();
        let mut preceding = String::new();
        for element in &template.elements {
            match element {
                Element::Sql(text) => preceding.push_str(text),
                Element::Compose(compose_ref) => {
                    if let ComposeTarget::Definition(path) = &compose_ref.target {
                        if let Some(name) =
                            cte_name_before(&preceding).or_else(|| view_name_before(&preceding))
                        {
                            let resolved = std::fs::canonicalize(self.find_template(path)?)?;
                            names.insert(resolved, name);
                        }
                    }
                    preceding.clear();
                }
                _ => preceding.clear(),
            }
        }
        Ok(names)
    }

    fn compose_root(&self, template: &Template) -> Result<ComposedSql> {
        let mut visited = HashSet::new();
        if let TemplateSource::File(ref path) = template.source {
            visited.insert(path.clone());
        }
        let slots = HashMap::new();
        self.compose_inner(template, &slots, &mut visited)
    }

    /// Compose a template with value counts, expanding multi-value bindings
    /// into multiple placeholders.
    ///
    /// When a `:bind(name)` has multiple values in the map, this method emits
    /// one placeholder per value (e.g. `$1, $2, $3` for 3 values), and repeats
    /// the bind name in `bind_params` for each. This enables `IN` clauses:
    ///
    /// ```text
    /// SELECT * FROM users WHERE id IN (:bind(ids))
    /// -- with ids=[10, 20, 30] becomes:
    /// SELECT * FROM users WHERE id IN ($1, $2, $3)
    /// -- bind_params = ["ids", "ids", "ids"]
    /// ```
    ///
    /// For bindings with only one value, behavior is identical to [`Composer::compose()`].
    pub fn compose_with_values<V>(
        &self,
        template: &Template,
        values: &BTreeMap<String, Vec<V>>,
    ) -> Result<ComposedSql> {
        if self.name_relations {
            return self.compose_named(template, |composer, statement| {
                composer.compose_with_values_root(statement, values)
            });
        }
        self.compose_with_values_root(template, values)
    }

    fn compose_with_values_root<V>(
        &self,
        template: &Template,
        values: &BTreeMap<String, Vec<V>>,
    ) -> Result<ComposedSql> {
        let mut visited = HashSet::new();
        if let TemplateSource::File(ref path) = template.source {
            visited.insert(path.clone());
        }
        let slots = HashMap::new();
        self.compose_with_values_inner(template, values, &slots, &mut visited)
    }

    // ── Named relations ───────────────────────────────────────────────

    /// Compose `template` with every relation it reads named once at the head of the statement
    /// that reads it. A template of several statements gets a `WITH` for each, naming what that
    /// statement reads. `emit` composes one statement once the names are in place.
    fn compose_named(
        &self,
        template: &Template,
        emit: impl Fn(&Composer, &Template) -> Result<ComposedSql>,
    ) -> Result<ComposedSql> {
        let mut closed = HashMap::new();
        let mut sql = String::new();
        let mut bind_params = Vec::new();
        for elements in split_statements(&template.elements) {
            let mut gathered = Gathered::default();
            if let TemplateSource::File(ref path) = template.source {
                if let Ok(root) = std::fs::canonicalize(path) {
                    gathered.open.insert(root);
                }
            }
            self.gather(&elements, &HashMap::new(), &mut gathered, &mut closed)?;

            let mut named = self.clone();
            let mut taken: HashMap<&str, &PathBuf> = HashMap::new();
            for (path, name) in &gathered.order {
                if let Some(first) = taken.insert(name, path) {
                    return Err(Error::NameTaken {
                        name: name.clone(),
                        first: first.clone(),
                        second: path.clone(),
                    });
                }
                named.views.insert(path.clone(), name.clone());
            }

            let statement = emit(
                &named,
                &Template {
                    elements,
                    source: template.source.clone(),
                },
            )?;
            bind_params.extend(statement.bind_params);
            let mut relations = Vec::with_capacity(gathered.order.len());
            for (path, name) in &gathered.order {
                let body = named.compose_root(&parser::parse_template_file(path)?)?;
                relations.push(format!("{name} AS (\n{}\n)", body.sql.trim()));
            }
            sql.push_str(&head_with(&statement.sql, &relations));
        }
        Ok(ComposedSql { sql, bind_params })
    }

    /// Walk `elements` as emitting them walks them, and gather every relation they read that can
    /// be named, each after the relations it reads.
    fn gather(
        &self,
        elements: &[Element],
        slots: &HashMap<String, PathBuf>,
        gathered: &mut Gathered,
        closed: &mut HashMap<PathBuf, bool>,
    ) -> Result<()> {
        for element in elements {
            match element {
                Element::Sql(_) | Element::Bind(_) => {}
                Element::Compose(compose_ref) => {
                    if let ComposeTarget::Definition(path) = &compose_ref.target {
                        return Err(Error::DefinitionWhileNaming { path: path.clone() });
                    }
                    let path = Self::resolve_compose_target(compose_ref, slots)?;
                    let child_slots = Self::build_child_slots(compose_ref);
                    self.gather_relation(&path, &child_slots, gathered, closed)?;
                }
                Element::Command(command) => {
                    for source in &command.sources {
                        self.gather_relation(source, &HashMap::new(), gathered, closed)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Gather what the relation at `path` reads, then the relation itself when it can be named:
    /// composed with no slot filled, it binds nothing and leaves nothing open. One that cannot be
    /// named is written in place, so only what it reads is gathered.
    fn gather_relation(
        &self,
        path: &Path,
        slots: &HashMap<String, PathBuf>,
        gathered: &mut Gathered,
        closed: &mut HashMap<PathBuf, bool>,
    ) -> Result<()> {
        if self.view_reference(path)?.is_some() {
            return Ok(());
        }
        let resolved = std::fs::canonicalize(self.find_template(path)?)?;
        if gathered.named.contains(&resolved) {
            return Ok(());
        }
        if !gathered.open.insert(resolved.clone()) {
            return Err(Error::CircularReference {
                path: path.to_path_buf(),
            });
        }
        let template = parser::parse_template_file(&resolved)?;
        self.gather(&template.elements, slots, gathered, closed)?;
        gathered.open.remove(&resolved);
        if slots.is_empty() && self.closed(&resolved, closed)? {
            let name = self.relation_name(&resolved, path)?;
            gathered.named.insert(resolved.clone());
            gathered.order.push((resolved, name));
        }
        Ok(())
    }

    /// Whether the template at `resolved` composes standing alone: nothing it reaches binds a
    /// parameter or leaves a slot open.
    fn closed(&self, resolved: &Path, closed: &mut HashMap<PathBuf, bool>) -> Result<bool> {
        if let Some(&known) = closed.get(resolved) {
            return Ok(known);
        }
        closed.insert(resolved.to_path_buf(), true);
        let template = parser::parse_template_file(resolved)?;
        let answer = self.closed_elements(&template.elements, &HashMap::new(), closed)?;
        closed.insert(resolved.to_path_buf(), answer);
        Ok(answer)
    }

    fn closed_elements(
        &self,
        elements: &[Element],
        slots: &HashMap<String, PathBuf>,
        closed: &mut HashMap<PathBuf, bool>,
    ) -> Result<bool> {
        for element in elements {
            let standing = match element {
                Element::Sql(_) => true,
                Element::Bind(_) => false,
                Element::Compose(compose_ref) => {
                    match Self::resolve_compose_target(compose_ref, slots) {
                        Err(Error::MissingSlot { .. }) => false,
                        Err(e) => return Err(e),
                        Ok(path) if self.view_reference(&path)?.is_some() => true,
                        Ok(path) => {
                            let resolved = std::fs::canonicalize(self.find_template(&path)?)?;
                            let child_slots = Self::build_child_slots(compose_ref);
                            if child_slots.is_empty() {
                                self.closed(&resolved, closed)?
                            } else {
                                let template = parser::parse_template_file(&resolved)?;
                                self.closed_elements(&template.elements, &child_slots, closed)?
                            }
                        }
                    }
                }
                Element::Command(command) => {
                    let mut all = true;
                    for source in &command.sources {
                        if self.view_reference(source)?.is_none() {
                            let resolved = std::fs::canonicalize(self.find_template(source)?)?;
                            all &= self.closed(&resolved, closed)?;
                        }
                    }
                    all
                }
            };
            if !standing {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// The name a relation takes from its file: its path under the search path that holds it,
    /// without the extension, each directory joined to the next by `_`.
    fn relation_name(&self, resolved: &Path, written: &Path) -> Result<String> {
        let relative = self
            .search_paths
            .iter()
            .filter_map(|root| std::fs::canonicalize(root).ok())
            .find_map(|root| resolved.strip_prefix(&root).ok().map(Path::to_path_buf))
            .unwrap_or_else(|| written.to_path_buf());
        let mut name = String::new();
        for part in relative.with_extension("").components() {
            if let Component::Normal(part) = part {
                if !name.is_empty() {
                    name.push('_');
                }
                name.extend(part.to_string_lossy().chars().map(|c| {
                    if c.is_ascii_alphanumeric() {
                        c.to_ascii_lowercase()
                    } else {
                        '_'
                    }
                }));
            }
        }
        if name.starts_with(|c: char| c.is_ascii_digit()) {
            name.insert(0, '_');
        }
        if RESERVED.contains(&name.as_str()) {
            return Err(Error::ReservedName {
                path: resolved.to_path_buf(),
                name,
            });
        }
        Ok(name)
    }

    // ── Slot helpers ─────────────────────────────────────────────────

    /// Resolve a compose target to a concrete file path.
    ///
    /// `ComposeTarget::Path` and `ComposeTarget::Definition` both return the path directly: a
    /// definition resolves to the very template its name stands for, which is what makes a name
    /// and its definition one relation rather than two.
    /// `ComposeTarget::Slot` looks up the slot name in the provided slots map.
    fn resolve_compose_target(
        compose_ref: &ComposeRef,
        slots: &HashMap<String, PathBuf>,
    ) -> Result<PathBuf> {
        match &compose_ref.target {
            ComposeTarget::Path(p) | ComposeTarget::Definition(p) => Ok(p.clone()),
            ComposeTarget::Slot(name) => slots
                .get(name)
                .cloned()
                .ok_or_else(|| Error::MissingSlot { name: name.clone() }),
        }
    }

    /// Build the child slot map from a compose reference's slot assignments.
    ///
    /// These are the ONLY slots the child template sees — parent slots are
    /// NOT inherited.
    fn build_child_slots(compose_ref: &ComposeRef) -> HashMap<String, PathBuf> {
        compose_ref
            .slots
            .iter()
            .map(|s| (s.name.clone(), s.path.clone()))
            .collect()
    }

    // ── Dispatch ──────────────────────────────────────────────────────

    fn compose_inner(
        &self,
        template: &Template,
        slots: &HashMap<String, PathBuf>,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<ComposedSql> {
        if self.dialect.supports_numbered_placeholders() {
            self.compose_inner_numbered(template, slots, visited)
        } else {
            self.compose_inner_positional(template, slots, visited)
        }
    }

    fn compose_with_values_inner<V>(
        &self,
        template: &Template,
        values: &BTreeMap<String, Vec<V>>,
        slots: &HashMap<String, PathBuf>,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<ComposedSql> {
        if self.dialect.supports_numbered_placeholders() {
            self.compose_with_values_numbered(template, values, slots, visited)
        } else {
            self.compose_with_values_positional(template, values, slots, visited)
        }
    }

    // ── Numbered path (Postgres, SQLite) ──────────────────────────────
    //
    // Two-pass approach:
    //   Pass 1 — collect all unique bind names (BTreeSet gives alphabetical order)
    //   Allocate — assign 1-based indices from the sorted names
    //   Pass 2 — emit SQL using the global index map (same name → same $N)

    /// Pass 1: Recursively collect unique bind names from a template tree.
    fn collect_bind_names(
        &self,
        template: &Template,
        slots: &HashMap<String, PathBuf>,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<BTreeSet<String>> {
        let mut names = BTreeSet::new();

        for element in &template.elements {
            match element {
                Element::Sql(_) => {}
                Element::Bind(binding) => {
                    names.insert(binding.name.clone());
                }
                Element::Compose(compose_ref) => {
                    let path = Self::resolve_compose_target(compose_ref, slots)?;
                    if self.reference(compose_ref, &path)?.is_some() {
                        continue;
                    }
                    let child_slots = Self::build_child_slots(compose_ref);
                    let sub = self.collect_compose_bind_names(&path, &child_slots, visited)?;
                    names.extend(sub);
                }
                Element::Command(command) => {
                    let sub = self.collect_command_bind_names(command, visited)?;
                    names.extend(sub);
                }
            }
        }

        Ok(names)
    }

    /// Collect bind names from a compose reference's resolved template.
    fn collect_compose_bind_names(
        &self,
        path: &Path,
        child_slots: &HashMap<String, PathBuf>,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<BTreeSet<String>> {
        let resolved = self.find_template(path)?;

        if !visited.insert(resolved.clone()) {
            return Err(Error::CircularReference {
                path: path.to_path_buf(),
            });
        }

        let template = parser::parse_template_file(&resolved)?;
        let names = self.collect_bind_names(&template, child_slots, visited)?;

        visited.remove(&resolved);
        Ok(names)
    }

    /// Collect bind names from all sources in a command.
    ///
    /// Command sources are standalone templates — they get empty slots.
    fn collect_command_bind_names(
        &self,
        command: &Command,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<BTreeSet<String>> {
        let mut names = BTreeSet::new();
        let empty_slots = HashMap::new();
        for source in &command.sources {
            if self.view_reference(source)?.is_some() {
                continue;
            }
            let resolved = self.find_template(source)?;
            let template = parser::parse_template_file(&resolved)?;
            let sub = self.collect_bind_names(&template, &empty_slots, visited)?;
            names.extend(sub);
        }
        Ok(names)
    }

    /// Build an index map for `compose` (single-value bindings).
    /// Each name maps to `(1-based-index, 1)`.
    fn build_index_map(names: &BTreeSet<String>) -> BTreeMap<String, (usize, usize)> {
        names
            .iter()
            .enumerate()
            .map(|(i, name)| (name.clone(), (i + 1, 1)))
            .collect()
    }

    /// Build an index map for `compose_with_values` (multi-value bindings).
    /// Each name maps to `(start_index, count)` where count comes from the
    /// values map (defaults to 1 if absent).
    fn build_index_map_with_values<V>(
        names: &BTreeSet<String>,
        values: &BTreeMap<String, Vec<V>>,
    ) -> BTreeMap<String, (usize, usize)> {
        let mut map = BTreeMap::new();
        let mut index = 1;
        for name in names {
            let count = values.get(name).map(|vs| vs.len()).unwrap_or(1).max(1);
            map.insert(name.clone(), (index, count));
            index += count;
        }
        map
    }

    /// Two-pass compose for numbered dialects (single-value).
    fn compose_inner_numbered(
        &self,
        template: &Template,
        slots: &HashMap<String, PathBuf>,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<ComposedSql> {
        // Pass 1: collect
        let mut collect_visited = visited.clone();
        let names = self.collect_bind_names(template, slots, &mut collect_visited)?;

        // Allocate
        let index_map = Self::build_index_map(&names);
        let bind_params: Vec<String> = names.into_iter().collect();

        // Pass 2: emit
        let mut sql = String::new();
        self.emit_sql_numbered(template, &index_map, &mut sql, slots, visited)?;

        Ok(ComposedSql { sql, bind_params })
    }

    /// Two-pass compose for numbered dialects (multi-value).
    fn compose_with_values_numbered<V>(
        &self,
        template: &Template,
        values: &BTreeMap<String, Vec<V>>,
        slots: &HashMap<String, PathBuf>,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<ComposedSql> {
        // Pass 1: collect
        let mut collect_visited = visited.clone();
        let names = self.collect_bind_names(template, slots, &mut collect_visited)?;

        // Allocate with value counts
        let index_map = Self::build_index_map_with_values(&names, values);

        // Build bind_params: each name repeated by its value count, alphabetical
        let mut bind_params = Vec::new();
        for name in &names {
            let count = values
                .get(name.as_str())
                .map(|vs| vs.len())
                .unwrap_or(1)
                .max(1);
            for _ in 0..count {
                bind_params.push(name.clone());
            }
        }

        // Pass 2: emit
        let mut sql = String::new();
        self.emit_sql_numbered(template, &index_map, &mut sql, slots, visited)?;

        Ok(ComposedSql { sql, bind_params })
    }

    /// Pass 2: Emit SQL for a template using the global index map.
    fn emit_sql_numbered(
        &self,
        template: &Template,
        index_map: &BTreeMap<String, (usize, usize)>,
        sql: &mut String,
        slots: &HashMap<String, PathBuf>,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<()> {
        for element in &template.elements {
            match element {
                Element::Sql(text) => sql.push_str(text),
                Element::Bind(binding) => {
                    let &(start, count) = &index_map[&binding.name];
                    for i in 0..count {
                        if i > 0 {
                            sql.push_str(", ");
                        }
                        sql.push_str(&self.dialect.placeholder(start + i));
                    }
                }
                Element::Compose(compose_ref) => {
                    let path = Self::resolve_compose_target(compose_ref, slots)?;
                    if let Some(view) = self.reference(compose_ref, &path)? {
                        sql.push_str(&format!("SELECT * FROM {view}"));
                        continue;
                    }
                    let child_slots = Self::build_child_slots(compose_ref);
                    self.emit_compose_numbered(&path, &child_slots, index_map, sql, visited)?;
                }
                Element::Command(command) => {
                    self.emit_command_numbered(command, index_map, sql, visited)?;
                }
            }
        }
        Ok(())
    }

    /// Emit SQL for a compose reference using the global index map.
    fn emit_compose_numbered(
        &self,
        path: &Path,
        child_slots: &HashMap<String, PathBuf>,
        index_map: &BTreeMap<String, (usize, usize)>,
        sql: &mut String,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<()> {
        let resolved = self.find_template(path)?;

        if !visited.insert(resolved.clone()) {
            return Err(Error::CircularReference {
                path: path.to_path_buf(),
            });
        }

        let template = parser::parse_template_file(&resolved)?;
        self.emit_sql_numbered(&template, index_map, sql, child_slots, visited)?;

        visited.remove(&resolved);
        Ok(())
    }

    /// Emit SQL for a command (union/count) using the global index map.
    ///
    /// Command sources are standalone templates — they get empty slots.
    fn emit_command_numbered(
        &self,
        command: &Command,
        index_map: &BTreeMap<String, (usize, usize)>,
        sql: &mut String,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<()> {
        match command.kind {
            CommandKind::Union => self.emit_union_numbered(command, index_map, sql, visited),
            CommandKind::Count => self.emit_count_numbered(command, index_map, sql, visited),
            CommandKind::Intersect => self.emit_set_operation_numbered(
                command,
                "INTERSECT",
                "_intersect",
                index_map,
                sql,
                visited,
            ),
            CommandKind::Except => self
                .emit_set_operation_numbered(command, "EXCEPT", "_except", index_map, sql, visited),
        }
    }

    /// Emit SQL for a UNION command using the global index map.
    fn emit_union_numbered(
        &self,
        command: &Command,
        index_map: &BTreeMap<String, (usize, usize)>,
        sql: &mut String,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<()> {
        let union_kw = if command.all {
            "UNION ALL"
        } else if command.distinct {
            "UNION DISTINCT"
        } else {
            "UNION"
        };

        let empty_slots = HashMap::new();
        for (i, source) in command.sources.iter().enumerate() {
            if i > 0 {
                let trimmed = sql.trim_end().len();
                sql.truncate(trimmed);
                sql.push_str(&format!("\n{union_kw}\n"));
            }
            if let Some(view) = self.view_reference(source)? {
                sql.push_str(&format!("SELECT * FROM {view}"));
                continue;
            }
            let resolved = self.find_template(source)?;
            let template = parser::parse_template_file(&resolved)?;
            self.emit_sql_numbered(&template, index_map, sql, &empty_slots, visited)?;
        }

        Ok(())
    }

    /// Emit SQL for a COUNT command using the global index map.
    fn emit_count_numbered(
        &self,
        command: &Command,
        index_map: &BTreeMap<String, (usize, usize)>,
        sql: &mut String,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<()> {
        let columns = match &command.columns {
            Some(cols) => cols.join(", "),
            None => "*".to_string(),
        };

        let count_expr = if command.distinct {
            format!("COUNT(DISTINCT {columns})")
        } else {
            format!("COUNT({columns})")
        };

        sql.push_str(&format!("SELECT {count_expr} FROM (\n"));

        let empty_slots = HashMap::new();
        if command.sources.len() > 1 {
            let union_cmd = Command {
                kind: CommandKind::Union,
                distinct: command.distinct,
                all: command.all,
                columns: None,
                sources: command.sources.clone(),
            };
            self.emit_union_numbered(&union_cmd, index_map, sql, visited)?;
        } else {
            let source = &command.sources[0];
            if let Some(view) = self.view_reference(source)? {
                sql.push_str(&format!("SELECT * FROM {view}"));
            } else {
                let resolved = self.find_template(source)?;
                let template = parser::parse_template_file(&resolved)?;
                self.emit_sql_numbered(&template, index_map, sql, &empty_slots, visited)?;
            }
        }

        sql.push_str("\n) AS _count_sub");
        Ok(())
    }

    /// Emit SQL for an INTERSECT or EXCEPT command using the global index map.
    ///
    /// Every source is its own derived table, and so is the whole. A source is text this
    /// composer does not parse, so a source that is itself a union (or ends in an `ORDER BY`)
    /// would otherwise regroup with this operator; and the whole is closed so that it composes
    /// wherever a single query does, a `:union` member included.
    fn emit_set_operation_numbered(
        &self,
        command: &Command,
        keyword: &str,
        alias: &str,
        index_map: &BTreeMap<String, (usize, usize)>,
        sql: &mut String,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<()> {
        let operator = Self::set_operator(command, keyword)?;
        let empty_slots = HashMap::new();

        sql.push_str("SELECT * FROM (\n");
        for (i, source) in command.sources.iter().enumerate() {
            if i > 0 {
                sql.push_str(&format!("\n{operator}\n"));
            }
            sql.push_str("SELECT * FROM (\n");
            if let Some(view) = self.view_reference(source)? {
                sql.push_str(&format!("SELECT * FROM {view}"));
            } else {
                let resolved = self.find_template(source)?;
                let template = parser::parse_template_file(&resolved)?;
                self.emit_sql_numbered(&template, index_map, sql, &empty_slots, visited)?;
            }
            let trimmed = sql.trim_end().len();
            sql.truncate(trimmed);
            sql.push_str(&format!("\n) AS {alias}_{}", i + 1));
        }
        sql.push_str(&format!("\n) AS {alias}"));

        Ok(())
    }

    // ── Positional path (MySQL) ───────────────────────────────────────
    //
    // Document-order placeholders with bare `?`. No reindexing needed
    // since MySQL placeholders carry no index number.

    fn compose_inner_positional(
        &self,
        template: &Template,
        slots: &HashMap<String, PathBuf>,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<ComposedSql> {
        let mut sql = String::new();
        let mut bind_params = Vec::new();

        for element in &template.elements {
            match element {
                Element::Sql(text) => {
                    sql.push_str(text);
                }
                Element::Bind(binding) => {
                    let index = bind_params.len() + 1;
                    sql.push_str(&self.dialect.placeholder(index));
                    bind_params.push(binding.name.clone());
                }
                Element::Compose(compose_ref) => {
                    let path = Self::resolve_compose_target(compose_ref, slots)?;
                    if let Some(view) = self.reference(compose_ref, &path)? {
                        sql.push_str(&format!("SELECT * FROM {view}"));
                        continue;
                    }
                    let child_slots = Self::build_child_slots(compose_ref);
                    let composed = self.resolve_compose_positional(&path, &child_slots, visited)?;
                    sql.push_str(&composed.sql);
                    bind_params.extend(composed.bind_params);
                }
                Element::Command(command) => {
                    let composed = self.compose_command(command, visited)?;
                    sql.push_str(&composed.sql);
                    bind_params.extend(composed.bind_params);
                }
            }
        }

        Ok(ComposedSql { sql, bind_params })
    }

    fn compose_with_values_positional<V>(
        &self,
        template: &Template,
        values: &BTreeMap<String, Vec<V>>,
        slots: &HashMap<String, PathBuf>,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<ComposedSql> {
        let mut sql = String::new();
        let mut bind_params = Vec::new();

        for element in &template.elements {
            match element {
                Element::Sql(text) => {
                    sql.push_str(text);
                }
                Element::Bind(binding) => {
                    let count = values
                        .get(&binding.name)
                        .map(|vs| vs.len())
                        .unwrap_or(1)
                        .max(1);

                    for i in 0..count {
                        if i > 0 {
                            sql.push_str(", ");
                        }
                        let index = bind_params.len() + 1;
                        sql.push_str(&self.dialect.placeholder(index));
                        bind_params.push(binding.name.clone());
                    }
                }
                Element::Compose(compose_ref) => {
                    let path = Self::resolve_compose_target(compose_ref, slots)?;
                    if let Some(view) = self.reference(compose_ref, &path)? {
                        sql.push_str(&format!("SELECT * FROM {view}"));
                        continue;
                    }
                    let child_slots = Self::build_child_slots(compose_ref);
                    let composed = self.resolve_compose_with_values_positional(
                        &path,
                        &child_slots,
                        values,
                        visited,
                    )?;
                    sql.push_str(&composed.sql);
                    bind_params.extend(composed.bind_params);
                }
                Element::Command(command) => {
                    let composed = self.compose_command(command, visited)?;
                    sql.push_str(&composed.sql);
                    bind_params.extend(composed.bind_params);
                }
            }
        }

        Ok(ComposedSql { sql, bind_params })
    }

    /// Resolve a compose reference by finding and parsing the template file (positional).
    fn resolve_compose_positional(
        &self,
        path: &Path,
        child_slots: &HashMap<String, PathBuf>,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<ComposedSql> {
        let resolved = self.find_template(path)?;

        if !visited.insert(resolved.clone()) {
            return Err(Error::CircularReference {
                path: path.to_path_buf(),
            });
        }

        let template = parser::parse_template_file(&resolved)?;
        let result = self.compose_inner_positional(&template, child_slots, visited)?;

        visited.remove(&resolved);

        Ok(result)
    }

    /// Resolve a compose reference with value-aware expansion (positional).
    fn resolve_compose_with_values_positional<V>(
        &self,
        path: &Path,
        child_slots: &HashMap<String, PathBuf>,
        values: &BTreeMap<String, Vec<V>>,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<ComposedSql> {
        let resolved = self.find_template(path)?;

        if !visited.insert(resolved.clone()) {
            return Err(Error::CircularReference {
                path: path.to_path_buf(),
            });
        }

        let template = parser::parse_template_file(&resolved)?;
        let result =
            self.compose_with_values_positional(&template, values, child_slots, visited)?;

        visited.remove(&resolved);
        Ok(result)
    }

    /// Compose a command (count/union) into SQL (positional path).
    ///
    /// Command sources are standalone templates — they get empty slots.
    fn compose_command(
        &self,
        command: &Command,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<ComposedSql> {
        match command.kind {
            CommandKind::Union => self.compose_union(command, visited),
            CommandKind::Count => self.compose_count(command, visited),
            CommandKind::Intersect => {
                self.compose_set_operation(command, "INTERSECT", "_intersect", visited)
            }
            CommandKind::Except => {
                self.compose_set_operation(command, "EXCEPT", "_except", visited)
            }
        }
    }

    /// Compose a UNION command (positional path).
    fn compose_union(
        &self,
        command: &Command,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<ComposedSql> {
        let mut parts = Vec::new();
        let mut all_params = Vec::new();
        let empty_slots = HashMap::new();

        for source in &command.sources {
            if let Some(view) = self.view_reference(source)? {
                parts.push(format!("SELECT * FROM {view}"));
                continue;
            }
            let resolved = self.find_template(source)?;
            let template = parser::parse_template_file(&resolved)?;
            let composed = self.compose_inner(&template, &empty_slots, visited)?;

            parts.push(composed.sql.trim_end().to_string());
            all_params.extend(composed.bind_params);
        }

        let union_kw = if command.all {
            "UNION ALL"
        } else if command.distinct {
            "UNION DISTINCT"
        } else {
            "UNION"
        };

        let sql = parts.join(&format!("\n{union_kw}\n"));

        Ok(ComposedSql {
            sql,
            bind_params: all_params,
        })
    }

    /// Compose a COUNT command (positional path).
    fn compose_count(
        &self,
        command: &Command,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<ComposedSql> {
        let columns = match &command.columns {
            Some(cols) => cols.join(", "),
            None => "*".to_string(),
        };

        let empty_slots = HashMap::new();

        // If multiple sources, wrap a union first
        let inner = if command.sources.len() > 1 {
            let union_cmd = Command {
                kind: CommandKind::Union,
                distinct: command.distinct,
                all: command.all,
                columns: None,
                sources: command.sources.clone(),
            };
            self.compose_union(&union_cmd, visited)?
        } else {
            let source = &command.sources[0];
            match self.view_reference(source)? {
                Some(view) => ComposedSql {
                    sql: format!("SELECT * FROM {view}"),
                    bind_params: vec![],
                },
                None => {
                    let resolved = self.find_template(source)?;
                    let template = parser::parse_template_file(&resolved)?;
                    self.compose_inner(&template, &empty_slots, visited)?
                }
            }
        };

        let count_expr = if command.distinct {
            format!("COUNT(DISTINCT {columns})")
        } else {
            format!("COUNT({columns})")
        };

        let sql = format!("SELECT {count_expr} FROM (\n{}\n) AS _count_sub", inner.sql);

        Ok(ComposedSql {
            sql,
            bind_params: inner.bind_params,
        })
    }

    /// Compose an INTERSECT or EXCEPT command (positional path), wrapped as
    /// [`Self::emit_set_operation_numbered`] wraps it and for the same reason.
    fn compose_set_operation(
        &self,
        command: &Command,
        keyword: &str,
        alias: &str,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<ComposedSql> {
        let operator = Self::set_operator(command, keyword)?;
        let empty_slots = HashMap::new();
        let mut parts = Vec::new();
        let mut all_params = Vec::new();

        for (i, source) in command.sources.iter().enumerate() {
            let body = match self.view_reference(source)? {
                Some(view) => format!("SELECT * FROM {view}"),
                None => {
                    let resolved = self.find_template(source)?;
                    let template = parser::parse_template_file(&resolved)?;
                    let composed = self.compose_inner(&template, &empty_slots, visited)?;
                    all_params.extend(composed.bind_params);
                    composed.sql
                }
            };
            parts.push(format!(
                "SELECT * FROM (\n{}\n) AS {alias}_{}",
                body.trim_end(),
                i + 1
            ));
        }

        let sql = format!(
            "SELECT * FROM (\n{}\n) AS {alias}",
            parts.join(&format!("\n{operator}\n"))
        );

        Ok(ComposedSql {
            sql,
            bind_params: all_params,
        })
    }

    /// The operator an `:intersect` or `:except` command writes, with its modifier.
    ///
    /// A column list is refused rather than ignored: it names what `:count` counts, and a set
    /// operation compares whole rows, so a list here could only be a caller expecting a
    /// projection that would not happen.
    fn set_operator(command: &Command, keyword: &str) -> Result<String> {
        if command.columns.is_some() {
            return Err(Error::Parse {
                location: format!(":{}(...)", keyword.to_ascii_lowercase()),
                message: format!("{keyword} compares whole rows and takes no `columns OF` list"),
            });
        }
        Ok(if command.all {
            format!("{keyword} ALL")
        } else if command.distinct {
            format!("{keyword} DISTINCT")
        } else {
            keyword.to_string()
        })
    }

    // ── Shared helpers ────────────────────────────────────────────────

    /// Find a template file on the search paths.
    fn find_template(&self, path: &Path) -> Result<PathBuf> {
        // Try the path directly first
        if path.exists() {
            return Ok(path.to_path_buf());
        }

        // Search on each search path
        for search_path in &self.search_paths {
            let candidate = search_path.join(path);
            if candidate.exists() {
                return Ok(candidate);
            }
        }

        Err(Error::TemplateNotFound {
            path: path.to_path_buf(),
        })
    }
}

/// The name a `WITH` clause gives a definition: the `<name>` of `<name> AS [NOT] MATERIALIZED (`
/// when that is how the text before the definition ends. The materialisation is the statement's
/// business and not this function's; all it reads is the name.
fn cte_name_before(sql: &str) -> Option<String> {
    let upper = sql.to_ascii_uppercase();
    let mut head = upper.trim_end().strip_suffix('(')?.trim_end();
    for word in ["MATERIALIZED", "NOT"] {
        if let Some(rest) = head.strip_suffix(word) {
            head = rest.trim_end();
        }
    }
    let head = head.strip_suffix("AS")?;
    if !head.ends_with(char::is_whitespace) {
        return None;
    }
    let head = head.trim_end();
    let start = head
        .rfind(|c: char| c.is_whitespace() || c == ',' || c == '(')
        .map_or(0, |i| i + 1);
    let name = sql.get(start..head.len())?.trim();
    (!name.is_empty()).then(|| name.to_string())
}

/// The view name in the text before a registry's definition: the `<name>` of the last
/// `CREATE [OR REPLACE] VIEW <name> AS`, when `AS` is the last word before the compose.
fn view_name_before(sql: &str) -> Option<String> {
    let upper = sql.to_ascii_uppercase();
    let at = upper
        .rfind(" VIEW ")
        .or_else(|| upper.strip_prefix("VIEW ").map(|_| 0))?;
    let head = upper[..at].trim_end();
    if !(head.ends_with("CREATE") || head.ends_with("CREATE OR REPLACE")) {
        return None;
    }
    let rest = sql[at..].trim_start().get(4..)?.trim();
    let (name, tail) = rest.split_once(char::is_whitespace)?;
    tail.trim()
        .eq_ignore_ascii_case("AS")
        .then(|| name.to_string())
}

/// The words PostgreSQL reserves, which no relation may be named.
const RESERVED: &[&str] = &[
    "all",
    "analyse",
    "analyze",
    "and",
    "any",
    "array",
    "as",
    "asc",
    "asymmetric",
    "both",
    "case",
    "cast",
    "check",
    "collate",
    "column",
    "constraint",
    "create",
    "current_catalog",
    "current_date",
    "current_role",
    "current_time",
    "current_timestamp",
    "current_user",
    "default",
    "deferrable",
    "desc",
    "distinct",
    "do",
    "else",
    "end",
    "except",
    "false",
    "fetch",
    "for",
    "foreign",
    "from",
    "grant",
    "group",
    "having",
    "in",
    "initially",
    "intersect",
    "into",
    "lateral",
    "leading",
    "limit",
    "localtime",
    "localtimestamp",
    "not",
    "null",
    "offset",
    "on",
    "only",
    "or",
    "order",
    "placing",
    "primary",
    "references",
    "returning",
    "select",
    "session_user",
    "some",
    "symmetric",
    "system_user",
    "table",
    "then",
    "to",
    "trailing",
    "true",
    "union",
    "unique",
    "user",
    "using",
    "variadic",
    "when",
    "where",
    "window",
    "with",
];

/// The statements in a template's elements, split after each `;` that ends one: outside
/// quotes, comments, dollar quoting and psql meta-commands. A template of one statement is one.
fn split_statements(elements: &[Element]) -> Vec<Vec<Element>> {
    let mut statements = vec![Vec::new()];
    let mut scanner = Scanner::default();
    for element in elements {
        match element {
            Element::Sql(text) => {
                let mut start = 0;
                for end in scanner.ends(text) {
                    if let Some(last) = statements.last_mut() {
                        last.push(Element::Sql(text[start..end].to_string()));
                    }
                    statements.push(Vec::new());
                    start = end;
                }
                if start < text.len() {
                    if let Some(last) = statements.last_mut() {
                        last.push(Element::Sql(text[start..].to_string()));
                    }
                }
            }
            other => {
                if let Some(last) = statements.last_mut() {
                    last.push(other.clone());
                }
            }
        }
    }
    statements.retain(|statement| !statement.is_empty());
    statements
}

/// Where SQL text stands, carried from one piece of a template to the next.
#[derive(Default)]
enum Scan {
    #[default]
    Code,
    Quoted,
    Identifier,
    LineComment,
    BlockComment(u32),
    Dollar(Vec<u8>),
    MetaCommand,
}

#[derive(Default)]
struct Scanner {
    scan: Scan,
}

impl Scanner {
    /// The offsets just past each `;` in `text` that ends a statement.
    fn ends(&mut self, text: &str) -> Vec<usize> {
        let bytes = text.as_bytes();
        let mut ends = Vec::new();
        let mut scan = std::mem::take(&mut self.scan);
        let mut i = 0;
        while i < bytes.len() {
            let next = bytes.get(i + 1).copied();
            scan = match (scan, bytes[i]) {
                (Scan::Code, b'\'') => Scan::Quoted,
                (Scan::Code, b'"') => Scan::Identifier,
                (Scan::Code, b'-') if next == Some(b'-') => {
                    i += 1;
                    Scan::LineComment
                }
                (Scan::Code, b'/') if next == Some(b'*') => {
                    i += 1;
                    Scan::BlockComment(1)
                }
                (Scan::Code, b'\\') => Scan::MetaCommand,
                (Scan::Code, b'$') => match dollar_quote(&bytes[i..]) {
                    Some(tag) => {
                        i += tag.len() - 1;
                        Scan::Dollar(tag.to_vec())
                    }
                    None => Scan::Code,
                },
                (Scan::Code, b';') => {
                    ends.push(i + 1);
                    Scan::Code
                }
                (Scan::Quoted, b'\'') | (Scan::Identifier, b'"') => Scan::Code,
                (Scan::LineComment | Scan::MetaCommand, b'\n') => Scan::Code,
                (Scan::BlockComment(depth), b'*') if next == Some(b'/') => {
                    i += 1;
                    if depth == 1 {
                        Scan::Code
                    } else {
                        Scan::BlockComment(depth - 1)
                    }
                }
                (Scan::BlockComment(depth), b'/') if next == Some(b'*') => {
                    i += 1;
                    Scan::BlockComment(depth + 1)
                }
                (Scan::Dollar(tag), _) if bytes[i..].starts_with(&tag) => {
                    i += tag.len() - 1;
                    Scan::Code
                }
                (scan, _) => scan,
            };
            i += 1;
        }
        self.scan = scan;
        ends
    }
}

/// The dollar quote that opens at the start of `bytes` (`$$` or `$tag$`), if one does.
fn dollar_quote(bytes: &[u8]) -> Option<&[u8]> {
    let tag = bytes[1..]
        .iter()
        .take_while(|b| b.is_ascii_alphanumeric() || **b == b'_')
        .count();
    let opens = bytes.get(tag + 1) == Some(&b'$') && !bytes.get(1).is_some_and(u8::is_ascii_digit);
    opens.then(|| &bytes[..tag + 2])
}

/// `statement` with `relations` named at the head of its query: after its leading comments and
/// psql meta-commands, and after the `AS` of a `CREATE … AS`; as the first entries of the
/// query's own `WITH` when it opens with one, or else in a `WITH` of their own.
fn head_with(statement: &str, relations: &[String]) -> String {
    if relations.is_empty() {
        return statement.to_string();
    }
    let head = statement_head(statement);
    let head = head + created_from(&statement[head..]).unwrap_or(0);
    let (lead, rest) = statement.split_at(head);
    let named = relations.join(",\n");
    match with_keyword(rest) {
        Some(end) => format!(
            "{lead}{}\n{named},\n{}",
            &rest[..end],
            rest[end..].trim_start()
        ),
        None => format!("{lead}WITH {named}\n{rest}"),
    }
}

/// Where a statement's own text starts: past blank lines, `--` comment lines and psql
/// meta-commands, which stand in front of a `WITH` rather than inside it.
fn statement_head(statement: &str) -> usize {
    let mut at = 0;
    for line in statement.split_inclusive('\n') {
        let text = line.trim_start();
        if text.is_empty() || text.starts_with("--") || text.starts_with('\\') {
            at += line.len();
        } else {
            return at + (line.len() - text.len());
        }
    }
    at
}

/// Where the query of a `CREATE … AS <query>` starts, when `text` is one: past the first `AS`
/// outside parentheses and quotes, and the space after it.
fn created_from(text: &str) -> Option<usize> {
    let opens = text.get(..6)?.eq_ignore_ascii_case("CREATE");
    if !opens || !text[6..].starts_with(char::is_whitespace) {
        return None;
    }
    let bytes = text.as_bytes();
    let (mut depth, mut quote) = (0u32, None);
    for i in 0..bytes.len() {
        match (quote, bytes[i]) {
            (Some(q), b) if b == q => quote = None,
            (Some(_), _) => {}
            (None, b @ (b'\'' | b'"')) => quote = Some(b),
            (None, b'(') => depth += 1,
            (None, b')') => depth = depth.saturating_sub(1),
            (None, b) if depth == 0 && b.is_ascii_whitespace() => {
                let after = &text[i + 1..];
                let is_as = after.get(..2).is_some_and(|w| w.eq_ignore_ascii_case("AS"))
                    && after[2..].starts_with(char::is_whitespace);
                if is_as {
                    let query = &after[2..];
                    return Some(text.len() - query.trim_start().len());
                }
            }
            _ => {}
        }
    }
    None
}

/// The length of the `WITH` or `WITH RECURSIVE` that `text` opens with, if it opens with one.
fn with_keyword(text: &str) -> Option<usize> {
    let word = |text: &str, word: &str| {
        let head = text.get(..word.len())?;
        let follows = text[word.len()..].starts_with(char::is_whitespace);
        (head.eq_ignore_ascii_case(word) && follows).then_some(word.len())
    };
    let with = word(text, "WITH")?;
    let after = &text[with..];
    let gap = after.len() - after.trim_start().len();
    Some(word(after.trim_start(), "RECURSIVE").map_or(with, |recursive| with + gap + recursive))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Binding, ComposeTarget, Element, SlotAssignment, TemplateSource};
    use std::io::Write;
    use tempfile::TempDir;

    #[test]
    fn test_compose_plain_sql() {
        let composer = Composer::new(Dialect::Postgres);
        let template = Template {
            elements: vec![Element::Sql("SELECT 1".into())],
            source: TemplateSource::Literal("test".into()),
        };
        let result = composer.compose(&template).unwrap();
        assert_eq!(result.sql, "SELECT 1");
        assert!(result.bind_params.is_empty());
    }

    #[test]
    fn test_compose_with_bindings_postgres() {
        let composer = Composer::new(Dialect::Postgres);
        let template = Template {
            elements: vec![
                Element::Sql("SELECT * FROM users WHERE id = ".into()),
                Element::Bind(Binding {
                    name: "user_id".into(),
                    min_values: None,
                    max_values: None,
                    nullable: false,
                }),
                Element::Sql(" AND active = ".into()),
                Element::Bind(Binding {
                    name: "active".into(),
                    min_values: None,
                    max_values: None,
                    nullable: false,
                }),
            ],
            source: TemplateSource::Literal("test".into()),
        };
        let result = composer.compose(&template).unwrap();
        // Alphabetical: active=$1, user_id=$2
        assert_eq!(
            result.sql,
            "SELECT * FROM users WHERE id = $2 AND active = $1"
        );
        assert_eq!(result.bind_params, vec!["active", "user_id"]);
    }

    #[test]
    fn test_compose_with_bindings_mysql() {
        let composer = Composer::new(Dialect::Mysql);
        let template = Template {
            elements: vec![
                Element::Sql("SELECT * FROM users WHERE id = ".into()),
                Element::Bind(Binding {
                    name: "user_id".into(),
                    min_values: None,
                    max_values: None,
                    nullable: false,
                }),
                Element::Sql(" AND active = ".into()),
                Element::Bind(Binding {
                    name: "active".into(),
                    min_values: None,
                    max_values: None,
                    nullable: false,
                }),
            ],
            source: TemplateSource::Literal("test".into()),
        };
        let result = composer.compose(&template).unwrap();
        // MySQL: document order, bare ?
        assert_eq!(
            result.sql,
            "SELECT * FROM users WHERE id = ? AND active = ?"
        );
        assert_eq!(result.bind_params, vec!["user_id", "active"]);
    }

    #[test]
    fn test_compose_with_bindings_sqlite() {
        let composer = Composer::new(Dialect::Sqlite);
        let template = Template {
            elements: vec![
                Element::Sql("SELECT * FROM users WHERE id = ".into()),
                Element::Bind(Binding {
                    name: "user_id".into(),
                    min_values: None,
                    max_values: None,
                    nullable: false,
                }),
                Element::Sql(" AND active = ".into()),
                Element::Bind(Binding {
                    name: "active".into(),
                    min_values: None,
                    max_values: None,
                    nullable: false,
                }),
            ],
            source: TemplateSource::Literal("test".into()),
        };
        let result = composer.compose(&template).unwrap();
        // Alphabetical: active=?1, user_id=?2
        assert_eq!(
            result.sql,
            "SELECT * FROM users WHERE id = ?2 AND active = ?1"
        );
        assert_eq!(result.bind_params, vec!["active", "user_id"]);
    }

    #[test]
    fn test_dialect_placeholder() {
        assert_eq!(Dialect::Postgres.placeholder(1), "$1");
        assert_eq!(Dialect::Postgres.placeholder(10), "$10");
        assert_eq!(Dialect::Mysql.placeholder(1), "?");
        assert_eq!(Dialect::Mysql.placeholder(10), "?");
        assert_eq!(Dialect::Sqlite.placeholder(1), "?1");
        assert_eq!(Dialect::Sqlite.placeholder(10), "?10");
    }

    #[test]
    fn test_compose_with_values_single() {
        let composer = Composer::new(Dialect::Postgres);
        let template = Template {
            elements: vec![
                Element::Sql("SELECT * FROM users WHERE id = ".into()),
                Element::Bind(Binding {
                    name: "user_id".into(),
                    min_values: None,
                    max_values: None,
                    nullable: false,
                }),
            ],
            source: TemplateSource::Literal("test".into()),
        };
        let values: BTreeMap<String, Vec<i32>> = BTreeMap::from([("user_id".into(), vec![42])]);
        let result = composer.compose_with_values(&template, &values).unwrap();
        assert_eq!(result.sql, "SELECT * FROM users WHERE id = $1");
        assert_eq!(result.bind_params, vec!["user_id"]);
    }

    #[test]
    fn test_compose_with_values_multi_postgres() {
        let composer = Composer::new(Dialect::Postgres);
        let template = Template {
            elements: vec![
                Element::Sql("SELECT * FROM users WHERE id IN (".into()),
                Element::Bind(Binding {
                    name: "ids".into(),
                    min_values: Some(1),
                    max_values: None,
                    nullable: false,
                }),
                Element::Sql(")".into()),
            ],
            source: TemplateSource::Literal("test".into()),
        };
        let values: BTreeMap<String, Vec<i32>> = BTreeMap::from([("ids".into(), vec![10, 20, 30])]);
        let result = composer.compose_with_values(&template, &values).unwrap();
        assert_eq!(result.sql, "SELECT * FROM users WHERE id IN ($1, $2, $3)");
        assert_eq!(result.bind_params, vec!["ids", "ids", "ids"]);
    }

    #[test]
    fn test_compose_with_values_multi_mysql() {
        let composer = Composer::new(Dialect::Mysql);
        let template = Template {
            elements: vec![
                Element::Sql("SELECT * FROM users WHERE id IN (".into()),
                Element::Bind(Binding {
                    name: "ids".into(),
                    min_values: Some(1),
                    max_values: None,
                    nullable: false,
                }),
                Element::Sql(")".into()),
            ],
            source: TemplateSource::Literal("test".into()),
        };
        let values: BTreeMap<String, Vec<i32>> = BTreeMap::from([("ids".into(), vec![10, 20, 30])]);
        let result = composer.compose_with_values(&template, &values).unwrap();
        assert_eq!(result.sql, "SELECT * FROM users WHERE id IN (?, ?, ?)");
        assert_eq!(result.bind_params, vec!["ids", "ids", "ids"]);
    }

    #[test]
    fn test_compose_with_values_multi_sqlite() {
        let composer = Composer::new(Dialect::Sqlite);
        let template = Template {
            elements: vec![
                Element::Sql("SELECT * FROM users WHERE id IN (".into()),
                Element::Bind(Binding {
                    name: "ids".into(),
                    min_values: Some(1),
                    max_values: None,
                    nullable: false,
                }),
                Element::Sql(") AND status = ".into()),
                Element::Bind(Binding {
                    name: "status".into(),
                    min_values: None,
                    max_values: None,
                    nullable: false,
                }),
            ],
            source: TemplateSource::Literal("test".into()),
        };
        let values: BTreeMap<String, Vec<i32>> =
            BTreeMap::from([("ids".into(), vec![10, 20]), ("status".into(), vec![1])]);
        let result = composer.compose_with_values(&template, &values).unwrap();
        // Alphabetical: ids=(1,2), status=(3,1) → ids=?1,?2  status=?3
        assert_eq!(
            result.sql,
            "SELECT * FROM users WHERE id IN (?1, ?2) AND status = ?3"
        );
        assert_eq!(result.bind_params, vec!["ids", "ids", "status"]);
    }

    // ── Alphabetical ordering tests ───────────────────────────────────

    #[test]
    fn test_alphabetical_ordering_postgres() {
        let composer = Composer::new(Dialect::Postgres);
        let template = Template {
            elements: vec![
                Element::Sql("SELECT ".into()),
                Element::Bind(Binding {
                    name: "z_param".into(),
                    min_values: None,
                    max_values: None,
                    nullable: false,
                }),
                Element::Sql(", ".into()),
                Element::Bind(Binding {
                    name: "a_param".into(),
                    min_values: None,
                    max_values: None,
                    nullable: false,
                }),
            ],
            source: TemplateSource::Literal("test".into()),
        };
        let result = composer.compose(&template).unwrap();
        // a_param=$1 (alphabetically first), z_param=$2
        assert_eq!(result.sql, "SELECT $2, $1");
        assert_eq!(result.bind_params, vec!["a_param", "z_param"]);
    }

    #[test]
    fn test_alphabetical_ordering_sqlite() {
        let composer = Composer::new(Dialect::Sqlite);
        let template = Template {
            elements: vec![
                Element::Sql("SELECT ".into()),
                Element::Bind(Binding {
                    name: "z_param".into(),
                    min_values: None,
                    max_values: None,
                    nullable: false,
                }),
                Element::Sql(", ".into()),
                Element::Bind(Binding {
                    name: "a_param".into(),
                    min_values: None,
                    max_values: None,
                    nullable: false,
                }),
            ],
            source: TemplateSource::Literal("test".into()),
        };
        let result = composer.compose(&template).unwrap();
        assert_eq!(result.sql, "SELECT ?2, ?1");
        assert_eq!(result.bind_params, vec!["a_param", "z_param"]);
    }

    // ── Dedup tests ───────────────────────────────────────────────────

    #[test]
    fn test_dedup_single_value_postgres() {
        let composer = Composer::new(Dialect::Postgres);
        let template = Template {
            elements: vec![
                Element::Sql("WHERE a = ".into()),
                Element::Bind(Binding {
                    name: "x".into(),
                    min_values: None,
                    max_values: None,
                    nullable: false,
                }),
                Element::Sql(" AND b = ".into()),
                Element::Bind(Binding {
                    name: "x".into(),
                    min_values: None,
                    max_values: None,
                    nullable: false,
                }),
            ],
            source: TemplateSource::Literal("test".into()),
        };
        let result = composer.compose(&template).unwrap();
        // Both :bind(x) emit $1, bind_params has one entry
        assert_eq!(result.sql, "WHERE a = $1 AND b = $1");
        assert_eq!(result.bind_params, vec!["x"]);
    }

    #[test]
    fn test_dedup_multi_value_postgres() {
        let composer = Composer::new(Dialect::Postgres);
        let template = Template {
            elements: vec![
                Element::Sql("WHERE a IN (".into()),
                Element::Bind(Binding {
                    name: "ids".into(),
                    min_values: Some(1),
                    max_values: None,
                    nullable: false,
                }),
                Element::Sql(") AND b IN (".into()),
                Element::Bind(Binding {
                    name: "ids".into(),
                    min_values: Some(1),
                    max_values: None,
                    nullable: false,
                }),
                Element::Sql(")".into()),
            ],
            source: TemplateSource::Literal("test".into()),
        };
        let values: BTreeMap<String, Vec<i32>> = BTreeMap::from([("ids".into(), vec![10, 20, 30])]);
        let result = composer.compose_with_values(&template, &values).unwrap();
        // Both emit $1, $2, $3 — same placeholders
        assert_eq!(result.sql, "WHERE a IN ($1, $2, $3) AND b IN ($1, $2, $3)");
        assert_eq!(result.bind_params, vec!["ids", "ids", "ids"]);
    }

    #[test]
    fn test_mixed_multi_and_single_values() {
        let composer = Composer::new(Dialect::Postgres);
        let template = Template {
            elements: vec![
                Element::Sql("WHERE active = ".into()),
                Element::Bind(Binding {
                    name: "active".into(),
                    min_values: None,
                    max_values: None,
                    nullable: false,
                }),
                Element::Sql(" AND id IN (".into()),
                Element::Bind(Binding {
                    name: "ids".into(),
                    min_values: Some(1),
                    max_values: None,
                    nullable: false,
                }),
                Element::Sql(") AND user_id = ".into()),
                Element::Bind(Binding {
                    name: "user_id".into(),
                    min_values: None,
                    max_values: None,
                    nullable: false,
                }),
            ],
            source: TemplateSource::Literal("test".into()),
        };
        let values: BTreeMap<String, Vec<i32>> = BTreeMap::from([
            ("active".into(), vec![1]),
            ("ids".into(), vec![10, 20, 30]),
            ("user_id".into(), vec![42]),
        ]);
        let result = composer.compose_with_values(&template, &values).unwrap();
        // Alphabetical: active(1)=$1, ids(3)=$2,$3,$4, user_id(1)=$5
        assert_eq!(
            result.sql,
            "WHERE active = $1 AND id IN ($2, $3, $4) AND user_id = $5"
        );
        assert_eq!(
            result.bind_params,
            vec!["active", "ids", "ids", "ids", "user_id"]
        );
    }

    #[test]
    fn test_mysql_no_dedup() {
        let composer = Composer::new(Dialect::Mysql);
        let template = Template {
            elements: vec![
                Element::Sql("WHERE a = ".into()),
                Element::Bind(Binding {
                    name: "x".into(),
                    min_values: None,
                    max_values: None,
                    nullable: false,
                }),
                Element::Sql(" AND b = ".into()),
                Element::Bind(Binding {
                    name: "x".into(),
                    min_values: None,
                    max_values: None,
                    nullable: false,
                }),
            ],
            source: TemplateSource::Literal("test".into()),
        };
        let result = composer.compose(&template).unwrap();
        // MySQL: document order, no dedup, bare ?
        assert_eq!(result.sql, "WHERE a = ? AND b = ?");
        assert_eq!(result.bind_params, vec!["x", "x"]);
    }

    #[test]
    fn test_supports_numbered_placeholders() {
        assert!(Dialect::Postgres.supports_numbered_placeholders());
        assert!(Dialect::Sqlite.supports_numbered_placeholders());
        assert!(!Dialect::Mysql.supports_numbered_placeholders());
    }

    // ── Slot tests ────────────────────────────────────────────────────

    /// Helper: write a temp file and return its path.
    fn write_temp_file(dir: &TempDir, name: &str, content: &str) -> PathBuf {
        let path = dir.path().join(name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(content.as_bytes()).unwrap();
        path
    }

    #[test]
    fn test_slot_resolution_numbered() {
        let dir = TempDir::new().unwrap();

        // Filter template: standalone query
        write_temp_file(
            &dir,
            "filter.sqlc",
            "SELECT part_num FROM parts WHERE color = :bind(color)",
        );

        // Base template with @filter slot
        write_temp_file(
            &dir,
            "base.sqlc",
            "WITH f AS (\n    :compose(@filter)\n)\nSELECT * FROM f",
        );

        let mut composer = Composer::new(Dialect::Postgres);
        composer.add_search_path(dir.path().to_path_buf());

        // Caller template: fills the @filter slot
        let template = Template {
            elements: vec![Element::Compose(ComposeRef {
                target: ComposeTarget::Path(PathBuf::from("base.sqlc")),
                slots: vec![SlotAssignment {
                    name: "filter".into(),
                    path: PathBuf::from("filter.sqlc"),
                }],
            })],
            source: TemplateSource::Literal("test".into()),
        };

        let result = composer.compose(&template).unwrap();
        assert_eq!(
            result.sql,
            "WITH f AS (\n    SELECT part_num FROM parts WHERE color = $1\n)\nSELECT * FROM f"
        );
        assert_eq!(result.bind_params, vec!["color"]);
    }

    #[test]
    fn test_slot_resolution_positional() {
        let dir = TempDir::new().unwrap();

        write_temp_file(
            &dir,
            "filter.sqlc",
            "SELECT part_num FROM parts WHERE color = :bind(color)",
        );

        write_temp_file(
            &dir,
            "base.sqlc",
            "WITH f AS (\n    :compose(@filter)\n)\nSELECT * FROM f",
        );

        let mut composer = Composer::new(Dialect::Mysql);
        composer.add_search_path(dir.path().to_path_buf());

        let template = Template {
            elements: vec![Element::Compose(ComposeRef {
                target: ComposeTarget::Path(PathBuf::from("base.sqlc")),
                slots: vec![SlotAssignment {
                    name: "filter".into(),
                    path: PathBuf::from("filter.sqlc"),
                }],
            })],
            source: TemplateSource::Literal("test".into()),
        };

        let result = composer.compose(&template).unwrap();
        assert_eq!(
            result.sql,
            "WITH f AS (\n    SELECT part_num FROM parts WHERE color = ?\n)\nSELECT * FROM f"
        );
        assert_eq!(result.bind_params, vec!["color"]);
    }

    #[test]
    fn test_missing_slot_error() {
        let dir = TempDir::new().unwrap();

        // Template uses @filter but caller doesn't provide it
        write_temp_file(
            &dir,
            "base.sqlc",
            "WITH f AS (\n    :compose(@filter)\n)\nSELECT * FROM f",
        );

        let mut composer = Composer::new(Dialect::Postgres);
        composer.add_search_path(dir.path().to_path_buf());

        let template = Template {
            elements: vec![Element::Compose(ComposeRef {
                target: ComposeTarget::Path(PathBuf::from("base.sqlc")),
                slots: vec![], // no slots provided
            })],
            source: TemplateSource::Literal("test".into()),
        };

        let err = composer.compose(&template).unwrap_err();
        match err {
            Error::MissingSlot { name } => assert_eq!(name, "filter"),
            other => panic!("expected MissingSlot, got {:?}", other),
        }
    }

    #[test]
    fn test_slots_not_inherited() {
        let dir = TempDir::new().unwrap();

        // C uses @deep slot — should NOT inherit from A→B
        write_temp_file(&dir, "c.sqlc", "SELECT id FROM t WHERE x = :compose(@deep)");

        // B composes C without passing any slots
        write_temp_file(&dir, "b.sqlc", ":compose(c.sqlc)");

        // A composes B with @deep slot
        // But B doesn't pass @deep to C, so C should fail
        let mut composer = Composer::new(Dialect::Postgres);
        composer.add_search_path(dir.path().to_path_buf());

        let template = Template {
            elements: vec![Element::Compose(ComposeRef {
                target: ComposeTarget::Path(PathBuf::from("b.sqlc")),
                slots: vec![SlotAssignment {
                    name: "deep".into(),
                    path: PathBuf::from("filter.sqlc"),
                }],
            })],
            source: TemplateSource::Literal("test".into()),
        };

        let err = composer.compose(&template).unwrap_err();
        match err {
            Error::MissingSlot { name } => assert_eq!(name, "deep"),
            other => panic!("expected MissingSlot, got {:?}", other),
        }
    }

    #[test]
    fn test_explicit_slot_passthrough() {
        let dir = TempDir::new().unwrap();

        // Deep template uses @deep slot
        write_temp_file(&dir, "deep.sqlc", ":compose(@deep)");

        // Middle template explicitly passes @deep through
        write_temp_file(&dir, "middle.sqlc", ":compose(deep.sqlc, @deep = @deep)");

        // Hmm, @deep = @deep isn't valid — slot values are file paths.
        // Instead, the middle template must know the concrete path:
        // :compose(deep.sqlc, @deep = leaf.sqlc)
        // OR middle itself takes a slot and passes it.
        // But slots are file paths, not slot references.
        // Let me redesign this test.

        // leaf.sqlc — the concrete content
        write_temp_file(&dir, "leaf.sqlc", "SELECT 1");

        // deep.sqlc — uses @inner slot
        write_temp_file(&dir, "deep.sqlc", ":compose(@inner)");

        // middle.sqlc — composes deep.sqlc, passes @inner = leaf.sqlc
        write_temp_file(
            &dir,
            "middle.sqlc",
            ":compose(deep.sqlc, @inner = leaf.sqlc)",
        );

        let mut composer = Composer::new(Dialect::Postgres);
        composer.add_search_path(dir.path().to_path_buf());

        let template = Template {
            elements: vec![Element::Compose(ComposeRef {
                target: ComposeTarget::Path(PathBuf::from("middle.sqlc")),
                slots: vec![],
            })],
            source: TemplateSource::Literal("test".into()),
        };

        let result = composer.compose(&template).unwrap();
        assert_eq!(result.sql, "SELECT 1");
    }

    #[test]
    fn test_slot_circular_reference() {
        let dir = TempDir::new().unwrap();

        // a.sqlc composes b.sqlc with @slot = a.sqlc (circular)
        write_temp_file(&dir, "a.sqlc", ":compose(b.sqlc, @slot = a.sqlc)");
        write_temp_file(&dir, "b.sqlc", ":compose(@slot)");

        let mut composer = Composer::new(Dialect::Postgres);
        composer.add_search_path(dir.path().to_path_buf());

        let template = parser::parse_template_file(&dir.path().join("a.sqlc")).unwrap();
        let err = composer.compose(&template).unwrap_err();
        assert!(matches!(err, Error::CircularReference { .. }));
    }

    #[test]
    fn test_slotted_template_with_bind_params() {
        let dir = TempDir::new().unwrap();

        write_temp_file(
            &dir,
            "filter.sqlc",
            "SELECT id FROM items WHERE color = :bind(color)",
        );

        write_temp_file(
            &dir,
            "base.sqlc",
            "WITH f AS (\n    :compose(@filter)\n)\nSELECT * FROM f WHERE active = :bind(active)",
        );

        let mut composer = Composer::new(Dialect::Postgres);
        composer.add_search_path(dir.path().to_path_buf());

        let template = Template {
            elements: vec![Element::Compose(ComposeRef {
                target: ComposeTarget::Path(PathBuf::from("base.sqlc")),
                slots: vec![SlotAssignment {
                    name: "filter".into(),
                    path: PathBuf::from("filter.sqlc"),
                }],
            })],
            source: TemplateSource::Literal("test".into()),
        };

        let result = composer.compose(&template).unwrap();
        // Alphabetical: active=$1, color=$2
        assert_eq!(
            result.sql,
            "WITH f AS (\n    SELECT id FROM items WHERE color = $2\n)\nSELECT * FROM f WHERE active = $1"
        );
        assert_eq!(result.bind_params, vec!["active", "color"]);
    }

    #[test]
    fn test_slot_path_resolved_via_search_paths() {
        let dir = TempDir::new().unwrap();

        // Put the filter in a subdirectory
        write_temp_file(
            &dir,
            "filters/by_color.sqlc",
            "SELECT part_num FROM parts WHERE color = :bind(color)",
        );

        write_temp_file(
            &dir,
            "shared/base.sqlc",
            "WITH f AS (\n    :compose(@filter)\n)\nSELECT * FROM f",
        );

        let mut composer = Composer::new(Dialect::Postgres);
        composer.add_search_path(dir.path().to_path_buf());

        let template = Template {
            elements: vec![Element::Compose(ComposeRef {
                target: ComposeTarget::Path(PathBuf::from("shared/base.sqlc")),
                slots: vec![SlotAssignment {
                    name: "filter".into(),
                    path: PathBuf::from("filters/by_color.sqlc"),
                }],
            })],
            source: TemplateSource::Literal("test".into()),
        };

        let result = composer.compose(&template).unwrap();
        assert_eq!(
            result.sql,
            "WITH f AS (\n    SELECT part_num FROM parts WHERE color = $1\n)\nSELECT * FROM f"
        );
        assert_eq!(result.bind_params, vec!["color"]);
    }

    #[test]
    fn test_slot_target_reference() {
        // The target itself is a slot reference: :compose(@slot)
        let dir = TempDir::new().unwrap();

        write_temp_file(&dir, "inner.sqlc", "SELECT 42");

        let mut composer = Composer::new(Dialect::Postgres);
        composer.add_search_path(dir.path().to_path_buf());

        // Template with a slot reference as the compose target
        let template = Template {
            elements: vec![
                Element::Sql("WITH cte AS (\n    ".into()),
                Element::Compose(ComposeRef {
                    target: ComposeTarget::Slot("source".into()),
                    slots: vec![],
                }),
                Element::Sql("\n)\nSELECT * FROM cte".into()),
            ],
            source: TemplateSource::Literal("test".into()),
        };

        // Without providing the slot, should fail
        let err = composer.compose(&template).unwrap_err();
        assert!(matches!(err, Error::MissingSlot { .. }));

        // Now compose with slots provided via internal API
        let mut visited = HashSet::new();
        let mut slots = HashMap::new();
        slots.insert("source".into(), PathBuf::from("inner.sqlc"));
        let result = composer
            .compose_inner(&template, &slots, &mut visited)
            .unwrap();
        assert_eq!(
            result.sql,
            "WITH cte AS (\n    SELECT 42\n)\nSELECT * FROM cte"
        );
    }

    #[test]
    fn test_multiple_slots() {
        let dir = TempDir::new().unwrap();

        write_temp_file(&dir, "source.sqlc", "SELECT id, name FROM items");
        write_temp_file(
            &dir,
            "filter.sqlc",
            "SELECT id FROM items WHERE active = :bind(active)",
        );

        write_temp_file(
            &dir,
            "base.sqlc",
            "WITH src AS (\n    :compose(@source)\n),\nf AS (\n    :compose(@filter)\n)\nSELECT s.* FROM src s JOIN f ON f.id = s.id",
        );

        let mut composer = Composer::new(Dialect::Postgres);
        composer.add_search_path(dir.path().to_path_buf());

        let template = Template {
            elements: vec![Element::Compose(ComposeRef {
                target: ComposeTarget::Path(PathBuf::from("base.sqlc")),
                slots: vec![
                    SlotAssignment {
                        name: "source".into(),
                        path: PathBuf::from("source.sqlc"),
                    },
                    SlotAssignment {
                        name: "filter".into(),
                        path: PathBuf::from("filter.sqlc"),
                    },
                ],
            })],
            source: TemplateSource::Literal("test".into()),
        };

        let result = composer.compose(&template).unwrap();
        assert_eq!(
            result.sql,
            "WITH src AS (\n    SELECT id, name FROM items\n),\nf AS (\n    SELECT id FROM items WHERE active = $1\n)\nSELECT s.* FROM src s JOIN f ON f.id = s.id"
        );
        assert_eq!(result.bind_params, vec!["active"]);
    }

    // ── Views ─────────────────────────────────────────────────────────

    /// A registry of two views, `v.grand` and `v.child`, where `child` composes `grand`, and a
    /// parent that composes `child` from FROM, JOIN and EXISTS positions.
    fn view_fixture() -> (TempDir, Composer) {
        let dir = TempDir::new().unwrap();
        write_temp_file(&dir, "grand.sqlc", "SELECT y FROM t");
        write_temp_file(
            &dir,
            "child.sqlc",
            "SELECT * FROM (\n:compose(grand.sqlc)\n) g",
        );
        write_temp_file(
            &dir,
            "parent.sqlc",
            "SELECT * FROM (\n:compose(child.sqlc)\n) a JOIN (\n:compose(child.sqlc)\n) b USING (y) \
             WHERE EXISTS (\n:compose(grand.sqlc)\n)",
        );
        write_temp_file(
            &dir,
            "views.sqlc",
            "CREATE SCHEMA v;\nCREATE VIEW v.grand AS :define(grand.sqlc);\n\
             CREATE OR REPLACE VIEW v.child AS :define(child.sqlc);\n",
        );
        let mut composer = Composer::new(Dialect::Postgres);
        composer.add_search_path(dir.path().to_path_buf());
        composer
            .load_view_registry(Path::new("views.sqlc"))
            .unwrap();
        (dir, composer)
    }

    fn file_template(dir: &TempDir, name: &str) -> Template {
        parser::parse_template_file(&dir.path().join(name)).unwrap()
    }

    #[test]
    fn test_view_registry_names_every_create_view() {
        let (_dir, composer) = view_fixture();
        let mut names: Vec<&String> = composer.views.values().collect();
        names.sort();
        assert_eq!(names, vec!["v.child", "v.grand"]);
    }

    #[test]
    fn test_view_is_referenced_wherever_it_is_composed() {
        let (dir, composer) = view_fixture();
        let sql = composer
            .compose(&file_template(&dir, "parent.sqlc"))
            .unwrap()
            .sql;
        assert_eq!(
            sql,
            "SELECT * FROM (\nSELECT * FROM v.child\n) a JOIN (\nSELECT * FROM v.child\n) b USING (y) \
             WHERE EXISTS (\nSELECT * FROM v.grand\n)"
        );
    }

    #[test]
    fn test_registry_defines_each_view_one_level_deep() {
        let (dir, composer) = view_fixture();
        let sql = composer
            .compose(&file_template(&dir, "views.sqlc"))
            .unwrap()
            .sql;
        assert_eq!(
            sql,
            "CREATE SCHEMA v;\nCREATE VIEW v.grand AS SELECT y FROM t;\n\
             CREATE OR REPLACE VIEW v.child AS SELECT * FROM (\nSELECT * FROM v.grand\n) g;\n"
        );
    }

    #[test]
    fn test_view_is_referenced_as_a_union_member() {
        let (dir, composer) = view_fixture();
        write_temp_file(&dir, "other.sqlc", "SELECT 1 AS y");
        write_temp_file(&dir, "all.sqlc", ":union(ALL grand.sqlc, other.sqlc)");
        let sql = composer
            .compose(&file_template(&dir, "all.sqlc"))
            .unwrap()
            .sql;
        assert_eq!(sql, "SELECT * FROM v.grand\nUNION ALL\nSELECT 1 AS y");
    }

    #[test]
    fn test_without_a_registry_everything_inlines() {
        let (dir, _) = view_fixture();
        let mut composer = Composer::new(Dialect::Postgres);
        composer.add_search_path(dir.path().to_path_buf());
        let sql = composer
            .compose(&file_template(&dir, "child.sqlc"))
            .unwrap()
            .sql;
        assert_eq!(sql, "SELECT * FROM (\nSELECT y FROM t\n) g");
    }

    #[test]
    fn test_a_view_with_an_open_slot_is_refused() {
        let dir = TempDir::new().unwrap();
        write_temp_file(&dir, "shape.sqlc", "SELECT * FROM (:compose(@part)) x");
        write_temp_file(
            &dir,
            "views.sqlc",
            "CREATE VIEW v.shape AS :define(shape.sqlc);",
        );
        let mut composer = Composer::new(Dialect::Postgres);
        composer.add_search_path(dir.path().to_path_buf());
        match composer
            .load_view_registry(Path::new("views.sqlc"))
            .unwrap_err()
        {
            Error::ViewTakesArguments { view, reason, .. } => {
                assert_eq!(view, "v.shape");
                assert!(reason.contains("@part"), "{reason}");
            }
            other => panic!("expected ViewTakesArguments, got {other:?}"),
        }
    }

    #[test]
    fn test_a_view_that_binds_is_refused() {
        let dir = TempDir::new().unwrap();
        write_temp_file(&dir, "bound.sqlc", "SELECT * FROM t WHERE id = :bind(id)");
        write_temp_file(
            &dir,
            "views.sqlc",
            "CREATE VIEW v.bound AS :define(bound.sqlc);",
        );
        let mut composer = Composer::new(Dialect::Postgres);
        composer.add_search_path(dir.path().to_path_buf());
        match composer
            .load_view_registry(Path::new("views.sqlc"))
            .unwrap_err()
        {
            Error::ViewTakesArguments { view, reason, .. } => {
                assert_eq!(view, "v.bound");
                assert!(reason.contains("id"), "{reason}");
            }
            other => panic!("expected ViewTakesArguments, got {other:?}"),
        }
    }

    #[test]
    fn test_view_name_before_reads_the_create_view_it_ends() {
        assert_eq!(view_name_before("CREATE VIEW s.x AS "), Some("s.x".into()));
        assert_eq!(
            view_name_before(";\ncreate or replace view s.y as\n"),
            Some("s.y".into())
        );
        assert_eq!(view_name_before("SELECT * FROM ("), None);
        assert_eq!(view_name_before("CREATE VIEW s.z AS SELECT 1 UNION "), None);
    }

    #[test]
    fn test_cte_name_before_reads_the_with_clause_it_ends() {
        assert_eq!(cte_name_before("WITH grand AS ("), Some("grand".into()));
        assert_eq!(
            cte_name_before("WITH grand AS MATERIALIZED (\n"),
            Some("grand".into())
        );
        assert_eq!(
            cte_name_before(",\n     child AS NOT MATERIALIZED ("),
            Some("child".into())
        );
        assert_eq!(
            cte_name_before("with grand as materialized ("),
            Some("grand".into())
        );
        // A subquery is not a definition, and neither is a name with nothing in front of it.
        assert_eq!(cte_name_before("SELECT * FROM ("), None);
        assert_eq!(cte_name_before("WITH grand AS "), None);
        assert_eq!(cte_name_before("SELECT * FROM (\n"), None);
    }

    #[test]
    fn test_a_statement_defining_a_name_says_it_everywhere_underneath() {
        let (dir, composer) = view_fixture();
        // `parent` composes `child` twice and `grand` once, and `child` composes `grand`. Defining
        // `grand` here puts its body in the WITH clause once; every reference below says the name,
        // including the one inside `child`'s own body, which this statement never wrote.
        write_temp_file(
            &dir,
            "statement.sqlc",
            "WITH grand AS MATERIALIZED (\n:define(grand.sqlc)\n)\n:compose(parent.sqlc)",
        );
        let sql = composer
            .compose(&file_template(&dir, "statement.sqlc"))
            .unwrap()
            .sql;
        assert_eq!(
            sql,
            "WITH grand AS MATERIALIZED (\nSELECT y FROM t\n)\n\
             SELECT * FROM (\nSELECT * FROM v.child\n) a JOIN (\nSELECT * FROM v.child\n) b \
             USING (y) WHERE EXISTS (\nSELECT * FROM grand\n)"
        );
        // The declaration is the statement's own: the next composition is unaffected.
        let after = composer
            .compose(&file_template(&dir, "parent.sqlc"))
            .unwrap()
            .sql;
        assert!(after.contains("SELECT * FROM v.grand"), "{after}");
        assert!(!after.contains("FROM grand"), "{after}");
    }

    #[test]
    fn test_a_local_name_stands_in_front_of_the_view_of_the_same_relation() {
        let (dir, composer) = view_fixture();
        // `child` is a view AND is defined here. The local name wins everywhere below, so the
        // statement reads one relation by one name rather than two names for one relation.
        write_temp_file(
            &dir,
            "statement.sqlc",
            "WITH child AS NOT MATERIALIZED (\n:define(child.sqlc)\n)\n:compose(parent.sqlc)",
        );
        let sql = composer
            .compose(&file_template(&dir, "statement.sqlc"))
            .unwrap()
            .sql;
        assert_eq!(
            sql,
            "WITH child AS NOT MATERIALIZED (\nSELECT * FROM (\nSELECT * FROM v.grand\n) g\n)\n\
             SELECT * FROM (\nSELECT * FROM child\n) a JOIN (\nSELECT * FROM child\n) b \
             USING (y) WHERE EXISTS (\nSELECT * FROM v.grand\n)"
        );
    }

    #[test]
    fn test_define_inlines_where_compose_would_name() {
        let (dir, composer) = view_fixture();
        // Same relation, same place, the two directives: `:compose` says which relation, and
        // `:define` says where its body goes.
        write_temp_file(
            &dir,
            "named.sqlc",
            "SELECT * FROM (\n:compose(grand.sqlc)\n) g",
        );
        write_temp_file(
            &dir,
            "defined.sqlc",
            "SELECT * FROM (\n:define(grand.sqlc)\n) g",
        );
        assert_eq!(
            composer
                .compose(&file_template(&dir, "named.sqlc"))
                .unwrap()
                .sql,
            "SELECT * FROM (\nSELECT * FROM v.grand\n) g"
        );
        assert_eq!(
            composer
                .compose(&file_template(&dir, "defined.sqlc"))
                .unwrap()
                .sql,
            "SELECT * FROM (\nSELECT y FROM t\n) g"
        );
    }

    #[test]
    fn test_define_and_compose_are_one_relation_without_a_registry() {
        // With nothing named, the two directives are the same splice, which is the ground the
        // proof that a name and its definition agree stands on.
        let (dir, _) = view_fixture();
        write_temp_file(
            &dir,
            "named.sqlc",
            "SELECT * FROM (\n:compose(child.sqlc)\n) g",
        );
        write_temp_file(
            &dir,
            "defined.sqlc",
            "SELECT * FROM (\n:define(child.sqlc)\n) g",
        );
        let mut composer = Composer::new(Dialect::Postgres);
        composer.add_search_path(dir.path().to_path_buf());
        assert_eq!(
            composer
                .compose(&file_template(&dir, "named.sqlc"))
                .unwrap()
                .sql,
            composer
                .compose(&file_template(&dir, "defined.sqlc"))
                .unwrap()
                .sql
        );
    }

    fn set_operation_fixture(dialect: Dialect) -> (TempDir, Composer) {
        let dir = TempDir::new().unwrap();
        write_temp_file(&dir, "a.sqlc", "SELECT id FROM a WHERE k = :bind(k)\n");
        write_temp_file(&dir, "b.sqlc", "SELECT id FROM b");
        write_temp_file(
            &dir,
            "c.sqlc",
            "-- c's provenance\nSELECT id FROM c WHERE j = :bind(j)",
        );
        write_temp_file(&dir, "b_or_c.sqlc", ":union(b.sqlc, c.sqlc)");
        let mut composer = Composer::new(dialect);
        composer.add_search_path(dir.path().to_path_buf());
        (dir, composer)
    }

    #[test]
    fn test_intersect_wraps_every_source_and_the_whole() {
        let (dir, composer) = set_operation_fixture(Dialect::Postgres);
        write_temp_file(&dir, "both.sqlc", ":intersect(a.sqlc, b.sqlc)");
        let composed = composer.compose(&file_template(&dir, "both.sqlc")).unwrap();
        assert_eq!(
            composed.sql,
            "SELECT * FROM (\nSELECT * FROM (\nSELECT id FROM a WHERE k = $1\n) AS _intersect_1\n\
             INTERSECT\nSELECT * FROM (\nSELECT id FROM b\n) AS _intersect_2\n) AS _intersect"
        );
        assert_eq!(composed.bind_params, vec!["k"]);
    }

    #[test]
    fn test_except_keeps_a_union_source_grouped() {
        // Spliced bare, `a EXCEPT b UNION c` is `(a EXCEPT b) UNION c`: every row of c comes back.
        let (dir, composer) = set_operation_fixture(Dialect::Postgres);
        write_temp_file(&dir, "neither.sqlc", ":except(a.sqlc, b_or_c.sqlc)");
        let composed = composer
            .compose(&file_template(&dir, "neither.sqlc"))
            .unwrap();
        assert_eq!(
            composed.sql,
            "SELECT * FROM (\nSELECT * FROM (\nSELECT id FROM a WHERE k = $2\n) AS _except_1\n\
             EXCEPT\nSELECT * FROM (\nSELECT id FROM b\nUNION\n-- c's provenance\n\
             SELECT id FROM c WHERE j = $1\n) AS _except_2\n) AS _except"
        );
        assert_eq!(composed.bind_params, vec!["j", "k"]);
    }

    #[test]
    fn test_a_set_operation_is_closed_as_a_union_member() {
        let (dir, composer) = set_operation_fixture(Dialect::Postgres);
        write_temp_file(&dir, "a_not_b.sqlc", ":except(a.sqlc, b.sqlc)");
        write_temp_file(&dir, "either.sqlc", ":union(ALL a_not_b.sqlc, c.sqlc)");
        let sql = composer
            .compose(&file_template(&dir, "either.sqlc"))
            .unwrap()
            .sql;
        assert_eq!(
            sql,
            "SELECT * FROM (\nSELECT * FROM (\nSELECT id FROM a WHERE k = $2\n) AS _except_1\n\
             EXCEPT\nSELECT * FROM (\nSELECT id FROM b\n) AS _except_2\n) AS _except\n\
             UNION ALL\n-- c's provenance\nSELECT id FROM c WHERE j = $1"
        );
    }

    #[test]
    fn test_set_operation_modifiers() {
        let (dir, composer) = set_operation_fixture(Dialect::Postgres);
        for (template, operator) in [
            (":except(ALL a.sqlc, b.sqlc)", "\nEXCEPT ALL\n"),
            (":except(DISTINCT a.sqlc, b.sqlc)", "\nEXCEPT DISTINCT\n"),
            (":intersect(ALL a.sqlc, b.sqlc)", "\nINTERSECT ALL\n"),
            (
                ":intersect(DISTINCT a.sqlc, b.sqlc)",
                "\nINTERSECT DISTINCT\n",
            ),
        ] {
            write_temp_file(&dir, "op.sqlc", template);
            let sql = composer
                .compose(&file_template(&dir, "op.sqlc"))
                .unwrap()
                .sql;
            assert!(sql.contains(operator), "{template} composed to {sql}");
        }
    }

    #[test]
    fn test_set_operation_positional_matches_numbered_text() {
        let (dir, postgres) = set_operation_fixture(Dialect::Postgres);
        let mut mysql = Composer::new(Dialect::Mysql);
        mysql.add_search_path(dir.path().to_path_buf());
        write_temp_file(&dir, "neither.sqlc", ":except(a.sqlc, b_or_c.sqlc)");
        let composed = mysql.compose(&file_template(&dir, "neither.sqlc")).unwrap();
        // Document order: a's bind comes before c's.
        assert_eq!(composed.bind_params, vec!["k", "j"]);

        let numbered = postgres
            .compose(&file_template(&dir, "neither.sqlc"))
            .unwrap()
            .sql;
        assert_eq!(composed.sql, numbered.replace("$1", "?").replace("$2", "?"));
    }

    #[test]
    fn test_set_operation_refuses_a_column_list() {
        for dialect in [Dialect::Postgres, Dialect::Mysql] {
            let (dir, composer) = set_operation_fixture(dialect);
            write_temp_file(&dir, "cols.sqlc", ":except(id OF a.sqlc, b.sqlc)");
            let err = composer
                .compose(&file_template(&dir, "cols.sqlc"))
                .unwrap_err();
            assert!(
                matches!(err, Error::Parse { .. }),
                "{dialect:?}: expected a refusal, got {err:?}"
            );
        }
    }

    #[test]
    fn test_view_is_referenced_as_a_set_operation_source() {
        let (dir, composer) = view_fixture();
        write_temp_file(&dir, "other.sqlc", "SELECT 1 AS y");
        write_temp_file(&dir, "rest.sqlc", ":except(grand.sqlc, other.sqlc)");
        let sql = composer
            .compose(&file_template(&dir, "rest.sqlc"))
            .unwrap()
            .sql;
        assert_eq!(
            sql,
            "SELECT * FROM (\nSELECT * FROM (\nSELECT * FROM v.grand\n) AS _except_1\n\
             EXCEPT\nSELECT * FROM (\nSELECT 1 AS y\n) AS _except_2\n) AS _except"
        );
    }

    // ── Named relations ───────────────────────────────────────────────

    fn named_fixture(dialect: Dialect) -> (TempDir, Composer) {
        let dir = TempDir::new().unwrap();
        write_temp_file(&dir, "rel/grand.sqlc", "SELECT y FROM t");
        write_temp_file(
            &dir,
            "rel/child.sqlc",
            "SELECT * FROM (\n:compose(rel/grand.sqlc)\n) g",
        );
        write_temp_file(
            &dir,
            "parent.sqlc",
            "SELECT * FROM (\n:compose(rel/child.sqlc)\n) a JOIN (\n:compose(rel/child.sqlc)\n) b \
             USING (y) WHERE EXISTS (\n:compose(rel/grand.sqlc)\n)",
        );
        let mut composer = Composer::new(dialect);
        composer.add_search_path(dir.path().to_path_buf());
        composer.name_relations = true;
        (dir, composer)
    }

    fn named(dir: &TempDir, composer: &Composer, name: &str) -> Result<ComposedSql> {
        composer.compose(&file_template(dir, name))
    }

    #[test]
    fn test_named_relations_are_written_once_at_the_head() {
        let (dir, composer) = named_fixture(Dialect::Postgres);
        assert_eq!(
            named(&dir, &composer, "parent.sqlc").unwrap().sql,
            "WITH rel_grand AS (\nSELECT y FROM t\n),\n\
             rel_child AS (\nSELECT * FROM (\nSELECT * FROM rel_grand\n) g\n)\n\
             SELECT * FROM (\nSELECT * FROM rel_child\n) a JOIN (\nSELECT * FROM rel_child\n) b \
             USING (y) WHERE EXISTS (\nSELECT * FROM rel_grand\n)"
        );
    }

    #[test]
    fn test_named_relations_read_the_same_in_every_dialect() {
        let (dir, postgres) = named_fixture(Dialect::Postgres);
        let mut mysql = postgres.clone();
        mysql.dialect = Dialect::Mysql;
        assert_eq!(
            named(&dir, &mysql, "parent.sqlc").unwrap(),
            named(&dir, &postgres, "parent.sqlc").unwrap()
        );
    }

    #[test]
    fn test_named_relations_lead_a_statements_own_with() {
        let (dir, composer) = named_fixture(Dialect::Postgres);
        write_temp_file(
            &dir,
            "recursive.sqlc",
            "-- a statement with its own WITH\nWITH RECURSIVE r AS (\nSELECT * FROM (\n\
             :compose(rel/grand.sqlc)\n) x\n)\nSELECT * FROM r",
        );
        assert_eq!(
            named(&dir, &composer, "recursive.sqlc").unwrap().sql,
            "-- a statement with its own WITH\nWITH RECURSIVE\nrel_grand AS (\nSELECT y FROM t\n),\n\
             r AS (\nSELECT * FROM (\nSELECT * FROM rel_grand\n) x\n)\nSELECT * FROM r"
        );
    }

    #[test]
    fn test_named_relations_go_after_the_as_of_a_create() {
        let (dir, composer) = named_fixture(Dialect::Postgres);
        write_temp_file(
            &dir,
            "create.sqlc",
            "CREATE TEMP TABLE kept (y) AS\n:compose(rel/grand.sqlc);",
        );
        assert_eq!(
            named(&dir, &composer, "create.sqlc").unwrap().sql,
            "CREATE TEMP TABLE kept (y) AS\nWITH rel_grand AS (\nSELECT y FROM t\n)\n\
             SELECT * FROM rel_grand;"
        );
    }

    #[test]
    fn test_each_statement_of_a_script_names_what_it_reads() {
        let (dir, composer) = named_fixture(Dialect::Postgres);
        write_temp_file(
            &dir,
            "script.sqlc",
            "\\echo 'one; and only one'\nSELECT ';' AS s, y FROM (\n:compose(rel/grand.sqlc)\n) a;\n\
             -- a comment; not a statement\n:compose(rel/child.sqlc);\nSELECT 1;\n",
        );
        assert_eq!(
            named(&dir, &composer, "script.sqlc").unwrap().sql,
            "\\echo 'one; and only one'\nWITH rel_grand AS (\nSELECT y FROM t\n)\n\
             SELECT ';' AS s, y FROM (\nSELECT * FROM rel_grand\n) a;\n\
             -- a comment; not a statement\nWITH rel_grand AS (\nSELECT y FROM t\n),\n\
             rel_child AS (\nSELECT * FROM (\nSELECT * FROM rel_grand\n) g\n)\n\
             SELECT * FROM rel_child;\nSELECT 1;\n"
        );
    }

    #[test]
    fn test_a_statement_ends_only_outside_quotes_comments_and_dollar_quotes() {
        let text =
            "SELECT ';', \";\" -- ;\n/* ; /* ; */ ; */ $$;$$, $q$;$q$, $1;\n\\echo ;\nSELECT 2;";
        let ends = Scanner::default().ends(text);
        assert_eq!(ends.len(), 2, "{ends:?}");
        assert!(text[..ends[0]].ends_with("$q$;$q$, $1;"));
        assert_eq!(ends[1], text.len());
    }

    #[test]
    fn test_named_relations_take_union_members() {
        let (dir, composer) = named_fixture(Dialect::Postgres);
        write_temp_file(&dir, "other.sqlc", "SELECT 1 AS y");
        write_temp_file(&dir, "all.sqlc", ":union(ALL rel/grand.sqlc, other.sqlc)");
        assert_eq!(
            named(&dir, &composer, "all.sqlc").unwrap().sql,
            "WITH rel_grand AS (\nSELECT y FROM t\n),\nother AS (\nSELECT 1 AS y\n)\n\
             SELECT * FROM rel_grand\nUNION ALL\nSELECT * FROM other"
        );
    }

    #[test]
    fn test_a_slot_filled_composition_is_written_in_place() {
        let (dir, composer) = named_fixture(Dialect::Postgres);
        write_temp_file(&dir, "shape.sqlc", "SELECT * FROM (\n:compose(@part)\n) x");
        write_temp_file(
            &dir,
            "filled.sqlc",
            "SELECT * FROM (\n:compose(shape.sqlc, @part = rel/grand.sqlc)\n) s",
        );
        assert_eq!(
            named(&dir, &composer, "filled.sqlc").unwrap().sql,
            "WITH rel_grand AS (\nSELECT y FROM t\n)\n\
             SELECT * FROM (\nSELECT * FROM (\nSELECT * FROM rel_grand\n) x\n) s"
        );
        assert!(matches!(
            named(&dir, &composer, "shape.sqlc").unwrap_err(),
            Error::MissingSlot { .. }
        ));
    }

    #[test]
    fn test_a_relation_that_binds_is_written_in_place() {
        let (dir, composer) = named_fixture(Dialect::Postgres);
        write_temp_file(&dir, "bound.sqlc", "SELECT * FROM t WHERE id = :bind(id)");
        write_temp_file(
            &dir,
            "reads.sqlc",
            "SELECT * FROM (\n:compose(bound.sqlc)\n) b",
        );
        let composed = named(&dir, &composer, "reads.sqlc").unwrap();
        assert_eq!(
            composed.sql,
            "SELECT * FROM (\nSELECT * FROM t WHERE id = $1\n) b"
        );
        assert_eq!(composed.bind_params, vec!["id"]);
    }

    #[test]
    fn test_a_define_is_refused_while_naming() {
        let (dir, composer) = named_fixture(Dialect::Postgres);
        write_temp_file(
            &dir,
            "defines.sqlc",
            "WITH g AS (\n:define(rel/grand.sqlc)\n)\nSELECT * FROM g",
        );
        assert!(matches!(
            named(&dir, &composer, "defines.sqlc").unwrap_err(),
            Error::DefinitionWhileNaming { .. }
        ));
    }

    #[test]
    fn test_a_reserved_name_is_refused() {
        let (dir, composer) = named_fixture(Dialect::Postgres);
        write_temp_file(&dir, "user.sqlc", "SELECT 1 AS y");
        write_temp_file(
            &dir,
            "reads.sqlc",
            "SELECT * FROM (\n:compose(user.sqlc)\n) u",
        );
        match named(&dir, &composer, "reads.sqlc").unwrap_err() {
            Error::ReservedName { name, .. } => assert_eq!(name, "user"),
            other => panic!("expected ReservedName, got {other:?}"),
        }
    }

    #[test]
    fn test_two_files_one_name_is_refused() {
        let (dir, composer) = named_fixture(Dialect::Postgres);
        write_temp_file(&dir, "a_b/c.sqlc", "SELECT 1 AS y");
        write_temp_file(&dir, "a/b_c.sqlc", "SELECT 2 AS y");
        write_temp_file(&dir, "both.sqlc", ":union(ALL a_b/c.sqlc, a/b_c.sqlc)");
        match named(&dir, &composer, "both.sqlc").unwrap_err() {
            Error::NameTaken { name, .. } => assert_eq!(name, "a_b_c"),
            other => panic!("expected NameTaken, got {other:?}"),
        }
    }

    #[test]
    fn test_a_relation_that_reads_itself_is_refused() {
        let (dir, composer) = named_fixture(Dialect::Postgres);
        write_temp_file(
            &dir,
            "loop/a.sqlc",
            "SELECT * FROM (\n:compose(loop/b.sqlc)\n) b",
        );
        write_temp_file(
            &dir,
            "loop/b.sqlc",
            "SELECT * FROM (\n:compose(loop/a.sqlc)\n) a",
        );
        assert!(matches!(
            named(&dir, &composer, "loop/a.sqlc").unwrap_err(),
            Error::CircularReference { .. }
        ));
    }
}
