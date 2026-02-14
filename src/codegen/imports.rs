use std::collections::{BTreeMap, BTreeSet};

/// Collects and renders Python import statements.
///
/// Groups imports by module and sorts them for deterministic output.
#[derive(Debug, Default)]
pub struct ImportCollector {
    /// module -> set of names
    imports: BTreeMap<String, BTreeSet<String>>,
}

impl ImportCollector {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an import: `from {module} import {name}`.
    pub fn add(&mut self, module: &str, name: &str) {
        self.imports
            .entry(module.to_string())
            .or_default()
            .insert(name.to_string());
    }

    /// Add a bare import: `import {module}`.
    /// We track these with a special key.
    pub fn add_bare(&mut self, module: &str) {
        self.imports
            .entry(format!("__bare__{module}"))
            .or_default()
            .insert(module.to_string());
    }

    /// Render all import statements as a string.
    ///
    /// Output order:
    /// 1. `from __future__` imports
    /// 2. Standard library `from` imports
    /// 3. Bare `import` statements for stdlib modules
    /// 4. Third-party `from` imports (sqlalchemy, etc.)
    pub fn render(&self) -> String {
        let mut lines: Vec<String> = Vec::new();

        // Separate bare imports, typing imports, stdlib imports, and third-party imports
        let mut bare_imports: Vec<String> = Vec::new();
        let mut typing_imports: Vec<(String, Vec<String>)> = Vec::new();
        let mut sqlalchemy_imports: Vec<(String, Vec<String>)> = Vec::new();
        let mut sqlalchemy_dialect_imports: Vec<(String, Vec<String>)> = Vec::new();
        let mut sqlalchemy_orm_imports: Vec<(String, Vec<String>)> = Vec::new();

        for (module, names) in &self.imports {
            if let Some(bare_module) = module.strip_prefix("__bare__") {
                bare_imports.push(bare_module.to_string());
            } else if module == "typing" {
                let sorted_names: Vec<String> = names.iter().cloned().collect();
                typing_imports.push((module.clone(), sorted_names));
            } else if module == "sqlalchemy" {
                let sorted_names: Vec<String> = names.iter().cloned().collect();
                sqlalchemy_imports.push((module.clone(), sorted_names));
            } else if module.starts_with("sqlalchemy.dialects") {
                let sorted_names: Vec<String> = names.iter().cloned().collect();
                sqlalchemy_dialect_imports.push((module.clone(), sorted_names));
            } else if module.starts_with("sqlalchemy.orm") {
                let sorted_names: Vec<String> = names.iter().cloned().collect();
                sqlalchemy_orm_imports.push((module.clone(), sorted_names));
            }
        }

        // 1. typing imports
        for (module, names) in &typing_imports {
            lines.push(format!("from {} import {}", module, names.join(", ")));
        }

        // 2. Blank line separator if we had typing imports and have more
        if !typing_imports.is_empty()
            && (!bare_imports.is_empty()
                || !sqlalchemy_imports.is_empty()
                || !sqlalchemy_dialect_imports.is_empty()
                || !sqlalchemy_orm_imports.is_empty())
        {
            lines.push(String::new());
        }

        // 3. bare imports (e.g. `import datetime`)
        bare_imports.sort();
        for module in &bare_imports {
            lines.push(format!("import {module}"));
        }

        // 4. sqlalchemy imports
        for (module, names) in &sqlalchemy_imports {
            lines.push(format!("from {} import {}", module, names.join(", ")));
        }

        // 5. sqlalchemy dialect imports
        for (module, names) in &sqlalchemy_dialect_imports {
            lines.push(format!("from {} import {}", module, names.join(", ")));
        }

        // 6. sqlalchemy.orm imports
        for (module, names) in &sqlalchemy_orm_imports {
            lines.push(format!("from {} import {}", module, names.join(", ")));
        }

        lines.join("\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_imports() {
        let mut ic = ImportCollector::new();
        ic.add("sqlalchemy", "Integer");
        ic.add("sqlalchemy", "String");
        ic.add("sqlalchemy", "Column");
        let result = ic.render();
        assert_eq!(result, "from sqlalchemy import Column, Integer, String");
    }

    #[test]
    fn test_mixed_imports() {
        let mut ic = ImportCollector::new();
        ic.add("typing", "Optional");
        ic.add_bare("datetime");
        ic.add("sqlalchemy", "Integer");
        ic.add("sqlalchemy.orm", "DeclarativeBase");
        let result = ic.render();
        let expected = "from typing import Optional\n\nimport datetime\nfrom sqlalchemy import Integer\nfrom sqlalchemy.orm import DeclarativeBase";
        assert_eq!(result, expected);
    }

    #[test]
    fn test_dialect_imports() {
        let mut ic = ImportCollector::new();
        ic.add("sqlalchemy", "Integer");
        ic.add("sqlalchemy.dialects.postgresql", "JSONB");
        let result = ic.render();
        assert_eq!(
            result,
            "from sqlalchemy import Integer\nfrom sqlalchemy.dialects.postgresql import JSONB"
        );
    }
}
