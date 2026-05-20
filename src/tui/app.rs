use crate::cli::Cli;
use crate::db;
use crate::output::{subdir_for, Change};

pub(super) enum AppState {
    InputUrls,
    Generating,
    ViewDdl,
    Confirming,
    Applying,
    Done,
}

/// A tree node = the SQL for one logical destination (a table, or
/// `_schema` for non-table-scoped DDL like `CREATE TYPE`). The user
/// toggles whole nodes on or off; `apply` runs the SQL of every checked
/// node, `_schema` first.
pub(super) struct TreeNode {
    /// Display name: `_schema`, `<table>`, or `<schema>__<table>`. Same
    /// convention as `--out-dir` subdirectory names so a user reading
    /// the TUI sees the same labels they'd see on disk.
    pub(super) name: String,
    pub(super) changes: Vec<Change>,
    pub(super) checked: bool,
}

pub(super) struct App {
    pub(super) state: AppState,
    pub(super) source_url: String,
    pub(super) target_url: String,
    pub(super) focused_field: usize, // 0 = source, 1 = target
    pub(super) cursor_pos: [usize; 2],

    /// Grouped output of the diff. `empty_diff` is set instead when the
    /// diff produced zero changes - `nodes` stays empty in that case.
    pub(super) nodes: Vec<TreeNode>,
    pub(super) selected_idx: usize,
    /// Per-tree-node detail scroll. Reset on selection change.
    pub(super) scroll_offset: u16,
    pub(super) empty_diff: bool,
    /// Cached executable statement count for checked nodes. Kept in sync
    /// when nodes are loaded or toggled so rendering and key handling do
    /// not rebuild the full checked SQL blob every frame.
    pub(super) executable_statement_count: usize,

    pub(super) status_msg: String,
    pub(super) error_msg: Option<String>,
    pub(super) success_msg: Option<String>,
    pub(super) apply_results: Vec<db::StmtResult>,
    pub(super) trust_cert: bool,
}

impl App {
    pub(super) fn new(cli: &Cli) -> Self {
        let source_url = cli.url.clone().unwrap_or_default();
        let source_len = source_url.len();
        let target_len = cli.target_url.as_ref().map_or(0, |u| u.len());
        Self {
            state: AppState::InputUrls,
            source_url,
            target_url: cli.target_url.clone().unwrap_or_default(),
            focused_field: if source_len == 0 { 0 } else { 1 },
            cursor_pos: [source_len, target_len],
            nodes: Vec::new(),
            selected_idx: 0,
            scroll_offset: 0,
            empty_diff: false,
            executable_statement_count: 0,
            status_msg: String::new(),
            error_msg: None,
            success_msg: None,
            apply_results: Vec::new(),
            trust_cert: cli.trust_cert,
        }
    }

    pub(super) fn active_input(&self) -> &str {
        if self.focused_field == 0 {
            &self.source_url
        } else {
            &self.target_url
        }
    }

    pub(super) fn active_input_mut(&mut self) -> &mut String {
        if self.focused_field == 0 {
            &mut self.source_url
        } else {
            &mut self.target_url
        }
    }

    pub(super) fn cursor(&self) -> usize {
        self.cursor_pos[self.focused_field]
    }

    pub(super) fn set_cursor(&mut self, pos: usize) {
        self.cursor_pos[self.focused_field] = pos;
    }

    pub(super) fn selected_node(&self) -> Option<&TreeNode> {
        self.nodes.get(self.selected_idx)
    }

    pub(super) fn checked_count(&self) -> usize {
        self.nodes.iter().filter(|n| n.checked).count()
    }

    pub(super) fn executable_statement_count(&self) -> usize {
        self.executable_statement_count
    }

    pub(super) fn set_nodes(&mut self, nodes: Vec<TreeNode>) {
        self.nodes = nodes;
        self.refresh_executable_statement_count();
    }

    pub(super) fn toggle_selected_node(&mut self) {
        if let Some(node) = self.nodes.get_mut(self.selected_idx) {
            node.checked = !node.checked;
            self.refresh_executable_statement_count();
        }
    }

    pub(super) fn toggle_all_nodes(&mut self) {
        let any_checked = self.nodes.iter().any(|n| n.checked);
        for node in self.nodes.iter_mut() {
            node.checked = !any_checked;
        }
        self.refresh_executable_statement_count();
    }

    pub(super) fn refresh_executable_statement_count(&mut self) {
        self.executable_statement_count = count_checked_statements(&self.nodes);
    }
}

/// Group a flat `Vec<Change>` into tree nodes, one per unique
/// destination subdir (`_schema` for non-table-scoped DDL, `<table>` or
/// `<schema>__<table>` otherwise). Insertion order is preserved so the
/// topological sort from `compute_changes` survives into the apply path.
pub(super) fn group_changes(changes: Vec<Change>) -> Vec<TreeNode> {
    let mut nodes: Vec<TreeNode> = Vec::new();
    for change in changes {
        let bucket = subdir_for(&change);
        match nodes.iter_mut().find(|n| n.name == bucket) {
            Some(n) => n.changes.push(change),
            None => nodes.push(TreeNode {
                name: bucket,
                changes: vec![change],
                checked: true,
            }),
        }
    }
    nodes
}

/// Render the SQL of a single tree node for the detail pane. Stable
/// across re-renders (does not depend on terminal width).
pub(super) fn node_detail_text(node: &TreeNode) -> String {
    node.changes
        .iter()
        .map(|c| c.sql.as_str())
        .collect::<Vec<_>>()
        .join("\n\n")
}

pub(super) fn node_detail_line_count(node: &TreeNode) -> u16 {
    let lines = node
        .changes
        .iter()
        .enumerate()
        .map(|(index, change)| change.sql.lines().count() + if index == 0 { 0 } else { 1 })
        .sum::<usize>();
    lines.min(u16::MAX as usize) as u16
}

fn count_checked_statements(nodes: &[TreeNode]) -> usize {
    nodes
        .iter()
        .filter(|node| node.checked)
        .flat_map(|node| &node.changes)
        .map(|change| db::split_statements(&change.sql).len())
        .sum()
}
