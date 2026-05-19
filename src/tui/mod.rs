use std::time::Duration;

use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph, Wrap};
use ratatui::Frame;

use crate::cli::{Cli, DdlOptions};
use crate::codegen::ddl_diff::compute_changes;
use crate::db;
use crate::output::{subdir_for, Change};

// ---------------------------------------------------------------------------
// State machine
// ---------------------------------------------------------------------------

enum AppState {
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
struct TreeNode {
    /// Display name: `_schema`, `<table>`, or `<schema>__<table>`. Same
    /// convention as `--out-dir` subdirectory names so a user reading
    /// the TUI sees the same labels they'd see on disk.
    name: String,
    changes: Vec<Change>,
    checked: bool,
}

struct App {
    state: AppState,
    source_url: String,
    target_url: String,
    focused_field: usize, // 0 = source, 1 = target
    cursor_pos: [usize; 2],

    /// Grouped output of the diff. `empty_diff` is set instead when the
    /// diff produced zero changes — `nodes` stays empty in that case.
    nodes: Vec<TreeNode>,
    selected_idx: usize,
    /// Per-tree-node detail scroll. Reset on selection change.
    scroll_offset: u16,
    empty_diff: bool,

    status_msg: String,
    error_msg: Option<String>,
    success_msg: Option<String>,
    apply_results: Vec<db::StmtResult>,
    trust_cert: bool,
}

impl App {
    fn new(cli: &Cli) -> Self {
        Self {
            state: AppState::InputUrls,
            source_url: cli.url.clone(),
            target_url: cli.target_url.clone().unwrap_or_default(),
            focused_field: if cli.url.is_empty() { 0 } else { 1 },
            cursor_pos: [cli.url.len(), cli.target_url.as_ref().map_or(0, |u| u.len())],
            nodes: Vec::new(),
            selected_idx: 0,
            scroll_offset: 0,
            empty_diff: false,
            status_msg: String::new(),
            error_msg: None,
            success_msg: None,
            apply_results: Vec::new(),
            trust_cert: cli.trust_cert,
        }
    }

    fn active_input(&self) -> &str {
        if self.focused_field == 0 {
            &self.source_url
        } else {
            &self.target_url
        }
    }

    fn active_input_mut(&mut self) -> &mut String {
        if self.focused_field == 0 {
            &mut self.source_url
        } else {
            &mut self.target_url
        }
    }

    fn cursor(&self) -> usize {
        self.cursor_pos[self.focused_field]
    }

    fn set_cursor(&mut self, pos: usize) {
        self.cursor_pos[self.focused_field] = pos;
    }

    fn selected_node(&self) -> Option<&TreeNode> {
        self.nodes.get(self.selected_idx)
    }

    fn checked_count(&self) -> usize {
        self.nodes.iter().filter(|n| n.checked).count()
    }
}

/// Group a flat `Vec<Change>` into tree nodes, one per unique
/// destination subdir (`_schema` for non-table-scoped DDL, `<table>` or
/// `<schema>__<table>` otherwise). Insertion order is preserved so the
/// topological sort from `compute_changes` survives into the apply path.
fn group_changes(changes: Vec<Change>) -> Vec<TreeNode> {
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

/// Concatenate the SQL of every checked node into a single blob suitable
/// for `db::execute_ddl()`. `_schema` always sorts first so enums and
/// schemas exist before tables that reference them; the remaining
/// per-table nodes keep their original (topo-sorted) order.
fn collect_apply_sql(nodes: &[TreeNode]) -> String {
    let mut ordered: Vec<&TreeNode> = nodes.iter().filter(|n| n.checked).collect();
    ordered.sort_by_key(|n| if n.name == "_schema" { 0 } else { 1 });
    let mut parts: Vec<String> = Vec::new();
    for node in ordered {
        for change in &node.changes {
            parts.push(change.sql.clone());
        }
    }
    parts.join("\n\n")
}

/// Render the SQL of a single tree node for the detail pane. Stable
/// across re-renders (does not depend on terminal width).
fn node_detail_text(node: &TreeNode) -> String {
    node.changes
        .iter()
        .map(|c| c.sql.as_str())
        .collect::<Vec<_>>()
        .join("\n\n")
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

pub(crate) async fn run(cli: Cli) -> Result<()> {
    let mut terminal = ratatui::init();
    let mut app = App::new(&cli);
    let result = event_loop(&mut terminal, &mut app).await;
    ratatui::restore();
    result
}

async fn event_loop(
    terminal: &mut ratatui::DefaultTerminal,
    app: &mut App,
) -> Result<()> {
    loop {
        terminal.draw(|f| render(f, app))?;

        match &app.state {
            AppState::Generating => {
                // Run introspection + DDL generation (blocks, but that's fine for a simple TUI)
                let result = generate_ddl(app).await;
                match result {
                    Ok(changes) => {
                        app.empty_diff = changes.is_empty();
                        app.nodes = group_changes(changes);
                        app.selected_idx = 0;
                        app.scroll_offset = 0;
                        app.error_msg = None;
                        app.state = AppState::ViewDdl;
                    }
                    Err(e) => {
                        app.error_msg = Some(format!("Error: {e}"));
                        app.state = AppState::InputUrls;
                    }
                }
                continue; // re-render immediately
            }
            AppState::Applying => {
                let result = apply_ddl(app).await;
                match result {
                    Ok(results) => {
                        let failed = results.iter().find(|r| r.error.is_some());
                        if let Some(f) = failed {
                            let applied = results.iter().take_while(|r| r.error.is_none()).count();
                            app.error_msg = Some(format!(
                                "Failed on statement {}/{}:\n{}\n\nError: {}",
                                applied + 1,
                                results.len(),
                                f.sql,
                                f.error.as_ref().unwrap()
                            ));
                        } else {
                            app.success_msg = Some(format!(
                                "Successfully applied {} statement(s) to target database.",
                                results.len()
                            ));
                        }
                        app.apply_results = results;
                        app.state = AppState::Done;
                    }
                    Err(e) => {
                        app.error_msg = Some(format!("Connection error: {e}"));
                        app.state = AppState::Done;
                    }
                }
                continue;
            }
            _ => {}
        }

        // Poll for input with a short timeout to keep responsive
        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                // Global quit: Ctrl-C
                if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c')
                {
                    return Ok(());
                }

                match &app.state {
                    AppState::InputUrls => handle_input_keys(app, key.code),
                    AppState::ViewDdl => handle_view_keys(app, key.code),
                    AppState::Confirming => handle_confirm_keys(app, key.code),
                    AppState::Done => handle_done_keys(app, key.code),
                    _ => {}
                }

                // Check if user wants to quit
                if matches!(app.state, AppState::InputUrls)
                    && key.code == KeyCode::Esc
                    && app.error_msg.is_none()
                {
                    return Ok(());
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Key handlers
// ---------------------------------------------------------------------------

fn handle_input_keys(app: &mut App, key: KeyCode) {
    // Clear error on any keypress
    if app.error_msg.is_some() && key != KeyCode::Esc {
        app.error_msg = None;
    }

    match key {
        KeyCode::Tab | KeyCode::BackTab => {
            app.focused_field = 1 - app.focused_field;
        }
        KeyCode::Up => {
            app.focused_field = 0;
        }
        KeyCode::Down => {
            app.focused_field = 1;
        }
        KeyCode::Char(c) => {
            let pos = app.cursor();
            app.active_input_mut().insert(pos, c);
            app.set_cursor(pos + c.len_utf8());
        }
        KeyCode::Backspace => {
            let pos = app.cursor();
            if pos > 0 {
                let input = app.active_input_mut();
                let prev = input[..pos]
                    .char_indices()
                    .next_back()
                    .map(|(i, _)| i)
                    .unwrap_or(0);
                input.remove(prev);
                app.set_cursor(prev);
            }
        }
        KeyCode::Delete => {
            let pos = app.cursor();
            let len = app.active_input().len();
            if pos < len {
                app.active_input_mut().remove(pos);
            }
        }
        KeyCode::Left => {
            let pos = app.cursor();
            if pos > 0 {
                let new_pos = app.active_input()[..pos]
                    .char_indices()
                    .next_back()
                    .map(|(i, _)| i)
                    .unwrap_or(0);
                app.set_cursor(new_pos);
            }
        }
        KeyCode::Right => {
            let pos = app.cursor();
            let len = app.active_input().len();
            if pos < len {
                let new_pos = app.active_input()[pos..]
                    .char_indices()
                    .nth(1)
                    .map(|(i, _)| pos + i)
                    .unwrap_or(len);
                app.set_cursor(new_pos);
            }
        }
        KeyCode::Home => {
            app.set_cursor(0);
        }
        KeyCode::End => {
            let len = app.active_input().len();
            app.set_cursor(len);
        }
        KeyCode::Enter => {
            if app.source_url.trim().is_empty() {
                app.error_msg = Some("Source URL is required.".into());
                app.focused_field = 0;
            } else if app.target_url.trim().is_empty() {
                app.error_msg = Some("Target URL is required.".into());
                app.focused_field = 1;
            } else {
                app.status_msg = "Introspecting databases...".into();
                app.state = AppState::Generating;
            }
        }
        KeyCode::Esc => {
            // If there's an error showing, clear it; otherwise quit is handled in event_loop
            if app.error_msg.is_some() {
                app.error_msg = None;
            }
        }
        _ => {}
    }
}

fn handle_view_keys(app: &mut App, key: KeyCode) {
    // Detail-pane line count for scrolling. Empty diff has no nodes — scroll is a no-op.
    let detail_lines = app
        .selected_node()
        .map(|n| node_detail_text(n).lines().count() as u16)
        .unwrap_or(0);

    // When the tree has more than one node, Up/Down navigates the
    // tree; j/k still scrolls the detail pane. With a single node
    // (or none), Up/Down falls through to scroll so the keys feel
    // natural in the flat-view case the plan specifies.
    let multi = app.nodes.len() > 1;

    match key {
        KeyCode::Up if multi => {
            if app.selected_idx > 0 {
                app.selected_idx -= 1;
                app.scroll_offset = 0;
            }
        }
        KeyCode::Down if multi => {
            if app.selected_idx + 1 < app.nodes.len() {
                app.selected_idx += 1;
                app.scroll_offset = 0;
            }
        }
        KeyCode::Up | KeyCode::Char('k') => {
            app.scroll_offset = app.scroll_offset.saturating_sub(1);
        }
        KeyCode::Down | KeyCode::Char('j') => {
            app.scroll_offset = app.scroll_offset.saturating_add(1).min(detail_lines);
        }
        KeyCode::PageUp => {
            app.scroll_offset = app.scroll_offset.saturating_sub(20);
        }
        KeyCode::PageDown => {
            app.scroll_offset = app.scroll_offset.saturating_add(20).min(detail_lines);
        }
        KeyCode::Home => {
            app.scroll_offset = 0;
        }
        KeyCode::End => {
            app.scroll_offset = detail_lines;
        }
        KeyCode::Char(' ') => {
            if let Some(node) = app.nodes.get_mut(app.selected_idx) {
                node.checked = !node.checked;
            }
        }
        KeyCode::Char('A') => {
            // Toggle all: if any is checked, uncheck all; otherwise check all.
            let any_checked = app.nodes.iter().any(|n| n.checked);
            for n in app.nodes.iter_mut() {
                n.checked = !any_checked;
            }
        }
        KeyCode::Char('a') => {
            // checked_count > 0 isn't enough — a node can be checked
            // but contain only advisory comment SQL (SQLite ALTER
            // warnings, MSSQL drop-default notes). Apply must require
            // at least one executable statement, otherwise the user
            // sees "successfully applied 0 statement(s)" with no
            // actual change to the target.
            if !app.empty_diff
                && count_statements(&collect_apply_sql(&app.nodes)) > 0
            {
                app.state = AppState::Confirming;
            }
        }
        KeyCode::Char('q') | KeyCode::Esc => {
            app.state = AppState::InputUrls;
            app.error_msg = None;
            app.success_msg = None;
        }
        _ => {}
    }
}

fn handle_confirm_keys(app: &mut App, key: KeyCode) {
    match key {
        KeyCode::Char('y') | KeyCode::Char('Y') => {
            app.status_msg = "Applying DDL...".into();
            app.state = AppState::Applying;
        }
        KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => {
            app.state = AppState::ViewDdl;
        }
        _ => {}
    }
}

fn handle_done_keys(app: &mut App, key: KeyCode) {
    match key {
        KeyCode::Char('q') => {
            app.state = AppState::InputUrls;
            app.error_msg = None;
            app.success_msg = None;
        }
        KeyCode::Esc => {
            app.state = AppState::ViewDdl;
            app.error_msg = None;
            app.success_msg = None;
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

fn render(f: &mut Frame, app: &App) {
    match &app.state {
        AppState::InputUrls => render_input(f, app),
        AppState::Generating | AppState::Applying => render_status(f, app),
        AppState::ViewDdl => render_ddl_view(f, app),
        AppState::Confirming => {
            render_ddl_view(f, app);
            render_confirm_popup(f, app);
        }
        AppState::Done => render_done(f, app),
    }
}

fn render_input(f: &mut Frame, app: &App) {
    let area = f.area();
    let chunks = Layout::vertical([
        Constraint::Length(3), // title
        Constraint::Length(3), // source URL
        Constraint::Length(3), // target URL
        Constraint::Length(2), // help
        Constraint::Min(0),   // spacer
        Constraint::Length(2), // error
    ])
    .split(area);

    // Title
    let title = Paragraph::new(Line::from(vec![
        Span::styled("uvg", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
        Span::raw(" -- DDL Diff & Apply"),
    ]))
    .block(Block::default().borders(Borders::BOTTOM));
    f.render_widget(title, chunks[0]);

    // Source URL
    let source_style = if app.focused_field == 0 {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default()
    };
    let source = Paragraph::new(app.source_url.as_str())
        .block(
            Block::default()
                .title(" Source URL ")
                .borders(Borders::ALL)
                .border_style(source_style),
        );
    f.render_widget(source, chunks[1]);

    // Target URL
    let target_style = if app.focused_field == 1 {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default()
    };
    let target = Paragraph::new(app.target_url.as_str())
        .block(
            Block::default()
                .title(" Target URL ")
                .borders(Borders::ALL)
                .border_style(target_style),
        );
    f.render_widget(target, chunks[2]);

    // Place cursor
    let field_chunk = if app.focused_field == 0 {
        chunks[1]
    } else {
        chunks[2]
    };
    let cursor_x = field_chunk.x + 1 + app.cursor() as u16;
    let cursor_y = field_chunk.y + 1;
    f.set_cursor_position((cursor_x, cursor_y));

    // Help text
    let help = Paragraph::new(Line::from(vec![
        Span::styled("Tab", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(" switch field  "),
        Span::styled("Enter", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(" generate  "),
        Span::styled("Esc", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(" quit"),
    ]))
    .style(Style::default().fg(Color::DarkGray));
    f.render_widget(help, chunks[3]);

    // Error message
    if let Some(ref err) = app.error_msg {
        let error = Paragraph::new(err.as_str())
            .style(Style::default().fg(Color::Red));
        f.render_widget(error, chunks[5]);
    }
}

fn render_status(f: &mut Frame, app: &App) {
    let area = f.area();
    let msg = Paragraph::new(app.status_msg.as_str())
        .style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))
        .block(Block::default().borders(Borders::ALL).title(" Working "));
    let centered = centered_rect(50, 5, area);
    f.render_widget(Clear, centered);
    f.render_widget(msg, centered);
}

fn render_ddl_view(f: &mut Frame, app: &App) {
    let area = f.area();
    let chunks = Layout::vertical([
        Constraint::Min(1),    // content (tree + detail, or flat)
        Constraint::Length(2), // footer
    ])
    .split(area);

    let title = format!(" DDL Diff: {} -> {} ", app.source_url, app.target_url);

    if app.empty_diff {
        // Empty-diff path keeps the legacy flat view so users who script
        // around "No schema changes detected" still get that string.
        let body = Paragraph::new("-- No schema changes detected.")
            .block(Block::default().title(title).borders(Borders::ALL))
            .wrap(Wrap { trim: false });
        f.render_widget(body, chunks[0]);
    } else if app.nodes.len() <= 1 {
        // Single-node case: tree adds no value, render the SQL flat.
        let detail = app
            .selected_node()
            .map(node_detail_text)
            .unwrap_or_default();
        let body = Paragraph::new(detail)
            .block(Block::default().title(title).borders(Borders::ALL))
            .scroll((app.scroll_offset, 0))
            .wrap(Wrap { trim: false });
        f.render_widget(body, chunks[0]);
    } else {
        // Two-pane: tree on the left, SQL detail on the right.
        let panes = Layout::horizontal([
            Constraint::Percentage(30),
            Constraint::Percentage(70),
        ])
        .split(chunks[0]);

        let items: Vec<ListItem> = app
            .nodes
            .iter()
            .map(|n| {
                let check = if n.checked { "[x]" } else { "[ ]" };
                let stmt_count = n.changes.len();
                let label = format!("{check} {} ({stmt_count})", n.name);
                ListItem::new(label)
            })
            .collect();
        let list = List::new(items)
            .block(
                Block::default()
                    .title(" Tables ")
                    .borders(Borders::ALL),
            )
            .highlight_style(
                Style::default()
                    .bg(Color::DarkGray)
                    .add_modifier(Modifier::BOLD),
            )
            .highlight_symbol("▶ ");
        let mut list_state = ListState::default();
        list_state.select(Some(app.selected_idx));
        f.render_stateful_widget(list, panes[0], &mut list_state);

        let detail = app
            .selected_node()
            .map(node_detail_text)
            .unwrap_or_default();
        let detail_title = app
            .selected_node()
            .map(|n| format!(" {} ", n.name))
            .unwrap_or_else(|| " (none) ".to_string());
        let body = Paragraph::new(detail)
            .block(Block::default().title(detail_title).borders(Borders::ALL))
            .scroll((app.scroll_offset, 0))
            .wrap(Wrap { trim: false });
        f.render_widget(body, panes[1]);
    }

    // Footer: status + hotkeys.
    let footer = render_view_footer(app);
    f.render_widget(footer, chunks[1]);
}

fn render_view_footer(app: &App) -> Paragraph<'static> {
    let multi = app.nodes.len() > 1;
    let mut spans: Vec<Span<'static>> = Vec::new();

    if app.empty_diff || app.nodes.is_empty() {
        spans.push(Span::styled("q/Esc", Style::default().add_modifier(Modifier::BOLD)));
        spans.push(Span::raw(" back"));
        return Paragraph::new(Line::from(spans)).style(Style::default().fg(Color::DarkGray));
    }

    // "N table(s) selected · M statement(s)"
    let stmt_count = count_statements(&collect_apply_sql(&app.nodes));
    let summary = format!(
        "{}/{} table(s) selected · {} statement(s)  ",
        app.checked_count(),
        app.nodes.len(),
        stmt_count,
    );
    spans.push(Span::styled(
        summary,
        Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
    ));

    if multi {
        spans.push(Span::styled("Up/Dn", Style::default().add_modifier(Modifier::BOLD)));
        spans.push(Span::raw(" nav  "));
        spans.push(Span::styled("j/k", Style::default().add_modifier(Modifier::BOLD)));
        spans.push(Span::raw(" scroll  "));
        spans.push(Span::styled("Space", Style::default().add_modifier(Modifier::BOLD)));
        spans.push(Span::raw(" toggle  "));
        spans.push(Span::styled("A", Style::default().add_modifier(Modifier::BOLD)));
        spans.push(Span::raw(" all  "));
    } else {
        spans.push(Span::styled("Up/Dn", Style::default().add_modifier(Modifier::BOLD)));
        spans.push(Span::raw(" scroll  "));
    }

    // Same guard the 'a' key handler uses: don't suggest apply if the
    // checked SQL is only comments (e.g. SQLite ALTER warnings).
    if stmt_count > 0 {
        spans.push(Span::styled("a", Style::default().add_modifier(Modifier::BOLD)));
        spans.push(Span::raw(" apply  "));
    }
    spans.push(Span::styled("q/Esc", Style::default().add_modifier(Modifier::BOLD)));
    spans.push(Span::raw(" back"));

    Paragraph::new(Line::from(spans)).style(Style::default().fg(Color::DarkGray))
}

fn render_confirm_popup(f: &mut Frame, app: &App) {
    let area = centered_rect(60, 7, f.area());
    f.render_widget(Clear, area);
    let stmt_count = count_statements(&collect_apply_sql(&app.nodes));
    let table_count = app.checked_count();
    let msg = Paragraph::new(vec![
        Line::from(""),
        Line::from(format!(
            "  Apply {stmt_count} statement(s) across {table_count} table(s) to the target?",
        )),
        Line::from(""),
        Line::from(vec![
            Span::raw("  "),
            Span::styled("y", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
            Span::raw(" yes  "),
            Span::styled("n", Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)),
            Span::raw(" no"),
        ]),
    ])
    .block(
        Block::default()
            .title(" Confirm Apply ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Yellow)),
    );
    f.render_widget(msg, area);
}

fn render_done(f: &mut Frame, app: &App) {
    let area = f.area();
    let chunks = Layout::vertical([
        Constraint::Min(1),
        Constraint::Length(2),
    ])
    .split(area);

    let (text, style) = if let Some(ref err) = app.error_msg {
        (err.as_str(), Style::default().fg(Color::Red))
    } else if let Some(ref msg) = app.success_msg {
        (msg.as_str(), Style::default().fg(Color::Green))
    } else {
        ("Done.", Style::default())
    };

    let content = Paragraph::new(text)
        .style(style)
        .block(Block::default().title(" Result ").borders(Borders::ALL))
        .wrap(Wrap { trim: false });
    f.render_widget(content, chunks[0]);

    let footer = Paragraph::new(Line::from(vec![
        Span::styled("q", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(" new diff  "),
        Span::styled("Esc", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(" back to DDL"),
    ]))
    .style(Style::default().fg(Color::DarkGray));
    f.render_widget(footer, chunks[1]);
}

fn centered_rect(percent_x: u16, height: u16, area: Rect) -> Rect {
    let vert = Layout::vertical([
        Constraint::Fill(1),
        Constraint::Length(height),
        Constraint::Fill(1),
    ])
    .split(area);
    Layout::horizontal([
        Constraint::Percentage((100 - percent_x) / 2),
        Constraint::Percentage(percent_x),
        Constraint::Percentage((100 - percent_x) / 2),
    ])
    .split(vert[1])[1]
}

// ---------------------------------------------------------------------------
// DDL generation & apply
// ---------------------------------------------------------------------------

async fn generate_ddl(app: &mut App) -> Result<Vec<Change>> {
    let source_url = app.source_url.trim().to_string();
    let target_url = app.target_url.trim().to_string();

    // Parse connection configs using a helper Cli
    let source_cli = make_cli(&source_url, app.trust_cert);
    let source_config = source_cli.parse_connection()?;
    let source_dialect = source_config.dialect();

    let target_cli = make_cli(&target_url, app.trust_cert);
    let target_config = target_cli.parse_connection()?;
    let target_dialect = target_config.dialect();

    // Introspect source
    let source_schemas = if let Some(db) = source_config.database_name() {
        vec![db]
    } else {
        vec![source_dialect.default_schema().to_string()]
    };
    let options = crate::cli::GeneratorOptions::default();
    let source_schema =
        db::introspect_with_config(
            source_config,
            &source_schemas,
            &crate::table_filter::TableFilter::allow_all(),
            false,
            &options,
        )
        .await?;

    // Introspect target
    let target_schemas = if let Some(db) = target_config.database_name() {
        vec![db]
    } else {
        vec![target_dialect.default_schema().to_string()]
    };

    let target_schema_data =
        db::introspect_with_config(
            make_cli(&target_url, app.trust_cert).parse_connection()?,
            &target_schemas,
            &crate::table_filter::TableFilter::allow_all(),
            false,
            &options,
        )
        .await?;

    let ddl_opts = DdlOptions {
        target_dialect,
        split_tables: false,
        apply: false,
        noindexes: false,
        noconstraints: false,
        nocomments: false,
    };

    Ok(compute_changes(&source_schema, &target_schema_data, &ddl_opts))
}

async fn apply_ddl(app: &mut App) -> Result<Vec<db::StmtResult>> {
    let target_url = app.target_url.trim().to_string();
    let config = make_cli(&target_url, app.trust_cert).parse_connection()?;
    let sql = collect_apply_sql(&app.nodes);
    db::execute_ddl(&config, &sql).await
}

fn make_cli(url: &str, trust_cert: bool) -> Cli {
    Cli {
        url: url.to_string(),
        target_url: None,
        generator: "ddl".to_string(),
        target_dialect: None,
        split_tables: false,
        apply: false,
        tables: None,
        exclude_tables: None,
        schemas: None,
        noviews: false,
        options: None,
        outfile: None,
        out_dir: None,
        name: None,
        trust_cert,
        interactive: false,
    }
}

fn count_statements(ddl: &str) -> usize {
    db::split_statements(ddl).len()
}

// ---------------------------------------------------------------------------
// Tests for the pure data transformations.
// (The terminal-rendering paths aren't unit-testable without a ratatui
// test harness; we cover them via manual smoke testing.)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn ch(schema: &str, table: Option<&str>, sql: &str) -> Change {
        Change {
            table_schema: schema.to_string(),
            table_name: table.map(|s| s.to_string()),
            sql: sql.to_string(),
        }
    }

    #[test]
    fn test_group_changes_one_node_per_table_preserves_order() {
        let changes = vec![
            ch("", Some("users"), "CREATE TABLE users();"),
            ch("", Some("posts"), "CREATE TABLE posts();"),
            ch("", Some("users"), "CREATE INDEX ix ON users(email);"),
        ];
        let nodes = group_changes(changes);
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].name, "users");
        assert_eq!(nodes[1].name, "posts");
        // Two changes attributed to users/, in insertion order.
        assert_eq!(nodes[0].changes.len(), 2);
        assert!(nodes[0].changes[0].sql.contains("CREATE TABLE"));
        assert!(nodes[0].changes[1].sql.contains("CREATE INDEX"));
        // All nodes default to checked.
        assert!(nodes.iter().all(|n| n.checked));
    }

    #[test]
    fn test_group_changes_schema_scoped_lands_in_underscore_schema() {
        let changes = vec![
            ch("", None, "CREATE TYPE status AS ENUM ('a');"),
            ch("", Some("users"), "CREATE TABLE users();"),
        ];
        let nodes = group_changes(changes);
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].name, "_schema");
        assert_eq!(nodes[1].name, "users");
    }

    #[test]
    fn test_group_changes_non_default_schema_uses_double_underscore() {
        let changes = vec![ch("billing", Some("orders"), "CREATE TABLE \"billing\".\"orders\"();")];
        let nodes = group_changes(changes);
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].name, "billing__orders");
    }

    #[test]
    fn test_collect_apply_sql_schema_first_then_tables() {
        // Insertion order: users, _schema, posts. _schema must still
        // come out first so enums/types exist before referencing tables.
        let nodes = vec![
            TreeNode {
                name: "users".into(),
                changes: vec![ch("", Some("users"), "ALTER TABLE users ADD COLUMN x int;")],
                checked: true,
            },
            TreeNode {
                name: "_schema".into(),
                changes: vec![ch("", None, "CREATE TYPE color AS ENUM ('r','g','b');")],
                checked: true,
            },
            TreeNode {
                name: "posts".into(),
                changes: vec![ch("", Some("posts"), "ALTER TABLE posts ADD COLUMN y int;")],
                checked: true,
            },
        ];
        let sql = collect_apply_sql(&nodes);
        let schema_pos = sql.find("CREATE TYPE").unwrap();
        let users_pos = sql.find("ALTER TABLE users").unwrap();
        let posts_pos = sql.find("ALTER TABLE posts").unwrap();
        assert!(schema_pos < users_pos, "_schema must precede users: {sql}");
        assert!(
            users_pos < posts_pos,
            "table order from compute_changes must be preserved among non-schema nodes: {sql}"
        );
    }

    #[test]
    fn test_collect_apply_sql_unchecked_nodes_excluded() {
        let nodes = vec![
            TreeNode {
                name: "users".into(),
                changes: vec![ch("", Some("users"), "ALTER TABLE users ...;")],
                checked: true,
            },
            TreeNode {
                name: "posts".into(),
                changes: vec![ch("", Some("posts"), "ALTER TABLE posts ...;")],
                checked: false,
            },
        ];
        let sql = collect_apply_sql(&nodes);
        assert!(sql.contains("ALTER TABLE users"));
        assert!(
            !sql.contains("ALTER TABLE posts"),
            "unchecked node must not contribute SQL"
        );
    }

    #[test]
    fn test_collect_apply_sql_no_checked_returns_empty() {
        let nodes = vec![TreeNode {
            name: "users".into(),
            changes: vec![ch("", Some("users"), "ALTER TABLE users ...;")],
            checked: false,
        }];
        assert_eq!(collect_apply_sql(&nodes), "");
    }

    #[test]
    fn test_comment_only_changes_have_zero_executable_statements() {
        // Regression: codex round 5 caught that checked_count() > 0 is
        // not a sufficient apply gate — SQLite ALTER warnings and
        // MSSQL default-drop notes are comment-only, so they
        // contribute non-empty SQL to collect_apply_sql() but split to
        // zero executable statements. The apply gate now uses
        // count_statements(collect_apply_sql(...)) instead. This test
        // pins the property the gate depends on: a node holding only
        // comment SQL must split to 0 statements.
        let nodes = vec![TreeNode {
            name: "users".into(),
            changes: vec![ch(
                "",
                Some("users"),
                "-- WARNING: SQLite does not support ALTER COLUMN. Table recreation required.\n\
                 -- ALTER TABLE \"users\" ALTER COLUMN \"email\" TYPE VARCHAR(255);",
            )],
            checked: true,
        }];
        let sql = collect_apply_sql(&nodes);
        assert!(!sql.is_empty(), "comment SQL is non-empty as text");
        assert_eq!(
            count_statements(&sql),
            0,
            "but it must split into zero executable statements; sql was: {sql}"
        );
    }
}
