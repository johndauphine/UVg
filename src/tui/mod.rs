use std::time::Duration;

use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use ratatui::Frame;

use crate::cli::{Cli, DdlOptions};
use crate::codegen::ddl::{DdlGenerator, DdlOutput};
use crate::db;

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

struct App {
    state: AppState,
    source_url: String,
    target_url: String,
    focused_field: usize, // 0 = source, 1 = target
    cursor_pos: [usize; 2],
    ddl_output: String,
    scroll_offset: u16,
    status_msg: String,
    error_msg: Option<String>,
    success_msg: Option<String>,
    apply_results: Vec<db::StmtResult>,
    trust_cert: bool,
    stmt_count: usize,
}

impl App {
    fn new(cli: &Cli) -> Self {
        Self {
            state: AppState::InputUrls,
            source_url: cli.url.clone(),
            target_url: cli.target_url.clone().unwrap_or_default(),
            focused_field: if cli.url.is_empty() { 0 } else { 1 },
            cursor_pos: [cli.url.len(), cli.target_url.as_ref().map_or(0, |u| u.len())],
            ddl_output: String::new(),
            scroll_offset: 0,
            status_msg: String::new(),
            error_msg: None,
            success_msg: None,
            apply_results: Vec::new(),
            trust_cert: cli.trust_cert,
            stmt_count: 0,
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
                let result = generate_ddl(app).await;
                match result {
                    Ok(ddl) => {
                        app.ddl_output = ddl;
                        app.stmt_count = db::count_statements(&app.ddl_output);
                        app.scroll_offset = 0;
                        app.error_msg = None;
                        app.state = AppState::ViewDdl;
                    }
                    Err(e) => {
                        app.error_msg = Some(format!("Error: {e}"));
                        app.state = AppState::InputUrls;
                    }
                }
                continue;
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

        // Poll for input on a blocking thread to avoid starving the tokio runtime
        if let Some(Event::Key(key)) = tokio::task::spawn_blocking(|| -> std::io::Result<Option<Event>> {
            if event::poll(Duration::from_millis(100))? {
                Ok(Some(event::read()?))
            } else {
                Ok(None)
            }
        })
        .await?? {
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
    let line_count = app.ddl_output.lines().count() as u16;
    match key {
        KeyCode::Up | KeyCode::Char('k') => {
            app.scroll_offset = app.scroll_offset.saturating_sub(1);
        }
        KeyCode::Down | KeyCode::Char('j') => {
            app.scroll_offset = app.scroll_offset.saturating_add(1).min(line_count);
        }
        KeyCode::PageUp => {
            app.scroll_offset = app.scroll_offset.saturating_sub(20);
        }
        KeyCode::PageDown => {
            app.scroll_offset = app.scroll_offset.saturating_add(20).min(line_count);
        }
        KeyCode::Home => {
            app.scroll_offset = 0;
        }
        KeyCode::End => {
            app.scroll_offset = line_count;
        }
        KeyCode::Char('a') => {
            if app.stmt_count > 0 && !app.ddl_output.contains("No schema changes detected") {
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

    // Place cursor (clamped to field bounds)
    let field_chunk = if app.focused_field == 0 {
        chunks[1]
    } else {
        chunks[2]
    };
    let inner_x = field_chunk.x.saturating_add(1);
    let inner_width = field_chunk.width.saturating_sub(2);
    let cursor_offset = u16::try_from(app.cursor()).unwrap_or(u16::MAX);
    let cursor_x = if inner_width == 0 {
        field_chunk.x
    } else {
        inner_x + cursor_offset.min(inner_width.saturating_sub(1))
    };
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
        Constraint::Min(1),   // DDL content
        Constraint::Length(2), // footer
    ])
    .split(area);

    let ddl = Paragraph::new(app.ddl_output.as_str())
        .block(
            Block::default()
                .title(format!(
                    " DDL Diff: {} -> {} ",
                    mask_url_password(&app.source_url),
                    mask_url_password(&app.target_url),
                ))
                .borders(Borders::ALL),
        )
        .scroll((app.scroll_offset, 0))
        .wrap(Wrap { trim: false });
    f.render_widget(ddl, chunks[0]);

    let no_changes = app.ddl_output.contains("No schema changes detected");
    let mut help_spans = vec![
        Span::styled("Up/Down", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(" scroll  "),
    ];
    if !no_changes && app.stmt_count > 0 {
        help_spans.push(Span::styled("a", Style::default().add_modifier(Modifier::BOLD)));
        help_spans.push(Span::raw(format!(" apply ({} stmts)  ", app.stmt_count)));
    }
    help_spans.push(Span::styled("q/Esc", Style::default().add_modifier(Modifier::BOLD)));
    help_spans.push(Span::raw(" back"));

    let footer = Paragraph::new(Line::from(help_spans))
        .style(Style::default().fg(Color::DarkGray));
    f.render_widget(footer, chunks[1]);
}

fn render_confirm_popup(f: &mut Frame, app: &App) {
    let area = centered_rect(60, 7, f.area());
    f.render_widget(Clear, area);
    let msg = Paragraph::new(vec![
        Line::from(""),
        Line::from(format!(
            "  Apply {} statement(s) to the target database?",
            app.stmt_count
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

async fn generate_ddl(app: &mut App) -> Result<String> {
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
        db::introspect_with_config(source_config, &source_schemas, &[], false, &options).await?;

    // Introspect target (re-parse config since introspect consumes it)
    let target_schemas = if let Some(db) = target_config.database_name() {
        vec![db]
    } else {
        vec![target_dialect.default_schema().to_string()]
    };
    let target_schema_data =
        db::introspect_with_config(target_config, &target_schemas, &[], false, &options).await?;

    // Generate DDL diff
    let ddl_opts = DdlOptions {
        target_dialect,
        split_tables: false,
        apply: false,
        noindexes: false,
        noconstraints: false,
        nocomments: false,
    };

    let gen = DdlGenerator;
    let output = gen.generate(&source_schema, Some(&target_schema_data), &ddl_opts);

    match output {
        DdlOutput::Single(content) => Ok(content),
        DdlOutput::Split(files) => {
            Ok(files
                .into_iter()
                .map(|(name, content)| format!("-- File: {name}\n{content}"))
                .collect::<Vec<_>>()
                .join("\n\n"))
        }
    }
}

async fn apply_ddl(app: &mut App) -> Result<Vec<db::StmtResult>> {
    let target_url = app.target_url.trim().to_string();
    let config = make_cli(&target_url, app.trust_cert).parse_connection()?;
    db::execute_ddl(&config, &app.ddl_output).await
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
        schemas: None,
        noviews: false,
        options: None,
        outfile: None,
        trust_cert,
        interactive: false,
    }
}

/// Mask the password portion of a database URL for display.
/// Replaces `user:password@` with `user:***@`.
fn mask_url_password(url: &str) -> String {
    // Find the scheme separator
    let after_scheme = match url.find("://") {
        Some(i) => i + 3,
        None => return url.to_string(),
    };

    let rest = &url[after_scheme..];

    // Find the @ separator (credentials end here)
    let at_pos = match rest.find('@') {
        Some(i) => i,
        None => return url.to_string(), // no credentials
    };

    let credentials = &rest[..at_pos];

    // Find the colon separating user from password
    match credentials.find(':') {
        Some(colon) => {
            let user = &credentials[..colon];
            format!("{}://{}:***@{}", &url[..after_scheme - 3], user, &rest[at_pos + 1..])
        }
        None => url.to_string(), // no password
    }
}
