use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph, Wrap};
use ratatui::Frame;

use super::app::{node_detail_text, App, AppState};
use crate::redaction::redact_connection_url;

pub(super) fn render(f: &mut Frame, app: &App) {
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
        Constraint::Min(0),    // spacer
        Constraint::Length(2), // error
    ])
    .split(area);

    // Title
    let title = Paragraph::new(Line::from(vec![
        Span::styled(
            "uvg",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
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
    let source = Paragraph::new(app.source_url.as_str()).block(
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
    let target = Paragraph::new(app.target_url.as_str()).block(
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
        let error = Paragraph::new(err.as_str()).style(Style::default().fg(Color::Red));
        f.render_widget(error, chunks[5]);
    }
}

fn render_status(f: &mut Frame, app: &App) {
    let area = f.area();
    let msg = Paragraph::new(app.status_msg.as_str())
        .style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )
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

    let title = format!(
        " DDL Diff: {} -> {} ",
        redact_connection_url(&app.source_url),
        redact_connection_url(&app.target_url)
    );

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
        let panes = Layout::horizontal([Constraint::Percentage(30), Constraint::Percentage(70)])
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
            .block(Block::default().title(" Tables ").borders(Borders::ALL))
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
        spans.push(Span::styled(
            "q/Esc",
            Style::default().add_modifier(Modifier::BOLD),
        ));
        spans.push(Span::raw(" back"));
        return Paragraph::new(Line::from(spans)).style(Style::default().fg(Color::DarkGray));
    }

    // "N table(s) selected · M statement(s)"
    let stmt_count = app.executable_statement_count();
    let summary = format!(
        "{}/{} table(s) selected · {} statement(s)  ",
        app.checked_count(),
        app.nodes.len(),
        stmt_count,
    );
    spans.push(Span::styled(
        summary,
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    ));

    if multi {
        spans.push(Span::styled(
            "Up/Dn",
            Style::default().add_modifier(Modifier::BOLD),
        ));
        spans.push(Span::raw(" nav  "));
        spans.push(Span::styled(
            "j/k",
            Style::default().add_modifier(Modifier::BOLD),
        ));
        spans.push(Span::raw(" scroll  "));
        spans.push(Span::styled(
            "Space",
            Style::default().add_modifier(Modifier::BOLD),
        ));
        spans.push(Span::raw(" toggle  "));
        spans.push(Span::styled(
            "A",
            Style::default().add_modifier(Modifier::BOLD),
        ));
        spans.push(Span::raw(" all  "));
    } else {
        spans.push(Span::styled(
            "Up/Dn",
            Style::default().add_modifier(Modifier::BOLD),
        ));
        spans.push(Span::raw(" scroll  "));
    }

    // Same guard the 'a' key handler uses: don't suggest apply if the
    // checked SQL is only comments (e.g. SQLite ALTER warnings).
    if stmt_count > 0 {
        spans.push(Span::styled(
            "a",
            Style::default().add_modifier(Modifier::BOLD),
        ));
        spans.push(Span::raw(" apply  "));
    }
    spans.push(Span::styled(
        "q/Esc",
        Style::default().add_modifier(Modifier::BOLD),
    ));
    spans.push(Span::raw(" back"));

    Paragraph::new(Line::from(spans)).style(Style::default().fg(Color::DarkGray))
}

fn render_confirm_popup(f: &mut Frame, app: &App) {
    let area = centered_rect(60, 7, f.area());
    f.render_widget(Clear, area);
    let stmt_count = app.executable_statement_count();
    let table_count = app.checked_count();
    let msg = Paragraph::new(vec![
        Line::from(""),
        Line::from(format!(
            "  Apply {stmt_count} statement(s) across {table_count} table(s) to the target?",
        )),
        Line::from(""),
        Line::from(vec![
            Span::raw("  "),
            Span::styled(
                "y",
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(" yes  "),
            Span::styled(
                "n",
                Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
            ),
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
    let chunks = Layout::vertical([Constraint::Min(1), Constraint::Length(2)]).split(area);

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
