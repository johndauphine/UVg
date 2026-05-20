use std::time::Duration;

use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyModifiers};

use super::actions::{apply_ddl, generate_ddl};
use super::app::{group_changes, node_detail_line_count, App, AppState};
use super::render::render;

pub(super) async fn event_loop(
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
                        app.set_nodes(group_changes(changes));
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
                if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
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
        KeyCode::Esc if app.error_msg.is_some() => {
            // If there's an error showing, clear it; otherwise quit is handled in event_loop
            app.error_msg = None;
        }
        _ => {}
    }
}

pub(super) fn handle_view_keys(app: &mut App, key: KeyCode) {
    // Detail-pane line count for scrolling. Empty diff has no nodes - scroll is a no-op.
    let detail_lines = app.selected_node().map(node_detail_line_count).unwrap_or(0);

    // When the tree has more than one node, Up/Down navigates the
    // tree; j/k still scrolls the detail pane. With a single node
    // (or none), Up/Down falls through to scroll so the keys feel
    // natural in the flat-view case the plan specifies.
    let multi = app.nodes.len() > 1;

    match key {
        KeyCode::Up if multi && app.selected_idx > 0 => {
            app.selected_idx -= 1;
            app.scroll_offset = 0;
        }
        KeyCode::Up if multi => {}
        KeyCode::Down if multi && app.selected_idx + 1 < app.nodes.len() => {
            app.selected_idx += 1;
            app.scroll_offset = 0;
        }
        KeyCode::Down if multi => {}
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
            app.toggle_selected_node();
        }
        KeyCode::Char('A') => {
            // Toggle all: if any is checked, uncheck all; otherwise check all.
            app.toggle_all_nodes();
        }
        KeyCode::Char('a') if !app.empty_diff && app.executable_statement_count() > 0 => {
            // checked_count > 0 isn't enough - a node can be checked
            // but contain only advisory comment SQL (SQLite ALTER
            // warnings, MSSQL drop-default notes). Apply must require
            // at least one executable statement, otherwise the user
            // sees "successfully applied 0 statement(s)" with no
            // actual change to the target.
            app.state = AppState::Confirming;
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
