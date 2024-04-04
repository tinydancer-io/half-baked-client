use crate::sampler::GetShredResponse;
use crate::tinydancer::{ClientService, ClientStatus, TinyDancer};
use async_trait::async_trait;
use crossterm::event::{KeyEvent, KeyEventKind, KeyEventState, KeyModifiers};
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use spinoff::{spinners, Color as SpinColor, Spinner};
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;
use std::{any::Any, thread::Thread};
use std::{fmt, mem::drop, thread::JoinHandle};
use thiserror::Error;
use tiny_logger::logs::info;
use tui::layout::Rect;
use tui::style::{Color, Modifier, Style};
use tui::text::{Span, Spans};
use tui::{
    backend::{Backend, CrosstermBackend},
    layout::Direction,
    layout::{Constraint, Layout},
    widgets::{
        Axis, BarChart, Block, Borders, Cell, Chart, Dataset, Gauge, LineGauge, List, ListItem,
        ListState, Paragraph, Row, Sparkline, Table, TableState, Tabs, Wrap,
    },
    Frame, Terminal,
};

pub struct UiService {
    //pub views: Vec<String>, //placeholder
    pub ui_service_handle: JoinHandle<()>, // pub table: TableState,  // placeholder view
}

pub struct App {
    title: String,
    tabs: TabsState,
    table: TableState,
    slot_list: StatefulList<(String, usize)>,
    peers_list: StatefulList<Vec<String>>,
    full_nodes_list: StatefulList<Vec<String>>,
}

// pub struct SlotList {
//     title: String,
//     state: ListState,
//     items: Vec<Vec<(usize, String)>>,
// }
pub struct StatefulList<T> {
    state: ListState,
    items: Vec<T>,
}
impl<T> StatefulList<T> {
    fn with_items(items: Vec<T>) -> StatefulList<T> {
        StatefulList {
            state: ListState::default(),
            items,
        }
    }

    fn next(&mut self) {
        let _i = match self.state.selected() {
            Some(i) => {
                if i >= self.items.len() - 1 {
                    0
                } else {
                    i + 1
                }
            }
            None => 0,
        };
    }
    fn previous(&mut self) {
        let i = match self.state.selected() {
            Some(i) => {
                if i == 0 {
                    self.items.len() - 1
                } else {
                    i - 1
                }
            }
            None => 0,
        };
        self.state.select(Some(i));
    }
    fn unselect(&mut self) {
        self.state.select(None);
    }
}

pub struct TabsState {
    pub titles: Vec<String>,
    pub index: usize,
}
impl TabsState {
    pub fn new(titles: Vec<String>) -> TabsState {
        TabsState { titles, index: 0 }
    }
    pub fn next(&mut self) {
        self.index = (self.index + 1) % self.titles.len();
    }

    pub fn previous(&mut self) {
        if self.index > 0 {
            self.index -= 1;
        } else {
            self.index = self.titles.len() - 1;
        }
    }
}
pub struct UiConfig {
    pub client_status: Arc<Mutex<ClientStatus>>,
    pub enable_ui_service: bool,
    pub tui_monitor: bool,
}
// main draw function
pub fn draw<B: Backend>(f: &mut Frame<B>) {
    let _chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(
            [
                Constraint::Percentage(20),
                Constraint::Percentage(40),
                Constraint::Percentage(40),
            ]
            .as_ref(),
        )
        .split(f.size());
}

fn draw_first_tab<B>(f: &mut Frame<B>, app: &mut App, area: Rect)
where
    B: Backend,
{
    let chunks = Layout::default()
        .constraints(
            [
                Constraint::Length(20),
                Constraint::Length(3),
                Constraint::Length(2),
            ]
            .as_ref(),
        )
        .split(area);

    //ui(f, app, chunks[0]);
    draw_slot_list(f, app, chunks[0]);
}
fn draw_second_tab<B>(_f: &mut Frame<B>, _app: &mut App, area: Rect)
where
    B: Backend,
{
    let _chunks = Layout::default()
        .constraints(
            [
                Constraint::Length(20),
                Constraint::Length(3),
                Constraint::Length(2),
            ]
            .as_ref(),
        )
        .split(area);
    //todo: Draw network usage metrics here
    todo!()
}

// draws list of slots
fn draw_slot_list<B: Backend>(f: &mut Frame<B>, app: &mut App, area: Rect) {
    // let selected_style = Style::default().add_modifier(Modifier::REVERSED);
    // let normal_style = Style::default().bg(Color::LightBlue);
    let items: Vec<ListItem> = app
        .slot_list
        .items
        .iter()
        .map(|i| {
            let mut lines = vec![Spans::from(i.0.clone())];
            for _ in 0..i.1 {
                lines.push(Spans::from(Span::styled(
                    // dummy values in list that would be replaced by slot numbers
                    "slots 1",
                    Style::default().add_modifier(Modifier::ITALIC),
                )));
            }
            ListItem::new(lines).style(Style::default().fg(Color::Black).bg(Color::White))
        })
        .collect();
    let list = List::new(items)
        .block(Block::default().borders(Borders::ALL).title("SLOTS"))
        .highlight_style(
            Style::default()
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol(">>");
    f.render_stateful_widget(list, area, &mut app.slot_list.state);
    // let peers_list: Vec<ListItem> = app
    //     .peers_list
    //     .items
    //     .iter()
    //     .map(|i|{

    //     })
}

#[async_trait]
impl ClientService<UiConfig> for UiService {
    type ServiceError = ThreadJoinError;
    fn new(config: UiConfig) -> Self {
        let ui_service_handle = std::thread::spawn(move || loop {
            let mut threads = Vec::default();

            if config.enable_ui_service {
                threads.push(std::thread::spawn(|| {
                    info!("rendering ui");
                    std::thread::sleep(std::time::Duration::from_secs(2));
                }));
            }

            if config.tui_monitor {
                let client_status = config.client_status.clone();
                let mut spinner =
                    Spinner::new(spinners::Dots, "Initializing Client...", SpinColor::Yellow);

                threads.push(std::thread::spawn(move || loop {
                    sleep(Duration::from_millis(100));

                    let status = client_status.lock().unwrap();
                    match &*status {
                        ClientStatus::Active(msg) => {
                            spinner.update(spinners::Dots, msg.clone(), SpinColor::Green);
                            // sleep(Duration::from_secs(100));
                        }
                        ClientStatus::Initializing(msg) => {
                            spinner.update(spinners::Dots, msg.clone(), SpinColor::Yellow);
                        }
                        ClientStatus::Crashed(msg) => {
                            spinner.update(spinners::Dots, msg.clone(), SpinColor::Red);
                        }
                        ClientStatus::ShuttingDown(msg) => {
                            spinner.update(spinners::Dots, msg.clone(), SpinColor::White);
                            sleep(Duration::from_millis(500));
                            std::process::exit(0);
                        }
                        _ => {}
                    }
                    drop(status);
                    enable_raw_mode();
                    if crossterm::event::poll(Duration::from_millis(100)).unwrap() {
                        let ev = crossterm::event::read().unwrap();

                        if ev
                            == Event::Key(KeyEvent {
                                code: KeyCode::Char('c'),
                                modifiers: KeyModifiers::CONTROL,
                                kind: KeyEventKind::Press,
                                state: KeyEventState::NONE,
                            })
                        {
                            let mut status = client_status.lock().unwrap();
                            *status = ClientStatus::ShuttingDown(String::from(
                                "Shutting Down Gracefully...",
                            ));
                            drop(status);
                            disable_raw_mode();
                        }
                    }
                }));
            }

            for handle in threads {
                handle.join();
            }
        });

        Self { ui_service_handle }
    }
    async fn join(self) -> std::result::Result<(), Self::ServiceError> {
        match self.ui_service_handle.join() {
            Ok(_) => Ok(()),
            Err(error) => Err(ThreadJoinError { error }),
        }
    }
}

#[derive(Debug, Error)]
pub struct ThreadJoinError {
    error: Box<dyn Any + Send>,
}

// impl ThreadJoinError {
//     fn new<E: Any + Send>(msg: Box<E>) -> ThreadJoinError {
//         ThreadJoinError { error: msg }
//     }
// }

impl fmt::Display for ThreadJoinError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.error)
    }
}

// impl Error for ThreadJoinError {
//     fn description(&self) -> &str {
//         &self.error.into()
//     }
// }
