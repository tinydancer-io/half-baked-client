//! client struct
//! new -> self
//! join -> JoinHandle
//!
//!
//! things happening at the same time:
//! sampling -> pull data -> run sampling algo
//! monitoring system -> slot number | shreds req/rec | sampling progress | connected nodes
//! ui -> display stats
//!
//! let (rx,tx) = channel();
//! spawn(move{
//!     let sampler = Sampler::new(tx)
//!     sampler.join();         
//! })
//!
//! spawn(move{
//!     let monitor = Monitor::new(config, rx)
//! });
//! let rx2 = rx.clone();
//! spawn(move{
//!     let ui = Ui::new(...)
//!  while let r = rx2.recv(){
//!     
//! }
//! })
// #![feature(async_closure)]
#![allow(unused_imports)]
#![allow(dead_code)]
// #![feature(mutex_unlock)]
mod tinydancer;
use crossterm::style::Stylize;
use reqwest::header::{ACCEPT, CONTENT_TYPE};
use sampler::{pull_and_verify_shreds, ArchiveConfig};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use spinoff::{spinners, Color, Spinner};
use std::{
    f32::consts::E,
    fs::{self, File, OpenOptions},
    io,
    path::{Path, PathBuf},
    thread::sleep,
    time::Duration,
};
use tinydancer::{endpoint, Cluster, TinyDancer, TinyDancerConfig};
mod macros;
use colored::Colorize;
mod rpc_wrapper;
mod sampler;
mod ui;

use anyhow::{anyhow, Result};
use clap::{ArgGroup, Parser, Subcommand, *};
use tracing::info;
use tracing_subscriber;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Subcommands to run
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Start the local light client
    Start {
        /// If you want to enable the status monitor
        #[clap(long, short, default_value_t = true)]
        enable_ui_service: bool,

        /// If you want to enable detailed tui to monitor
        #[clap(long, short, default_value_t = false)]
        tui_monitor: bool,

        /// Amount of shreds you want to sample per slot
        #[clap(long, short, default_value_t = 10)]
        sample_qty: usize,
        /// Rocks db path for storing shreds
        #[clap(required = false)]
        archive_path: Option<String>,

        /// Duration after which shreds will be purged
        #[clap(required = false, default_value_t = 10000000)]
        shred_archive_duration: u64,
    },
    /// Verify the samples for a single slot
    Verify {
        #[clap(long, required = false, default_value = "0")]
        slot: usize,

        #[clap(long, required = false, default_value = "10")]
        sample_qty: usize,
    },
    /// Stream the client logs to your terminal
    Logs {
        #[clap(long, required = false, default_value = "/tmp/client.log")]
        log_path: String,
    },
    /// Edit your client config
    #[clap(subcommand)]
    Config(ConfigSubcommands),
    // Get the latest slot
    Slot,
}

#[derive(Debug, Subcommand)]
pub enum ConfigSubcommands {
    Set {
        #[clap(long, required = false, default_value = "/tmp/client.log")]
        log_path: String,
        /// The cluster you want to run the client on (Mainnet, Localnet,Devnet, <custom-url>)
        #[clap(long, short, required = false, default_value = "Localnet")]
        cluster: String,
    },
    Get,
}

pub fn get_config_file() -> Result<ConfigSchema> {
    let home_path = std::env::var("HOME")?;
    let path = home_path + "/.config/tinydancer/config.json";
    let config_str = std::fs::read_to_string(path)?;
    Ok(serde_json::from_str::<ConfigSchema>(&config_str)?)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    match args.command {
        Commands::Logs { log_path } => {
            std::process::Command::new("tail")
                .arg("-f")
                .arg(log_path)
                .output()
                .expect("log command failed");
        }

        Commands::Start {
            enable_ui_service,
            sample_qty,
            archive_path,
            shred_archive_duration,
            tui_monitor,
        } => {
            let config_file =
                get_config_file().map_err(|_| anyhow!("tinydancer config not set"))?;
            let config = TinyDancerConfig {
                enable_ui_service,
                rpc_endpoint: get_cluster(config_file.cluster),
                sample_qty,
                tui_monitor,
                log_path: config_file.log_path,
                archive_config: {
                    archive_path
                        .map(|path| {
                            Ok(ArchiveConfig {
                                shred_archive_duration,
                                archive_path: path,
                            })
                        })
                        .unwrap_or(Err(anyhow!("shred path not provided...")))?
                },
            };

            TinyDancer::start(config).await.unwrap();
        }

        Commands::Slot => {
            let config_file =
                get_config_file().map_err(|_| anyhow!("tinydancer config not set"))?;
            let slot_res = {
                let req_client = reqwest::Client::new();
                let res = req_client
                    .post(get_endpoint(config_file.cluster))
                    .body(
                        serde_json::json!({"jsonrpc":"2.0","id":1, "method":"getSlot"}).to_string(),
                    )
                    .header(CONTENT_TYPE, "application/json")
                    .header(ACCEPT, "application/json")
                    .send()
                    .await;

                res
            };

            match slot_res {
                Ok(get_slot_response) => {
                    let slot_text = get_slot_response.text().await.map_err(|e| {
                        anyhow!("Failed to get slot due to error: {}", e.to_string())
                    })?;

                    let slot = serde_json::from_str::<GetSlotResponse>(&slot_text.as_str());

                    match slot {
                        Ok(slot) => {
                            println!("Slot: {}", slot.result.to_string().green(),);
                        }
                        Err(e) => {
                            println!("Failed to get slot due to error: {}", e.to_string().red());
                        }
                    }
                }
                Err(e) => {
                    println!("Failed to get slot,due to error: {}", e.to_string().red());
                }
            }
        }
        Commands::Config(sub_config) => match sub_config {
            ConfigSubcommands::Get => {
                let home_path = std::env::var("HOME").unwrap();
                let is_existing = home_path.clone() + "/.config/tinydancer/config.json";
                let path = Path::new(&is_existing);
                if path.exists() {
                    std::process::Command::new("cat")
                        .arg(home_path + "/.config/tinydancer/config.json")
                        .spawn()
                        .expect("Config not set");
                } else {
                    println!(
                        "{} {}",
                        "Initialise a config first using:".to_string().yellow(),
                        "tinydancer set config".to_string().green()
                    );
                }
            }
            ConfigSubcommands::Set { log_path, cluster } => {
                // println!("{:?}", fs::create_dir_all("~/.config/tinydancer"));

                let home_path = std::env::var("HOME").unwrap();
                let tinydancer_dir = home_path + "/.config/tinydancer";

                let path = Path::new(&tinydancer_dir);
                if !path.exists() {
                    std::process::Command::new("mkdir")
                        .arg(&tinydancer_dir)
                        .stdout(std::process::Stdio::null())
                        .spawn()
                        .expect("couldnt make dir");
                }
                sleep(Duration::from_secs(1));

                let config_path = tinydancer_dir + "/config.json";
                let path = Path::new(&config_path);
                if !path.exists() {
                    std::process::Command::new("touch")
                        .arg(&config_path)
                        .stdout(std::process::Stdio::null())
                        .spawn()
                        .expect("couldnt make file");
                }
                sleep(Duration::from_secs(1));

                let config_file = get_config_file();
                match config_file {
                    Ok(mut config_file) => {
                        // overwrite
                        config_file.log_path = log_path;
                        config_file.cluster = cluster;
                        std::fs::write(config_path, serde_json::to_string_pretty(&config_file)?)?;
                    }
                    Err(_) => {
                        // initialize
                        std::fs::write(
                            config_path,
                            serde_json::to_string_pretty(&serde_json::json!({
                                "cluster":"Localnet",
                                "logPath":"/tmp/client.log"
                            }))?,
                        )?;
                    }
                }
            }
        },
        Commands::Verify { slot, sample_qty } => {
            let _spinner = Spinner::new(
                spinners::Dots,
                format!("Verifying Shreds for Slot {}", slot),
                Color::Green,
            );

            let config_file =
                get_config_file().map_err(|_| anyhow!("tinydancer config not set"))?;
            let is_verified =
                pull_and_verify_shreds(slot, get_endpoint(config_file.cluster), sample_qty).await;

            if is_verified {
                println!(
                    "\nSlot {} is {} ✓",
                    slot.to_string().yellow(),
                    "Valid".to_string().green()
                );
            } else {
                println!(
                    "\nSlot {} is not {} ❌",
                    slot.to_string().yellow(),
                    "Valid".to_string().red()
                );
            }
        }
    }

    Ok(())
}

pub fn get_cluster(cluster: String) -> Cluster {
    match cluster.as_str() {
        "Mainnet" => Cluster::Mainnet,
        "Devnet" => Cluster::Devnet,
        "Localnet" => Cluster::Localnet,
        _ => Cluster::Custom(cluster),
    }
}
pub fn get_endpoint(cluster: String) -> String {
    match cluster.as_str() {
        "Mainnet" => "https://api.mainnet-beta.solana.com".to_owned(),
        "Devnet" => "https://api.devnet.solana.com".to_owned(),
        "Localnet" => "http://0.0.0.0:8899".to_owned(),
        _ => cluster.to_owned(),
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigSchema {
    pub log_path: String,
    pub cluster: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetSlotResponse {
    pub jsonrpc: String,
    pub result: i64,
    pub id: i64,
}
