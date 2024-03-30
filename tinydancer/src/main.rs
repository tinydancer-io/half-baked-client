// #![feature(async_closure)]
// #![allow(unused_imports)]
// #![allow(dead_code)]
// #![feature(mutex_unlock)]
// #![feature(inherent_associated_types)]
mod tinydancer;
use crossterm::style::Stylize;
use reqwest::header::{ACCEPT, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use solana_sdk::pubkey::Pubkey;
use spinoff::{spinners, Color, Spinner};
use std::{
    f32::consts::E,
    fs::{self, File, OpenOptions},
    io,
    path::{Path, PathBuf},
    str::FromStr,
    thread::sleep,
    time::Duration,
};
use tinydancer::{endpoint, Cluster, TinyDancer, TinyDancerConfig};
use transaction_service::read_validator_set;

mod macros;
mod transaction_service;
mod utils;
use colored::Colorize;

use anyhow::{anyhow, Result};
use clap::{ArgGroup, Parser, Subcommand, *};
use tracing::info;
use tracing_subscriber;

pub const COPY_PROGRAM: &str = "BgaRwBpqNYbK8WSR4x1rtZn7LMhuwpHqF3nCoFtSjZjg";
pub const COPY_PDA: &str = "Bg3ZP9GymdRNSojqPs3BrDfwmjS3jXJUMu5jYZ6hR7kv";
pub const DEFAULT_SIGNER_PATH: &str = "/.config/solana/id.json";

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

        #[clap(long, required = true)]
        slot: u64,

        #[clap(long, required = true)]
        source_account: String,
        // /// If you want to enable detailed tui to monitor
        // #[clap(long, short, default_value_t = false)]
        // tui_monitor: bool,

        // /// Amount of shreds you want to sample per slot
        // #[clap(long, short, default_value_t = 10)]
        // sample_qty: usize,
        // /// Rocks db path for storing shreds
        // #[clap(required = false)]
        // archive_path: Option<String>,
        // /// Duration after which shreds will be purged
        // #[clap(required = false, default_value_t = 10000000)]
        // shred_archive_duration: u64,
    },
    /// Verify the samples for a single slot
    Verify {
        #[clap(long, required = false, default_value = "0")]
        slot: u64,
        // #[clap(long, required = false, default_value = "10")]
        // sample_qty: usize,
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
        #[clap(long, required = false)]
        log_path: Option<String>,
        /// The cluster you want to run the client on (Mainnet, Localnet,Devnet, <custom-url>)
        #[clap(long, short, required = false)]
        cluster: Option<String>,

        #[clap(long, short, required = false)]
        validator_set_path: Option<String>,

        #[clap(long, short, required = false)]
        copy_program: Option<String>,

        #[clap(long, short, required = false)]
        copy_pda: Option<String>,

        #[clap(long, short, required = false)]
        signer_path: Option<String>,
    },
    Get,
}

pub fn get_config_file() -> Result<ConfigSchema> {
    let home_path = std::env::var("HOME")?;
    let path = home_path.clone() + "/.config/tinydancer/config.json";
    let config_str = std::fs::read_to_string(path)?;
    Ok(serde_json::from_str::<ConfigSchema>(&config_str)?)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    match args.command {
        Commands::Logs { log_path } => {
            let log_path = match get_config_file() {
                Ok(config) => config.log_path,
                Err(_) => log_path,
            };
            println!("Log Path: {}", log_path);
            std::process::Command::new("tail")
                .arg("-f")
                .arg(log_path)
                .status()
                .expect("log command failed");
        }

        Commands::Start {
            enable_ui_service,
            slot,
            source_account, // sample_qty,
                            // archive_path,
                            // shred_archive_duration,
                            // tui_monitor,
        } => {
            let config_file =
                get_config_file().map_err(|_| anyhow!("tinydancer config not set"))?;
            println!("genesis path {:?}", config_file.validator_set_path);
            let config = TinyDancerConfig {
                // enable_ui_service,
                rpc_endpoint: get_cluster(config_file.cluster),
                // sample_qty,
                // tui_monitor,
                log_path: config_file.log_path,
                slot,
                validator_set_path: config_file.validator_set_path,
                copy_program: Pubkey::from_str(&config_file.copy_program).unwrap(),
                copy_pda: Pubkey::from_str(&config_file.copy_pda).unwrap(),
                signer_path: config_file.signer_path,

                source_account: Pubkey::from_str(&source_account).unwrap(), // archive_config: {
                                                                            //     archive_path
                                                                            //         .map(|path| {
                                                                            //             Ok(ArchiveConfig {
                                                                            //                 shred_archive_duration,
                                                                            //                 archive_path: path,
                                                                            //             })
                                                                            //         })
                                                                            //         .unwrap_or(Err(anyhow!("shred path not provided...")))?
                                                                            // },
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
                        .arg(home_path.clone() + "/.config/tinydancer/config.json")
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
            ConfigSubcommands::Set {
                log_path,
                cluster,
                validator_set_path,
                copy_pda,
                copy_program,
                signer_path,
            } => {
                // println!("{:?}", fs::create_dir_all("~/.config/tinydancer"));

                let home_path = std::env::var("HOME").unwrap();
                let tinydancer_dir = home_path.clone() + "/.config/tinydancer";

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

                    std::fs::write(
                        config_path.clone(),
                        serde_json::to_string_pretty(&serde_json::json!({
                            "cluster":"",
                            "validatorSetPath":"",
                            "logPath":"",
                            "copyProgram":"",
                            "copyPda": "",
                            "signerPath":""
                        }))?,
                    )?;
                }
                sleep(Duration::from_secs(1));

                let config_file = get_config_file();
                match config_file {
                    Ok(mut config_file) => {
                        // overwrite
                        config_file.cluster = cluster.unwrap_or(config_file.cluster);
                        config_file.log_path = log_path.unwrap_or(config_file.log_path);
                        config_file.validator_set_path =
                            validator_set_path.unwrap_or(config_file.validator_set_path);
                        config_file.copy_program = copy_program.unwrap_or(config_file.copy_program);

                        config_file.copy_pda = copy_pda.unwrap_or(config_file.copy_pda);
                        config_file.signer_path = signer_path.unwrap_or(config_file.signer_path);
                        std::fs::write(config_path, serde_json::to_string_pretty(&config_file)?)?;
                    }
                    Err(e) => {
                        println!("Error: {:?}", e.to_string());
                        // initialize
                        std::fs::write(
                            config_path,
                            serde_json::to_string_pretty(&serde_json::json!({
                                "cluster":"Localnet",
                                "logPath":"/tmp/client.log",
                                "copyProgram": COPY_PROGRAM,
                                "copyPda":COPY_PDA,
                                "signerPath": home_path + DEFAULT_SIGNER_PATH,
                                "validatorSetPath": ""
                            }))?,
                        )?;
                    }
                }
            }
        },
        Commands::Verify { slot } => {
            todo!()
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
    pub validator_set_path: String,
    pub copy_program: String,
    pub copy_pda: String,
    pub signer_path: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ValidatorSet {
    pub epoch: u64,
    #[serde(rename = "mainnet-beta")]
    pub mainnet_beta: Vec<(String, u64)>,
    pub testnet: Vec<(String, u64)>,
    pub devnet: Vec<(String, u64)>,
    pub custom: Vec<(String, u64)>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetSlotResponse {
    pub jsonrpc: String,
    pub result: i64,
    pub id: i64,
}
