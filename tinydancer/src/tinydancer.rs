//! Sampler struct - incharge of sampling shreds
// use rayon::prelude::*;

use std::{
    env,
    sync::{Arc, MutexGuard},
    thread::Result,
};

// use tokio::time::Duration;
use crate::{
    block_on,
    transaction_service::{read_validator_set, TransactionService, TransactionServiceConfig},
};
use anyhow::anyhow;
use async_trait::async_trait;
use futures::{future::join_all, TryFutureExt};
use rand::seq::index::sample;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use tiny_logger::logs::info;
// use log::info;
// use log4rs;
use std::error::Error;
use tokio::{runtime::Runtime, sync::Mutex, task::JoinError, try_join};
// use std::{thread, thread::JoinHandle, time::Duration};

#[async_trait]
pub trait ClientService<T> {
    type ServiceError: std::error::Error;

    fn new(config: T) -> Self;
    async fn join(self) -> std::result::Result<(), Self::ServiceError>;
}

pub struct TinyDancer {
    config: TinyDancerConfig,
    transaction_service: TransactionService,
    epoch_validator_set: Vec<(String, u64)>,
    // current_epoch: u64
}

#[derive(Clone)]
pub struct TinyDancerConfig {
    pub rpc_endpoint: Cluster,
    pub validator_set_path: String,
    // pub archive_config: ArchiveConfig,
    pub log_path: String,
    pub slot: u64,
    pub copy_program: Pubkey,
    pub copy_pda: Pubkey,
    pub signer_path: String,
    pub source_account: Pubkey,
}

// use solana_metrics::datapoint_info;
// use std::ffi::OsString;
// use std::fs::read_dir;
// use std::io;
// use std::io::ErrorKind;
// use std::path::PathBuf;
//
impl TinyDancer {
    pub async fn start(config: TinyDancerConfig) -> anyhow::Result<()> {
        let status = ClientStatus::Initializing(String::from("Starting Up Tinydancer"));

        let client_status = Arc::new(Mutex::new(status));
        // let status_sampler = client_status.clone();

        let validator_set =
            read_validator_set(&config.validator_set_path).map_err(|e| anyhow!(e.to_string()))?;
        let current_epoch = validator_set.epoch;
        let TinyDancerConfig {
            rpc_endpoint,
            log_path,
            slot,
            validator_set_path, // archive_config,

            copy_program,
            copy_pda,
            signer_path,
            source_account,
        } = config.clone();
        std::env::set_var("RUST_LOG", "info");
        tiny_logger::setup_file_with_default(&log_path, "RUST_LOG");

        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.set_error_if_exists(false);
        opts.create_missing_column_families(true);

        let validator_set = match config.rpc_endpoint {
            Cluster::Mainnet => validator_set.mainnet_beta,
            Cluster::Testnet => validator_set.testnet,
            Cluster::Devnet => validator_set.devnet,
            _ => validator_set.custom,
        };

        let validator_set = Arc::new(Mutex::new(validator_set));
        // setup db
        // let db = rocksdb::DB::open_cf(&opts, archive_config.clone().archive_path, vec![SHRED_CF])
        //     .unwrap();
        // let db = Arc::new(db);

        let transaction_service = TransactionService::new(TransactionServiceConfig {
            cluster: rpc_endpoint.clone(),
            // db_instance: db.clone(),
            validator_set,
            slot,
            current_epoch,
            copy_program,
            copy_pda,
            signer_path,
            source_account,
        });

        transaction_service
            .join()
            .await
            .expect("error in transaction_service");

        // if let Some(ui_service) = ui_service {
        //     block_on!(async { ui_service.join().await }, "Ui Service Error");
        // }

        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Cluster {
    Mainnet,
    Devnet,
    Testnet,
    Localnet,
    Custom(String),
}

pub fn endpoint(cluster: Cluster) -> String {
    let cluster = cluster;
    match cluster {
        Cluster::Mainnet => String::from("https://api.mainnet-beta.solana.com"),
        Cluster::Testnet => String::from("https://api.testnet.solana.com"),
        Cluster::Devnet => String::from("https://api.devnet.solana.com"),
        Cluster::Localnet => String::from("http://0.0.0.0:8899"),
        Cluster::Custom(url) => url,
    }
}
pub fn geyser_endpoint(rpc_url: String) -> String {
    rpc_url.replace("8899", "5000").replace("http://", "")
}
pub enum ClientStatus {
    Initializing(String),
    SearchingForRPCService(String),
    Active(String),
    Crashed(String),
    ShuttingDown(String),
}
