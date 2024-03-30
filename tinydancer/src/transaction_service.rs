use std::{str::FromStr, sync::Arc};

use crate::{
    convert_to_websocket, datapoint_valid_signature, send_rpc_call,
    tinydancer::{endpoint, ClientService, Cluster},
    utils::{monitor_and_verify_updates, query_account, send_transaction},
    ValidatorSet,
};
use anyhow::anyhow;
use async_trait::async_trait;
use colored::Colorize;
use serde::{Deserialize, Serialize};
use solana_sdk::{
    pubkey::Pubkey,
    signature::{read_keypair_file, Signable, Signature},
    vote::program::ID as VOTE_PROGRAM_ID,
};
use std::collections::HashMap;
use tiny_logger::logs::info;
use tokio::{sync::Mutex, task::JoinHandle};

pub struct TransactionService {
    pub handler: JoinHandle<()>,
    // pub status: Arc<Mutex<ClientStatus>>,
}

pub struct TransactionServiceConfig {
    pub cluster: Cluster,
    pub validator_set: Arc<Mutex<Vec<(String, u64)>>>,
    pub slot: u64,
    pub current_epoch: u64,

    pub copy_program: Pubkey,
    pub copy_pda: Pubkey,
    pub signer_path: String,
    pub source_account: Pubkey,
}

#[async_trait]
impl ClientService<TransactionServiceConfig> for TransactionService {
    type ServiceError = tokio::task::JoinError;

    fn new(config: TransactionServiceConfig) -> Self {
        // let validator_set = Arc::new(
        //     read_validator_set(&config.validator_set_path).unwrap_or(ValidatorSet::default()),
        // );
        let mut verified_signatures: HashMap<Pubkey, (solana_sdk::message::Message, Signature)> =
            HashMap::new();
        let validator_set = config.validator_set;
        // let slot = config.slot;

        let handler = tokio::spawn(async move {
            info!("Source Account: {:?}", config.source_account);
            info!("Copy Account: {:?}", config.copy_pda);
            let source_account_data =
                query_account(&config.source_account, endpoint(config.cluster.clone()));
            let keypair = read_keypair_file(config.signer_path).unwrap();

            let rpc_url = endpoint(config.cluster);
            send_transaction(
                keypair,
                rpc_url.clone(),
                convert_to_websocket!(rpc_url),
                config.copy_program,
                &config.source_account,
                config.copy_pda,
            );
            println!("Sent txn!");
            monitor_and_verify_updates(&config.source_account, &source_account_data).await;
            // let vote_pubkeys: Vec<String> = validator_set
            //     .lock()
            //     .await
            //     .iter()
            //     .map(|item| item.0.clone())
            //     .collect();
            // // println!("keys: {:?}", vote_pubkeys);
            // let vote_signatures = request_vote_signatures(config.slot, rpc_url, vote_pubkeys).await;
            // // println!("votes: {:?}", vote_signatures);
            // let signatures: Vec<Signature> = vote_signatures
            //     .as_ref()
            //     .unwrap()
            //     .result
            //     .vote_signature
            //     .iter()
            //     .map(|sig| Signature::from_str(sig.as_str()))
            //     .flatten()
            //     .collect();
            // let vote_messages: Vec<solana_sdk::message::Message> = vote_signatures
            //     .unwrap()
            //     .result
            //     .vote_messages
            //     .iter()
            //     .map(|raw_message| bincode::deserialize(raw_message.as_slice()).unwrap())
            //     .collect();
            // let vote_pair: Vec<(&Signature, &solana_sdk::message::Message)> =
            //     signatures.iter().zip(vote_messages.iter()).collect();
            // //println!("pairs: {:?}", vote_pair);
            // for (signature, message) in vote_pair {
            //     let message_pubkey = message.account_keys[0];
            //     assert!(
            //         message.account_keys.contains(&VOTE_PROGRAM_ID),
            //         "This txn is not a vote txn"
            //     );
            //     let validator_set_w = validator_set.lock().await;
            //     let maybe_trusted_pubkey =
            //         validator_set_w
            //             .iter()
            //             .find(|(validator_key, active_stake)| {
            //                 *validator_key == message_pubkey.to_string()
            //             });
            //     match maybe_trusted_pubkey {
            //         Some(key) => {
            //             let validator_pubkey = Pubkey::from_str(key.0.as_str()).unwrap();
            //             let is_signature_valid = signature.verify(
            //                 validator_pubkey.to_bytes().as_slice(),
            //                 message.serialize().as_slice(),
            //             );
            //             if is_signature_valid {
            //                 verified_signatures
            //                     .insert(validator_pubkey, (message.clone(), signature.clone()));
            //                 datapoint_valid_signature!(validator_pubkey, signature);
            //             } else {
            //                 println!("{:?} is not valid", key);
            //             }
            //         }
            //         None => panic!("not a trusted validator: {:?}", message_pubkey),
            //     }
            // }
        });
        Self { handler }
    }
    async fn join(self) -> std::result::Result<(), Self::ServiceError> {
        self.handler.await
    }
}

pub async fn request_vote_signatures(
    slot: u64,
    endpoint: String,
    vote_pubkeys: Vec<String>,
) -> Result<GetVoteSignaturesResponse, serde_json::Error> {
    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getVoteSignatures",
        "params":[
            slot,
            {
                "votePubkey": vote_pubkeys,
                "commitment": "confirmed"
            }
        ]
    }) // getting one shred just to get max shreds per slot, can maybe randomize the selection here
    .to_string();

    let res = send_rpc_call!(endpoint, request);
    // info!("{:?}", res);
    serde_json::from_str::<GetVoteSignaturesResponse>(&res)
}

pub fn read_validator_set(path: &str) -> anyhow::Result<ValidatorSet> {
    match std::fs::read_to_string(path) {
        Ok(data) => {
            let validator_set: Result<ValidatorSet, serde_json::Error> =
                serde_json::from_str(&data);
            validator_set.map_err(|e| anyhow!(e.to_string()))
        }
        Err(e) => Err(anyhow!(e.to_string())),
    }
}
#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetVoteSignaturesResponse {
    pub jsonrpc: String,
    pub result: GetVoteSignaturesResult,
    pub id: i64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetVoteSignaturesResult {
    pub vote_signature: Vec<String>,
    pub vote_messages: Vec<Vec<u8>>,
}
