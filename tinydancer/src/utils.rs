use alloc::rc::Rc;
use anchor_client::{Client, Cluster};
use solana_client::rpc_config::RpcSendTransactionConfig;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::account::Account;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signature, Signer};
use solana_transaction_status::UiTransactionEncoding;
// use solana_transaction_status::UiTransactionEncoding;
// use solana_transaction_status::{UiTransaction, UiTransactionEncoding};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
pub async fn query_account(addr: &Pubkey, url: String) -> Account {
    // let url = DEFAULT_RPC_URL.to_string();
    let client = RpcClient::new(url);
    client.get_account(addr).await.unwrap()
}
use account_proof_geyser::types::Update;
use account_proof_geyser::utils::verify_leaves_against_bankhash;
use borsh::de::BorshDeserialize;
use copy::{
    account_hasher, accounts as copy_accounts, instruction as copy_instruction, CopyAccount, PREFIX,
};
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::runtime::Runtime;
extern crate alloc;
pub async fn monitor_and_verify_updates(
    geyser_ip: String,
    rpc_pubkey: &Pubkey,
    rpc_account: &Account,
) -> anyhow::Result<()> {
    println!("starting monitor: {:?}", geyser_ip);
    let mut stream = TcpStream::connect(&geyser_ip)
        .await
        .expect("unable to connect to 127.0.0.1 on port 5000");
    println!("got stream");
    let mut buffer = vec![0u8; 65536];
    let mut n = stream
        .read(&mut buffer)
        .await
        .expect("unable to read to mutable buffer");
    println!("Reading stream");
    loop {
        if n == 0 {
            tokio::time::sleep(Duration::from_millis(400)).await;
            n = stream
                .read(&mut buffer)
                .await
                .expect("unable to read to mutable buffer");

            // anyhow::bail!("Connection closed");
        } else {
            break;
        }
    }
    let received_update: Update = BorshDeserialize::try_from_slice(&buffer[..n]).unwrap();

    let bankhash = received_update.root;
    let bankhash_proof = received_update.proof;
    let slot_num = received_update.slot;
    for p in bankhash_proof.proofs {
        verify_leaves_against_bankhash(
            &p,
            bankhash,
            bankhash_proof.num_sigs,
            bankhash_proof.account_delta_root,
            bankhash_proof.parent_bankhash,
            bankhash_proof.blockhash,
        )
        .unwrap();

        println!(
            "\nBankHash proof verification succeeded for account with Pubkey: {:?} in slot {}",
            &p.0, slot_num
        );
        println!("Bankhash is: {:?}", received_update.root);
        let copy_account: CopyAccount =
            anchor_lang::AccountDeserialize::try_deserialize(&mut p.1 .0.account.data.as_slice())?;
        let rpc_account_hash = account_hasher(
            &rpc_pubkey,
            rpc_account.lamports,
            &rpc_account.data,
            &rpc_account.owner,
            rpc_account.rent_epoch,
        );
        assert_eq!(rpc_account_hash.as_ref(), &copy_account.digest);
        println!(
            "Hash for rpc account matches Hash verified as part of the BankHash: {}",
            rpc_account_hash
        );
        println!("{:?}", &rpc_account);
    }
    Ok(())
}

pub async fn send_transaction(
    signer: Arc<Keypair>,
    rpc_url: String,
    ws_url: String,
    copy_program: Pubkey,
    source_account: &Pubkey,
    copy_pda: Pubkey,
) -> anyhow::Result<Signature> {
    let creator_pubkey = signer.pubkey();
    let c = Arc::new(Client::new_with_options(
        Cluster::Custom(rpc_url.clone(), ws_url.clone()),
        signer.clone(),
        CommitmentConfig::confirmed(),
    ));
    let prog = c.program(copy_program).unwrap();
    let copy_pda_bump = Pubkey::find_program_address(&[b"copy_hash"], &copy_program).1;
    let mut txn = prog
        .request()
        .accounts(copy_accounts::CopyHash {
            creator: creator_pubkey,
            source_account: *source_account,
            copy_account: copy_pda,
            clock: solana_sdk::sysvar::clock::id(),
            system_program: solana_sdk::system_program::id(),
        })
        .args(copy_instruction::CopyHash {
            bump: copy_pda_bump,
        })
        .options(CommitmentConfig {
            commitment: CommitmentLevel::Processed,
        })
        .transaction()?;

    let client = RpcClient::new_with_commitment(rpc_url.clone(), CommitmentConfig::confirmed());
    let (hash, slot) = client
        .get_latest_blockhash_with_commitment(CommitmentConfig::processed())
        .await
        .unwrap();
    println!("min slot: {:?}", slot - 300);
    let send_cfg = RpcSendTransactionConfig {
        skip_preflight: true,
        preflight_commitment: Some(CommitmentLevel::Processed),
        encoding: Some(UiTransactionEncoding::Base64),
        max_retries: Some(0),
        min_context_slot: Some(slot - 300),
    };
    // let signer_c = Arc::clone(&signer);
    txn.sign(&[&signer], hash);
    let signature = client
        .send_and_confirm_transaction_with_spinner_and_config(
            &txn,
            CommitmentConfig::processed(),
            send_cfg,
        )
        .await?;
    println!("{:?}", signature.to_string());
    Ok(signature)
}
