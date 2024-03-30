use solana_rpc_client::rpc_client::RpcClient;
use solana_sdk::account::Account;
use solana_sdk::pubkey::Pubkey;
pub fn query_account(addr: &Pubkey, url: String) -> Account {
    // let url = DEFAULT_RPC_URL.to_string();
    let client = RpcClient::new(url);
    client.get_account(addr).unwrap()
}
