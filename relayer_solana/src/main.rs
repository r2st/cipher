use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    system_instruction::transfer,
    system_program,
    transaction::Transaction,
};
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    // Connect to a Solana RPC endpoint
    let rpc_url = "https://api.mainnet-beta.solana.com".to_string();
    let rpc_client = RpcClient::new(rpc_url);

    // Replace with the sender's private key file path
    let sender_keypair = solana_sdk::signature::read_keypair_file("/path/to/sender/keypair.json")?;
    let sender_pubkey = sender_keypair.pubkey();

    // Replace with the recipient's public key
    let recipient_pubkey = Pubkey::new(&[0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa]);

    // Amount to transfer
    let lamports_to_send = 1000000; // Replace with the desired amount

    // Create a transaction
    let recent_blockhash = rpc_client.get_latest_blockhash()?;
    let mut transaction = Transaction::new_with_payer(
        &[transfer(
            &sender_pubkey,
            &recipient_pubkey,
            lamports_to_send,
        )],
        Some(&sender_pubkey),
    );

    // Sign the transaction
    transaction.sign(&[&sender_keypair], recent_blockhash);

    // Send the transaction to the Solana network
    let signature = rpc_client.send_and_confirm_transaction(&transaction)?;

    println!("Transaction sent with signature: {:?}", signature);

    Ok(())
}
