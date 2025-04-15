pub mod blockchain;
pub mod cache;
pub mod cli;
pub mod constant;
pub mod primitives;
pub mod score_db;

use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use blockchain::Blockchain;
use clap::Parser;
use cli::Cli;
use constant::{INI_BLOCK_NUMBER_TC, INI_BLOCK_NUMBER_BYBIT, POS_BLOCK_NUMBER, END_BLOCK_NUMBER};
use dotenvy::dotenv;
use eyre::Result;
use nimiq_database::mdbx::MdbxDatabase;
use std::{env, sync::Arc};
use tokio::sync::{Mutex, Notify};

#[tokio::main]
async fn main() -> Result<()> {
    // Load the environment variables from the file ".env".
    dotenv().expect(".env file not found");
    // Get the RPC URL from the environment variable.
    let rpc_url: &str = &env::var("LOCAL_WS_URL").expect("URL must be set");
    // Create a provider.
    let provider = ProviderBuilder::new()
        .on_ws(WsConnect::new(rpc_url))
        .await?;

    // Create a database under the directory "./database".
    let db = MdbxDatabase::new("./database/database_final", Default::default())?;

    // Get the current blockchain state
    let blockchain = Blockchain::load(db.clone());
    let init_block_number = blockchain.get_last_block_number();
    // Get the command from CLI
    let cli = Cli::parse();
    let (blockchain, block_number) = if cli.is_reset() || init_block_number.is_none() {
        (
            Blockchain::init_new(db, &provider, INI_BLOCK_NUMBER_TC - 1).await?,
            INI_BLOCK_NUMBER_TC,
        )
    } else {
        // The default mode is resuming from the current state.
        (Blockchain::load(db), init_block_number.unwrap() + 1)
    };

    // let latest_block_number = Arc::new(Mutex::new(POS_BLOCK_NUMBER));
    // let notify = Arc::new(Notify::new());

    // let provider_clone = provider.clone();
    // let latest_block_number_clone = Arc::clone(&latest_block_number);
    // let notify_clone = Arc::clone(&notify);

    // tokio::spawn(async move {
    //     // Subscribe to new blocks.
    //     let mut block_subscription = provider_clone
    //         .subscribe_blocks()
    //         .await
    //         .expect("Cannot subscribe the latest block!");
    //     // Set the block number upon receiving a new block.
    //     while let Ok(block) = block_subscription.recv().await {
    //         let new_block_number = block
    //             .header
    //             .number
    //             .expect("Cannot get the latest block number!");
    //         let mut latest_block_number = latest_block_number_clone.lock().await;
    //         *latest_block_number = new_block_number;
    //         notify_clone.notify_one();
    //     }
    // });

    let mut current_block_number = block_number;
    println!("Start from   {}   ...", current_block_number);
    // loop {
    //     {
    //         let latest_block_number = latest_block_number.lock().await;
    //         // Trace each block
    //         while current_block_number <= *latest_block_number {
    //             // Get the target block using block number.
    //             let target_block = provider
    //                 .get_block_by_number(current_block_number.into(), true)
    //                 .await?
    //                 .expect("Cannot get the target block!");

    //             blockchain.record_block(target_block, &provider).await?;
    //             current_block_number += 1;
    //         }
    //     }
    //     notify.notified().await;
    // }

    let latest_block_number = END_BLOCK_NUMBER;
    while current_block_number <= latest_block_number {
        // Get the target block using block number.
        let target_block = provider
            .get_block_by_number(current_block_number.into(), true)
            .await?
            .expect("Cannot get the target block!");

        blockchain.record_block(target_block, &provider).await?;
        current_block_number += 1;
    }

    Ok(())
}
