use alloy::{
    primitives::{utils::parse_units, Address, U256},
    providers::{ext::DebugApi, Provider, RootProvider},
    pubsub::PubSubFrontend,
    rpc::types::{
        trace::{
            common::TraceResult,
            geth::{
                CallFrame, GethDebugBuiltInTracerType, GethDebugTracerType,
                GethDebugTracingOptions, GethTrace,
            },
        },
        Block, Transaction,
    },
};
use bb8::PooledConnection;
use bb8_postgres::PostgresConnectionManager;
use eyre::Result;
use nimiq_database::{
    mdbx::{MdbxDatabase, MdbxWriteTransaction},
    traits::{Database, WriteTransaction},
};
use std::collections::BTreeMap;
use tokio_postgres::{Client as PostgresClient, NoTls};

use crate::{
    cache::Cache,
    constant::POS_BLOCK_NUMBER,
    primitives::{AddressKey, Score},
    score_db::ScoreDb,
};

pub struct Blockchain {
    score_db: ScoreDb,
    cache: Cache,
    db: MdbxDatabase,
}

impl Blockchain {
    /// Loads existing state.
    pub fn load(db: MdbxDatabase) -> Self {
        Self {
            score_db: ScoreDb::new(db.clone()),
            cache: Cache::new(),
            db,
        }
    }

    /// Gets the block number from the last time running
    pub fn get_last_block_number(&self) -> Option<u64> {
        let txn = self.db.read_transaction();
        self.score_db.get_last_block_number(&txn)
    }

    /// Initializes a new state and removes any existing data.
    pub async fn init_new(
        db: MdbxDatabase,
        provider: &RootProvider<PubSubFrontend>,
        block_number: u64,
    ) -> Result<Self> {
        let blockchain = Blockchain::load(db);
        let mut txn = blockchain.db.write_transaction();
        let cache = &blockchain.cache;
        blockchain.score_db.clear(&mut txn);
        blockchain
            .score_db
            .init_tc(cache, provider, block_number)
            .await?;
        blockchain
            .score_db
            .flush_cache(&mut txn, provider, cache, block_number)
            .await?;
        txn.commit();
        Ok(blockchain)
    }

    /// Atomically record all transactions in a block.
    pub async fn record_block(
        &self,
        block: Block,
        provider: &RootProvider<PubSubFrontend>,
    ) -> Result<()> {
        let mut txn = self.db.write_transaction();
        let cache = &self.cache;

        let block_number = block.header.number.expect("Block should have a number");
        let block_transactions = block
            .transactions
            .as_transactions()
            .expect("Cannot get the block transactions!");

        // Deal with uncleanliness state change using Geth Debug trace results
        let geth_trace_options = GethDebugTracingOptions::default().with_tracer(
            GethDebugTracerType::BuiltInTracer(GethDebugBuiltInTracerType::CallTracer),
        );
        let geth_trace_results = provider
            .debug_trace_block_by_number(block_number.into(), geth_trace_options)
            .await?;
        for (transaction, geth_trace_result) in
            block_transactions.iter().zip(geth_trace_results.iter())
        {
            self.record_transaction(
                &mut txn,
                cache,
                &block,
                transaction,
                geth_trace_result,
                provider,
            )
            .await?;
        }

        self.record_reward(&mut txn, cache, block, provider).await?;

        self.score_db
            .flush_cache(&mut txn, provider, cache, block_number)
            .await?;

        txn.commit();

        Ok(())
    }

    /// Record transaction.
    async fn record_transaction<'a>(
        &self,
        txn: &mut MdbxWriteTransaction<'a>,
        cache: &Cache,
        block: &Block,
        transaction: &Transaction,
        geth_trace_result: &TraceResult<GethTrace, String>,
        provider: &RootProvider<PubSubFrontend>,
    ) -> Result<()> {
        let block_number = block.header.number.expect("Block should have a number");
        let block_miner = block.header.miner;
        let block_base_fee_per_gas = U256::from(block.header.base_fee_per_gas.unwrap_or(0));

        let gas_price = U256::from(transaction.gas_price.unwrap_or(0));
        let max_fee_per_gas = U256::from(transaction.max_fee_per_gas.unwrap_or(0));
        let max_priority_fee_per_gas = match transaction.max_priority_fee_per_gas {
            // EIP-1559 transactions will have this field
            Some(tips) => {
                let tips = U256::from(tips);
                // EIP-1559 requirement: max_fee_per_gas > max_priority_fee_per_gas + base_fee_per_gas
                if max_fee_per_gas > tips + block_base_fee_per_gas {
                    tips
                } else {
                    // Transactions that do not meet that requirement
                    U256::from(gas_price - block_base_fee_per_gas)
                }
            }
            // Legacy transactions
            None => U256::from(gas_price - block_base_fee_per_gas),
        };
        let has_blobs = transaction.blob_versioned_hashes.is_some();

        match geth_trace_result {
            TraceResult::Success { result, tx_hash } => {
                let tx_hash = tx_hash.expect("Cannot get the transaction hash from geth trace!");
                let call_trace = result.clone().try_into_call_frame()?;
                let tx_gas_used = call_trace.gas_used;
                let transaction_fee_from_sender = tx_gas_used * gas_price;
                let transaction_fee_to_miner = tx_gas_used * max_priority_fee_per_gas;
                let mut blob_fee = U256::ZERO;
                if has_blobs {
                    let transaction_receipt = provider
                        .get_transaction_receipt(tx_hash)
                        .await?
                        .expect("Cannot get transaction receipt!");
                    let blob_gas_used = transaction_receipt.blob_gas_used.unwrap_or(0_u128);
                    let blob_gas_price = transaction_receipt.blob_gas_price.unwrap_or(0_u128);
                    blob_fee = U256::from(blob_gas_used * blob_gas_price);
                }

                // Deal with the transaction fee sent by the transaction sender
                let tx_sender = call_trace.from;

                let address_key_tx_sender = AddressKey::new(tx_sender);
                let address_key_block_miner = AddressKey::new(block_miner);

                // Record transaction fees.
                self.score_db
                    .record_transfer(
                        txn,
                        cache,
                        provider,
                        block_number,
                        Some(&address_key_tx_sender),
                        &address_key_block_miner,
                        transaction_fee_from_sender + blob_fee,
                        Some(transaction_fee_to_miner),
                    )
                    .await?;

                // Deal with ETH transfer in Geth Debug trace results using Depth First Traversal
                self.depth_first_traversal(txn, cache, &call_trace, provider, block_number)
                    .await?;
            }
            TraceResult::Error { .. } => {
                eprintln!("Trace Failed")
            }
        }

        // The address in this transaction is self-destructed, so we set the score to zero.
        for address in cache.drain_self_destruct() {
            cache.insert_data(address, Score::new(U256::ZERO, U256::ZERO));
        }

        Ok(())
    }

    /// Deal with ETH transfer in Geth Debug trace results using Depth First Traversal
    async fn depth_first_traversal<'a>(
        &self,
        txn: &mut MdbxWriteTransaction<'a>,
        cache: &Cache,
        root: &CallFrame,
        provider: &RootProvider<PubSubFrontend>,
        block_number: u64,
    ) -> Result<()> {
        // Define a stack to help with Depth First Traversal
        let mut stack: Vec<std::slice::Iter<CallFrame>> = Vec::new();

        // If the parent trace is failed,
        // itself and all its child traces will not be executed.
        if root.error.is_none() {
            stack.push(root.calls.iter());
            self.process_frame(txn, cache, root, provider, block_number)
                .await?;
        }

        while let Some(top_iter) = stack.last_mut() {
            if let Some(next_frame) = top_iter.next() {
                // If the parent trace is failed,
                // itself and all its child traces will not be executed.
                if next_frame.error.is_none() {
                    stack.push(next_frame.calls.iter());
                    self.process_frame(txn, cache, next_frame, provider, block_number)
                        .await?;
                }
            } else {
                stack.pop();
            }
        }

        Ok(())
    }

    /// Process ETH transfer in each Geth Debug trace frame
    async fn process_frame<'a>(
        &self,
        txn: &mut MdbxWriteTransaction<'a>,
        cache: &Cache,
        frame: &CallFrame,
        provider: &RootProvider<PubSubFrontend>,
        block_number: u64,
    ) -> Result<()> {
        let call_type = &frame.typ;
        // The `DELEGATECALL` (`CALLCODE`) and `STATICCALL` call types cannot transfer ETH
        if call_type != "DELEGATECALL" && call_type != "CALLCODE" && call_type != "STATICCALL" {
            // Record the self-destructed address
            if call_type == "SELFDESTRUCT" {
                // A strange corner case:
                // After the self-destruct operation, the address is still a contract address
                // probably due to the error in self-destruct operation (but not explicitly shown in the trace)
                // 0x6550f9A4bd878A384625F62Ad5AAb1fE7C3412dE in block 19481732
                let code = provider
                    .get_code_at(frame.from)
                    .block_id(block_number.into())
                    .await?;
                if code.len() == 0 {
                    cache.insert_self_destruct(AddressKey::new(frame.from));
                }
            }
            let transfer_value = frame.value.unwrap_or(U256::ZERO);
            // The state changes if and only if the transfer value is not ZERO
            if transfer_value != U256::ZERO {
                // Deal with the ETH transfer sent by the from address in trace frame
                let from_address = frame.from;
                let address_key_from = AddressKey::new(from_address);
                let to_address = frame
                    .to
                    .expect("Cannot get the to address in the trace frame");
                let address_key_to = AddressKey::new(to_address);

                self.score_db
                    .record_transfer(
                        txn,
                        cache,
                        provider,
                        block_number,
                        Some(&address_key_from),
                        &address_key_to,
                        transfer_value,
                        None,
                    )
                    .await?;
            }
        }
        Ok(())
    }

    /// Record block/uncle rewards in PoW and beacon withdrawals in PoS.
    async fn record_reward<'a>(
        &self,
        txn: &mut MdbxWriteTransaction<'a>,
        cache: &Cache,
        block: Block,
        provider: &RootProvider<PubSubFrontend>,
    ) -> Result<()> {
        let block_number = block.header.number.expect("Block should have a number");
        let block_miner = block.header.miner;

        // Deal with block reward and uncle reward in PoW, and beacon withdrawal in PoS
        if block_number < POS_BLOCK_NUMBER {
            // Calculate static block reward and uncle inclusion reward when PoW
            let static_block_reward: U256 = parse_units("2", "ether")?.into();
            let uncle_count = block.uncles.len() as u64;
            let uncle_inclusion_reward =
                static_block_reward / U256::from(32) * U256::from(uncle_count);

            let address_key_block_miner = AddressKey::new(block_miner);

            // Block miner reward
            self.score_db
                .record_transfer(
                    txn,
                    cache,
                    provider,
                    block_number,
                    None,
                    &address_key_block_miner,
                    static_block_reward + uncle_inclusion_reward,
                    None,
                )
                .await?;

            // Update the state of all uncle miners
            for idx in 0..uncle_count {
                let uncle_block = provider
                    .get_uncle(block_number.into(), idx)
                    .await?
                    .expect("Cannot get the uncle block!");
                let uncle_miner = uncle_block.header.miner;
                let uncle_number = uncle_block.header.number;
                if let Some(uncle_number) = uncle_number {
                    // Calculate the uncle reward when PoW
                    let uncle_reward = U256::from(uncle_number + 8 - block_number)
                        * static_block_reward
                        / U256::from(8);

                    // Update the state of the uncle miner
                    let address_key_uncle_miner = AddressKey::new(uncle_miner);
                    self.score_db
                        .record_transfer(
                            txn,
                            cache,
                            provider,
                            block_number,
                            None,
                            &address_key_uncle_miner,
                            uncle_reward,
                            None,
                        )
                        .await?;
                }
            }
        } else {
            // Only deal with beacon withdrawal in PoS
            // since the static block reward is ZERO, and no uncle blocks any more
            let beacon_withdrawals = block.withdrawals;
            if let Some(beacon_withdrawals) = beacon_withdrawals {
                for withdrawal in beacon_withdrawals.iter() {
                    let withdrawal_address = withdrawal.address;
                    let withdrawal_amount: U256 =
                        parse_units(&withdrawal.amount.to_string(), "gwei")?.into();

                    // Update the state of the beacon withdrawal
                    let address_key_withdrawal = AddressKey::new(withdrawal_address);
                    self.score_db
                        .record_transfer(
                            txn,
                            cache,
                            provider,
                            block_number,
                            None,
                            &address_key_withdrawal,
                            withdrawal_amount,
                            None,
                        )
                        .await?;
                }
            }
        }

        Ok(())
    }

    /// Print the number of records in each database
    pub fn print_record_number(&self) {
        let txn = self.db.read_transaction();
        self.score_db.print_record_number(&txn);
    }

    /// Export the latest address score to CSV
    pub async fn store_latest_address_score_into_postgresql(
        &self,
        client: &PostgresClient,
    ) -> Result<()> {
        let txn = self.db.read_transaction();
        self.score_db
            .store_latest_address_score_into_postgresql(&txn, client)
            .await?;
        Ok(())
    }

    /// Export historical address scores to CSV
    pub fn export_address_historical_score_between_block_range(
        &self,
        address: Address,
        from_block: u64,
        to_block: u64,
    ) -> Result<()> {
        let address = AddressKey::new(address);
        let txn = self.db.read_transaction();
        self.score_db
            .export_address_historical_score_between_block_range(
                &txn, &address, from_block, to_block,
            )?;
        Ok(())
    }

    /// Export the number of tainted addresses in each block
    pub fn export_historical_amount_of_tainted_addresses(&self) -> Result<()> {
        let txn = self.db.read_transaction();
        self.score_db
            .export_historical_amount_of_tainted_addresses(&txn)
    }

    /// Get tainted addresses until the given block
    pub fn export_tainted_addresses_until_block_number(
        &self,
        block_number: u64,
    ) -> Result<BTreeMap<Address, Score>> {
        let txn = self.db.read_transaction();
        self.score_db
            .export_tainted_addresses_until_block_number(&txn, block_number)
    }

    pub async fn export_address_score_between_block_range<'a>(
        &self,
        conn: &PooledConnection<'a, PostgresConnectionManager<NoTls>>,
        from_block: u64,
        to_block: u64,
    ) -> Result<()> {
        let txn = self.db.read_transaction();
        self.score_db
            .export_address_score_between_block_range(&txn, conn, from_block, to_block)
            .await
    }

    pub async fn get_address_latest_score(
        &self,
        provider: &RootProvider<PubSubFrontend>,
        address: Address,
    ) -> Result<Score> {
        let address = AddressKey::new(address);
        let txn = self.db.read_transaction();
        self.score_db
            .get_address_latest_score(&txn, provider, &address)
            .await
    }

    /// Get the score of the given address in the given block
    pub async fn get_address_score_by_block_number(
        &self,
        provider: &RootProvider<PubSubFrontend>,
        address: Address,
        block_number: u64,
    ) -> Result<Score> {
        let address = AddressKey::new(address);
        let txn = self.db.read_transaction();
        self.score_db
            .get_address_score_by_block_number(&txn, provider, &address, block_number)
            .await
    }

    /// Get the maximum dirty amount of the given address
    pub fn get_address_max_dirty_amount(&self, address: Address) -> Result<Score> {
        let address = AddressKey::new(address);
        let txn = self.db.read_transaction();
        self.score_db.get_address_max_dirty_amount(&txn, &address)
    }
}
