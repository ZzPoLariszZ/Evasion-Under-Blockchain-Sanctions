use alloy::{
    primitives::{Address, U256},
    providers::{Provider, RootProvider},
    pubsub::PubSubFrontend,
};
use bb8::PooledConnection;
use bb8_postgres::PostgresConnectionManager;
use csv::Writer;
use eyre::Result;
use nimiq_database::{
    declare_table,
    mdbx::{MdbxDatabase, MdbxReadTransaction, MdbxWriteTransaction},
    traits::{Database, DupReadCursor, ReadCursor, ReadTransaction, WriteTransaction},
    utils::IndexedValue,
};
use rust_decimal::prelude::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
};
use tokio_postgres::{Client as PostgresClient, NoTls};

use crate::{
    cache::Cache,
    constant::{TC_ETH_ADDRESS, BYBIT_EXPLOITER_ADDRESS},
    primitives::{AddressKey, Score},
};

// Current score.
declare_table!(AddressScoreTable, "address_score", AddressKey => Score);
// Score at given block *after* block transition.
declare_table!(BlockSnapshotTable, "block_snapshots", u64 => AddressKey => Score);
// Blocks at which the address has changed.
declare_table!(AddressHistoryTable, "address_history", AddressKey => dup(u64));

pub struct ScoreDb;

impl ScoreDb {
    pub fn new(db: MdbxDatabase) -> Self {
        db.create_regular_table(&AddressScoreTable);
        db.create_dup_table(&BlockSnapshotTable);
        db.create_dup_table(&AddressHistoryTable);
        Self
    }

    /// Initializes the TC contracts as fully dirty.
    pub async fn init_tc(
        &self,
        cache: &Cache,
        provider: &RootProvider<PubSubFrontend>,
        block_number: u64,
    ) -> Result<()> {
        for address in TC_ETH_ADDRESS.iter() {
            let balance = provider
                .get_balance(*address)
                .block_id(block_number.into())
                .await?;
            cache.insert_data(AddressKey::new(*address), Score::new_dirty(balance));
        }
        Ok(())
    }

    /// Clears all tables.
    pub fn clear(&self, txn: &mut MdbxWriteTransaction) {
        txn.clear_table(&AddressScoreTable);
        txn.clear_table(&BlockSnapshotTable);
        txn.clear_table(&AddressHistoryTable);
    }

    /// Gets the block number from the last time running
    pub fn get_last_block_number(&self, txn: &MdbxReadTransaction) -> Option<u64> {
        let mut cursor = txn.dup_cursor(&BlockSnapshotTable);
        cursor.last().map(|(k, _)| k)
    }

    /// Updates database after going through the entire block.
    pub async fn flush_cache<'a>(
        &self,
        txn: &mut MdbxWriteTransaction<'a>,
        provider: &RootProvider<PubSubFrontend>,
        cache: &Cache,
        block_number: u64,
    ) -> Result<()> {
        for (address, score) in cache.drain_data() {
            // // Check whether the calculated balance of the address at the given block number is correct or not
            // let balance = provider
            //     .get_balance(*address)
            //     .block_id((block_number).into())
            //     .await?;
            // // Panics if the calculated balance is not correct
            // assert_eq!(
            //     score.balance, balance,
            //     "Balance mismatch for address {} at block {}",
            //     **address, block_number
            // );

            // Check whether the address is stored in permanent table or not
            match txn.get(&AddressScoreTable, &address) {
                // `AddressScoreTable` only stores unclean addresses.
                // `AddressHistoryTable` and `BlockSnapshotTable` store every change involving unclean addresses,
                // which include:
                // - situation-1: addresses that were unclean previously,
                // - situation-2: previously clean addresses that became unclean,
                // such that only addresses that were clean and did not become unclean are omitted.
                Some(_) => {
                    if score.is_dirty() {
                        txn.put(&AddressScoreTable, &address, &score);
                    } else {
                        txn.remove(&AddressScoreTable, &address);
                    }
                    // Situation-1: addresses that were unclean previously
                    txn.put(&AddressHistoryTable, &address, &block_number);
                    txn.put(
                        &BlockSnapshotTable,
                        &block_number,
                        &IndexedValue::new(address.clone(), score),
                    );
                }
                None => {
                    if score.is_dirty() {
                        txn.put(&AddressScoreTable, &address, &score);
                        // Situation-2: previously clean addresses that became unclean
                        txn.put(&AddressHistoryTable, &address, &block_number);
                        txn.put(
                            &BlockSnapshotTable,
                            &block_number,
                            &IndexedValue::new(address.clone(), score),
                        );
                    }
                }
            }
        }
        Ok(())
    }

    /// Tries to get a previous score from cache and database.
    fn get_score(
        &self,
        txn: &MdbxReadTransaction,
        cache: &Cache,
        address: &AddressKey,
    ) -> Option<Score> {
        cache
            .get_data(address)
            .or_else(|| txn.get(&AddressScoreTable, address))
    }

    /// Subtracts a given amount from an address and returns the score to be added to the recipient.
    async fn update_sender_state<'a>(
        &self,
        txn: &mut MdbxWriteTransaction<'a>,
        cache: &Cache,
        provider: &RootProvider<PubSubFrontend>,
        block_number: u64,
        address: &AddressKey,
        transfer_value: U256,
    ) -> Result<Score> {
        // Retrieve sender's current score and calculate transfer ETH's score.
        let (sender_score, transfer_score) = match self.get_score(txn, cache, address) {
            Some(score) => {
                // We already have a previous score, so we calculate the transfer score based on this
                let transfer_score = Score::with_same_uncleanliness_ceil(transfer_value, &score);
                (score, transfer_score)
            }
            None => {
                // We don't have a previous score, so the balance and transfer value is totally clean
                let balance = provider
                    .get_balance(**address)
                    .block_id((block_number - 1).into())
                    .await?;
                let score = Score::new_clean(balance);
                let transfer_score = Score::new_clean(transfer_value);
                (score, transfer_score)
            }
        };
        // Panics if `transfer value > the balance sender holds``
        assert!(
            transfer_score.balance <= sender_score.balance,
            "Cannot send more than available ({} <= {})",
            transfer_score.balance,
            sender_score.balance
        );
        // Cache the score of every occurred address in the block.
        let score_post = sender_score - transfer_score;
        cache.insert_data(address.clone(), score_post);
        Ok(transfer_score)
    }

    /// Adds the transfer score to the recipient.
    async fn update_recipient_state<'a>(
        &self,
        txn: &mut MdbxWriteTransaction<'a>,
        cache: &Cache,
        provider: &RootProvider<PubSubFrontend>,
        block_number: u64,
        address: &AddressKey,
        mut transfer_score: Score,
    ) -> Result<()> {
        // If depositing into a TC contract, mark the full balance as dirty.
        if TC_ETH_ADDRESS.iter().any(|tc_addr| *tc_addr == **address) {
            transfer_score = transfer_score.as_dirty();
        }

        // Retrieve recipient's current score.
        let recipient_score = match self.get_score(txn, cache, address) {
            // We already have a previous score
            Some(score) => score,
            None => {
                // We don't have a previous score, so the balance is totally clean
                let balance = provider
                    .get_balance(**address)
                    .block_id((block_number - 1).into())
                    .await?;
                Score::new_clean(balance)
            }
        };

        // Cache the score of every occurred address in the block.
        let score_post = recipient_score + transfer_score;
        cache.insert_data(address.clone(), score_post);
        Ok(())
    }

    /// Records a transfer.
    /// The `sender_address` can be `None` for coinbase transactions.
    /// If no `recipient_value` is given, it is assumed to be the same as the `sender_value` (normal case).
    /// They only differ if parts are burned when considering the transaction fee.
    #[allow(clippy::too_many_arguments)]
    pub async fn record_transfer<'a>(
        &self,
        txn: &mut MdbxWriteTransaction<'a>,
        cache: &Cache,
        provider: &RootProvider<PubSubFrontend>,
        block_number: u64,
        sender_address: Option<&AddressKey>,
        recipient_address: &AddressKey,
        sender_value: U256,
        recipient_value: Option<U256>,
    ) -> Result<()> {
        // Update sender, otherwise assume clean amount.
        let mut transfer_score = if let Some(sender_address) = sender_address {
            self.update_sender_state(
                txn,
                cache,
                provider,
                block_number,
                sender_address,
                sender_value,
            )
            .await?
        } else {
            Score::new_clean(sender_value)
        };

        // Recalculate transfer value on recipient side if `recipient_value` is given.
        if let Some(recipient_value) = recipient_value {
            assert!(
                recipient_value <= sender_value,
                "Cannot receive more than sent ({} <= {})",
                recipient_value,
                sender_value
            );
            transfer_score = Score::with_same_uncleanliness_ceil(recipient_value, &transfer_score);
        }

        // Update recipient.
        self.update_recipient_state(
            txn,
            cache,
            provider,
            block_number,
            recipient_address,
            transfer_score,
        )
        .await?;

        Ok(())
    }

    /// Print the number of records in each database
    pub fn print_record_number(&self, txn: &MdbxReadTransaction) {
        let cursor_address_score = ReadTransaction::cursor(txn, &AddressScoreTable);
        let count_address_score = cursor_address_score.into_iter_start().count();
        println!(
            "The number of records in the AddressScoreTable is {}",
            count_address_score
        );
        let dup_cursor_block_snapshot = ReadTransaction::dup_cursor(txn, &BlockSnapshotTable);
        let count_block_snapshot = dup_cursor_block_snapshot.into_iter_start().count();
        println!(
            "The number of records in the BlockSnapshotTable is {}",
            count_block_snapshot
        );
        let dup_cursor_address_history = ReadTransaction::dup_cursor(txn, &AddressHistoryTable);
        let count_address_history = dup_cursor_address_history.into_iter_start().count();
        println!(
            "The number of records in the AddressHistoryTable is {}",
            count_address_history
        );
    }

    /// Export the latest address score to CSV
    pub async fn store_latest_address_score_into_postgresql<'a>(
        &self,
        txn: &MdbxReadTransaction<'a>,
        client: &PostgresClient,
    ) -> Result<()> {
        let cursor = ReadTransaction::cursor(txn, &AddressScoreTable);
        for (address, score) in cursor.into_iter_start() {
            client
                .execute(
                    "
                INSERT INTO latest_address_score_bybit (address, total_balance, dirty_amount)
                VALUES ($1, $2, $3)
                ON CONFLICT (address) DO NOTHING;",
                    &[
                        &address.to_string(),
                        &Decimal::from_str_exact(&score.balance.to_string())
                            .expect("Invalid total balance"),
                        &Decimal::from_str_exact(&score.dirty_amount.to_string())
                            .expect("Invalid dirty amount"),
                    ],
                )
                .await?;
        }
        Ok(())
    }

    /// Export historical address scores to CSV
    pub fn export_address_historical_score_between_block_range(
        &self,
        txn: &MdbxReadTransaction,
        address: &AddressKey,
        from_block: u64,
        to_block: u64,
    ) -> Result<()> {
        let file = File::create(format!(
            "./output/output_historical_{}_between_{}_and_{}.csv",
            **address, from_block, to_block
        ))?;
        let mut wtr = Writer::from_writer(file);
        wtr.serialize(("address", "block_number", "total_balance", "dirty_amount"))?;
        let dup_cursor_address_history = ReadTransaction::dup_cursor(txn, &AddressHistoryTable);
        let mut dup_cursor_block_snap = ReadTransaction::dup_cursor(txn, &BlockSnapshotTable);
        let block_entire_history: Vec<u64> = dup_cursor_address_history
            .into_iter_dup_of(address)
            .map(|(_, v)| v)
            .collect();

        let start_idx = match block_entire_history.binary_search(&from_block) {
            Ok(idx) => idx,
            Err(idx) => idx,
        };
        let end_idx = match block_entire_history.binary_search(&to_block) {
            Ok(idx) => idx + 1,
            Err(idx) => idx,
        };

        for block_number in block_entire_history[start_idx..end_idx].iter() {
            let score = dup_cursor_block_snap
                .set_subkey(block_number, address)
                .unwrap()
                .value;
            wtr.serialize((**address, block_number, score.balance, score.dirty_amount))?;
        }
        wtr.flush()?;
        Ok(())
    }

    /// Export the number of tainted addresses in each block
    pub fn export_historical_amount_of_tainted_addresses(
        &self,
        txn: &MdbxReadTransaction,
    ) -> Result<()> {
        let file = File::create("./output/output_historical_amount_of_tainted_addresses.csv")?;
        let mut wtr = Writer::from_writer(file);
        wtr.serialize(("block_number", "address_amount"))?;

        let mut prev_key = None;
        let mut address_set: BTreeSet<Address> = BTreeSet::new();
        let dup_cursor_block_snap = ReadTransaction::dup_cursor(txn, &BlockSnapshotTable);
        for (k, v) in dup_cursor_block_snap.into_iter_start() {
            // Write the length of the address_set when the key changes
            if prev_key.is_some() && prev_key != Some(k) {
                wtr.serialize((prev_key.unwrap(), address_set.len()))?;
            }
            address_set.insert(*v.index);
            prev_key = Some(k);
        }

        // Export the last block
        if let Some(last_key) = prev_key {
            wtr.serialize((last_key, address_set.len()))?;
        }
        wtr.flush()?;

        Ok(())
    }

    /// Export tainted addresses until the given block
    pub fn export_tainted_addresses_until_block_number(
        &self,
        txn: &MdbxReadTransaction,
        block_number: u64,
    ) -> Result<BTreeMap<Address, Score>> {
        let mut address_score: BTreeMap<Address, Score> = BTreeMap::new();
        let file = File::create(format!(
            "./output/output_tainted_addresses_until_{}.csv",
            block_number
        ))?;
        let dup_cursor_block_snap = ReadTransaction::dup_cursor(txn, &BlockSnapshotTable);
        for (k, v) in dup_cursor_block_snap.into_iter_start() {
            if k > block_number {
                break;
            } else {
                address_score.insert(*v.index, v.value);
            }
        }
        let mut wtr = Writer::from_writer(file);
        wtr.serialize(("address", "total_balance", "dirty_amount"))?;
        for (address, score) in address_score.iter() {
            if score.is_dirty() {
                wtr.serialize((*address, score.balance, score.dirty_amount))?;
            }
        }
        wtr.flush()?;
        Ok(address_score)
    }

    /// Export the address score between the given block range
    pub async fn export_address_score_between_block_range<'a>(
        &self,
        txn: &MdbxReadTransaction<'a>,
        conn: &PooledConnection<'a, PostgresConnectionManager<NoTls>>,
        from_block: u64,
        to_block: u64,
    ) -> Result<()> {
        let dup_cursor_block_snap = ReadTransaction::dup_cursor(txn, &BlockSnapshotTable);
        for (block_number, address_with_score) in dup_cursor_block_snap.into_iter_from(&from_block)
        {
            if block_number > to_block {
                break;
            }
            let address = address_with_score.index;
            let total_balance = address_with_score.value.balance;
            let dirty_amount = address_with_score.value.dirty_amount;
            conn.execute(
                "
                INSERT INTO block_snapshot (block_number, address, total_balance, dirty_amount)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (block_number, address) DO NOTHING;",
                &[
                    &(block_number as i64),
                    &address.to_string(),
                    &Decimal::from_str_exact(&total_balance.to_string())
                        .expect("Invalid total balance"),
                    &Decimal::from_str_exact(&dirty_amount.to_string())
                        .expect("Invalid dirty amount"),
                ],
            )
            .await?;
        }
        Ok(())
    }

    pub async fn get_address_latest_score<'a>(
        &self,
        txn: &MdbxReadTransaction<'a>,
        provider: &RootProvider<PubSubFrontend>,
        address: &AddressKey,
    ) -> Result<Score> {
        match txn.get(&AddressScoreTable, address) {
            Some(score) => Ok(score),
            None => {
                let balance = provider
                    .get_balance(**address)
                    .block_id(20305757.into())
                    .await?;
                Ok(Score::new_clean(balance))
            }
        }
    }

    /// Get the score of the given address in the given block
    pub async fn get_address_score_by_block_number<'a>(
        &self,
        txn: &MdbxReadTransaction<'a>,
        provider: &RootProvider<PubSubFrontend>,
        address: &AddressKey,
        block_number: u64,
    ) -> Result<Score> {
        let dup_cursor_address_history = ReadTransaction::dup_cursor(txn, &AddressHistoryTable);
        let mut dup_cursor_block_snap = ReadTransaction::dup_cursor(txn, &BlockSnapshotTable);
        let block_entire_history: Vec<u64> = dup_cursor_address_history
            .into_iter_dup_of(address)
            .map(|(_, v)| v)
            .collect();
        // println!("The block_entire_history is {:?}", block_entire_history);
        let closest_previous_block_number = match block_entire_history.binary_search(&block_number)
        {
            Ok(idx) => block_entire_history[idx],
            Err(idx) => {
                if idx == 0 {
                    block_number
                } else {
                    block_entire_history[idx - 1]
                }
            }
        };
        // println!("The block number is {}", closest_previous_block_number);
        let score = match dup_cursor_block_snap.set_subkey(&closest_previous_block_number, address)
        {
            Some(result) => {
                let score = result.value;
                if score.balance != U256::ZERO {
                    score
                } else {
                    let balance = provider
                        .get_balance(**address)
                        .block_id(block_number.into())
                        .await?;
                    Score::new_clean(balance)
                }
            }
            None => {
                let balance = provider
                    .get_balance(**address)
                    .block_id(block_number.into())
                    .await?;
                Score::new_clean(balance)
            }
        };
        Ok(score)
    }

    /// Get the maximum dirty amount of the given address
    pub fn get_address_max_dirty_amount(
        &self,
        txn: &MdbxReadTransaction,
        address: &AddressKey,
    ) -> Result<Score> {
        let dup_cursor_address_history = ReadTransaction::dup_cursor(txn, &AddressHistoryTable);
        let mut dup_cursor_block_snap = ReadTransaction::dup_cursor(txn, &BlockSnapshotTable);
        let block_entire_history: Vec<u64> = dup_cursor_address_history
            .into_iter_dup_of(address)
            .map(|(_, v)| v)
            .collect();
        let mut score_with_max_dirty_amount = Score::new_clean(U256::ZERO);
        for block_number in block_entire_history {
            let score = dup_cursor_block_snap
                .set_subkey(&block_number, address)
                .unwrap()
                .value;
            if score.dirty_amount > score_with_max_dirty_amount.dirty_amount {
                score_with_max_dirty_amount = score;
            }
        }

        Ok(score_with_max_dirty_amount)
    }
}
