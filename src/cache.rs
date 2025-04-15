use crate::primitives::{AddressKey, Score};
use parking_lot::Mutex;
use std::{collections::BTreeMap, mem, ops::DerefMut};

#[derive(Debug)]
pub struct Cache {
    pub data: Mutex<BTreeMap<AddressKey, Score>>,
    pub self_destruct: Mutex<Vec<AddressKey>>,
}

impl Default for Cache {
    /// Create a new cache.
    fn default() -> Self {
        Self {
            data: Mutex::new(BTreeMap::new()),
            self_destruct: Mutex::new(Vec::new()),
        }
    }
}

impl Cache {
    /// Create a new cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the score of the given address.
    pub fn get_data(&self, address: &AddressKey) -> Option<Score> {
        let cache_lock = self.data.lock();
        cache_lock.get(address).copied()
    }

    /// Insert a new address and its score.
    pub fn insert_data(&self, address: AddressKey, score: Score) {
        let mut cache_lock = self.data.lock();
        cache_lock.insert(address, score);
    }

    /// Drain the address and score from the cache and turn into a new collection.
    pub fn drain_data(&self) -> BTreeMap<AddressKey, Score> {
        let mut cache_lock = self.data.lock();
        mem::take(cache_lock.deref_mut())
    }

    /// Check if the address is self-destructed.
    pub fn check_self_destructed(&self, address: &AddressKey) -> bool {
        let cache_lock = self.self_destruct.lock();
        cache_lock.contains(address)
    }

    /// Insert a self-destruct address.
    pub fn insert_self_destruct(&self, address: AddressKey) {
        let mut cache_lock = self.self_destruct.lock();
        cache_lock.push(address);
    }

    /// Drain the self-destructed addresses from the cache and turn into a new collection.
    pub fn drain_self_destruct(&self) -> Vec<AddressKey> {
        let mut cache_lock = self.self_destruct.lock();
        cache_lock.drain(..).collect()
    }
}
