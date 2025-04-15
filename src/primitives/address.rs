use alloy::primitives::Address;
use nimiq_database_value::{AsDatabaseBytes, FromDatabaseBytes};
use std::{borrow::Cow, ops::Deref};

/// A wrapper around alloy's `Address` so we can implement our own traits.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct AddressKey(Address);

impl AddressKey {
    pub fn new(raw_address: Address) -> Self {
        Self(raw_address)
    }
}

impl Deref for AddressKey {
    type Target = Address;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsDatabaseBytes for AddressKey {
    fn as_key_bytes(&self) -> Cow<[u8]> {
        Cow::Borrowed(self.0.as_ref())
    }
    const FIXED_SIZE: Option<usize> = Some(20);
}

impl FromDatabaseBytes for AddressKey {
    fn from_key_bytes(bytes: &[u8]) -> Self
    where
        Self: Sized,
    {
        Self(Address::try_from(bytes).expect("Invalid address format"))
    }
}
