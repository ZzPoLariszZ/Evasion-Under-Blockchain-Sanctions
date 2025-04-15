use std::ops::{Add, AddAssign, Sub, SubAssign};

use alloy::primitives::{U256, U512};
use nimiq_database_value_derive::DbSerializable;
use nimiq_serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, DbSerializable)]
pub struct Score {
    pub balance: U256,
    pub dirty_amount: U256,
}

impl Score {
    /// Manually set score.
    /// Panics if `dirty_amount > balance`
    pub fn new(balance: U256, dirty_amount: U256) -> Self {
        assert!(
            dirty_amount <= balance,
            "Dirty amount must be <= balance ({} < {})",
            dirty_amount,
            balance
        );
        Self {
            balance,
            dirty_amount,
        }
    }

    /// Creates a clean score.
    pub fn new_clean(balance: U256) -> Self {
        Self::new(balance, U256::ZERO)
    }

    /// Creates a fully dirty score.
    pub fn new_dirty(balance: U256) -> Self {
        Self::new(balance, balance)
    }

    /// Creates a new score with a given `balance`
    /// that has the same uncleanliness as the given `proportion`.
    /// As a design choice, we use a ceiling division here.
    /// This will have the effect that we might overestimate the uncleanliness
    /// slightly (by a fraction of a coin).
    pub fn with_same_uncleanliness_ceil(balance: U256, proportion: &Self) -> Self {
        let dirty_amount = (U512::from(balance) * U512::from(proportion.dirty_amount))
            .div_ceil(U512::from(proportion.balance));
        Self::new(balance, U256::from(dirty_amount))
    }

    /// Returns a fully dirty score with the same balance.
    pub fn as_dirty(&self) -> Self {
        Self {
            balance: self.balance,
            dirty_amount: self.balance,
        }
    }

    /// Returns `true` if the address has some dirty amount.
    pub fn is_dirty(&self) -> bool {
        !self.dirty_amount.is_zero()
    }
}

macro_rules! impl_op {
    ($trait:ident, $fn:ident, $trait_assign:ident, $fn_assign:ident) => {
        impl $trait<&Score> for Score {
            type Output = Score;

            fn $fn(self, rhs: &Score) -> Self::Output {
                Self {
                    balance: self.balance.$fn(rhs.balance),
                    dirty_amount: self.dirty_amount.$fn(rhs.dirty_amount),
                }
            }
        }

        impl $trait for Score {
            type Output = Score;

            fn $fn(self, rhs: Score) -> Self::Output {
                self.$fn(&rhs)
            }
        }

        impl $trait_assign<&Score> for Score {
            fn $fn_assign(&mut self, rhs: &Score) {
                *self = self.$fn(rhs)
            }
        }

        impl $trait_assign for Score {
            fn $fn_assign(&mut self, rhs: Score) {
                *self = self.$fn(rhs)
            }
        }
    };
}

impl_op!(Add, add, AddAssign, add_assign);
impl_op!(Sub, sub, SubAssign, sub_assign);
