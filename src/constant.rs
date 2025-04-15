use alloy::primitives::{address, Address};

/// Addresses of TC pools.
pub const TC_ETH_ADDRESS: [Address; 4] = [
    address!("A160cdAB225685dA1d56aa342Ad8841c3b53f291"), // TC ETH 100
    address!("910Cbd523D972eb0a6f4cAe4618aD62622b39DbF"), // TC ETH 10
    address!("47CE0C6eD5B0Ce3d3A51fdb1C52DC66a7c3c2936"), // TC ETH 1
    address!("12D66f87A04A9E220743712cE6d9bB1B5616B8Fc"), // TC ETH 0.1
];

pub const BYBIT_EXPLOITER_ADDRESS: [Address; 2] = [
    address!("47666Fab8bd0Ac7003bce3f5C3585383F09486E2"),
    address!("A4B2Fd68593B6F34E51cB9eDB66E71c1B4Ab449e"),
];

/// Block that initialized
pub const INI_BLOCK_NUMBER_TC: u64 = 15302392;
pub const INI_BLOCK_NUMBER_BYBIT: u64 = 21895251;
/// Block that switched to POS.
pub const POS_BLOCK_NUMBER: u64 = 15537394;
/// Block that terminated
pub const END_BLOCK_NUMBER: u64 = 22097863;
