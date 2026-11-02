use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read, Write};
use crate::db::{AssetBalance, AssetInfo,AssetSymbol};

// Import your Order struct from your command or market module
// use super::command::Order; 
use crate::models::Order;

#[derive(Serialize, Deserialize, Debug)]
pub struct ExchangeSnapshot {
 #[serde(alias = "sequence_id", default)]
    pub seq: u64,

    #[serde(default)]
    pub markets: HashMap<String, MarketSnapshotData>,
    #[serde(default)]
    pub user_balances: HashMap<u32, HashMap<AssetSymbol, AssetBalance>>, // 👈 ADD THIS: user_id -> { asset -> balance }
}


#[derive(Serialize, Deserialize, Debug,)]
pub struct MarketSnapshotData {
    pub bids: Vec<Order>,
    pub asks: Vec<Order>,
}

impl ExchangeSnapshot {
    pub fn save(path: &str, snapshot: &ExchangeSnapshot) -> io::Result<()> {
        let data = serde_json::to_string(snapshot)?;
        let mut file = File::create(path)?;
        file.write_all(data.as_bytes())?;
        Ok(())
    }

    pub fn load(path: &str) -> io::Result<Option<ExchangeSnapshot>> {
        if !std::path::Path::new(path).exists() {
            return Ok(None);
        }

        let mut file = File::open(path)?;
        let mut contents = String::new();  //create an variable which save values as string
        file.read_to_string(&mut contents)?;
        let snapshot: ExchangeSnapshot = serde_json::from_str(&contents)?;
        Ok(Some(snapshot))
    }
}