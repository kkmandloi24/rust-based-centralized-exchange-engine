// src/engine/market.rs
use tokio::sync::mpsc;
use serde::{Serialize, Deserialize};
use crate::engine::{OrderBook};
use crate::models::Order;
use std::collections::HashMap;
use crate::ledger::BalanceCommand;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash,Serialize, Deserialize)]
pub enum MarketPair {
    SolUsdc,
    BtcUsdc,
    EthUsdc,
}


pub struct MatchingEngine {
    pub order_book: OrderBook,
    pub next_sequence_number: u64, // <-- Order sequence lives here! we use it to give order id
    pub active_orders:HashMap<u64,Order>
}

impl MatchingEngine {
    pub fn new() -> Self {
        Self {
            order_book: OrderBook::new(),
            next_sequence_number: 1, // Start sequence at 1
            active_orders:HashMap::new(),
        }
    }

   //we use belowfunction only at the time just after wal recover, to track last order_id so we can do it +1 and update the next sequence number 
   //so next id will be incremental excat 1 from the order just before crash the last order had
 pub fn recalculate_sequence_number(&mut self) {
        // Find the highest order ID currently sitting in this specific market's active orders
        let max_id = self.active_orders.keys().cloned().max().unwrap_or(0);
        
        // Set the next sequence to max + 1 (if empty, it starts at 1)
        self.next_sequence_number = max_id + 1;
    }



}



