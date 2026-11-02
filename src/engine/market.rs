// src/engine/market.rs
use tokio::sync::{mpsc, oneshot};
use serde::{Serialize, Deserialize};
use crate::engine::{OrderBook};
use crate::models::{Order,Side,UserOrdersResponse,PublicOrderDto};
use std::any::Any;
use std::collections::HashMap;
use std::io;
use std::fs::File;
use std::io::{ Write, Read};
use crate::wal::{EngineCommand,Wal,ExchangeSnapshot,MarketSnapshotData,WallEntry};
use crate::BalanceCommand;


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

//Matches the order pair with the orderbook logic

pub struct ExchangeServer {
    pub engines: HashMap<MarketPair, MatchingEngine>,
    pub balance_sender: mpsc::Sender<BalanceCommand>, //<-- Added to talk to workers.rs

}


//New impl ExchangeServer{} , coe with truncate or removed wall after snapshot
impl ExchangeServer {
//initiating server , as: 
pub fn new(balance_sender:mpsc::Sender<BalanceCommand>) -> Self {
        let mut engines = HashMap::new();

        // Initialize your 3 default trading pairs
        engines.insert(MarketPair::SolUsdc, MatchingEngine::new());
        engines.insert(MarketPair::BtcUsdc, MatchingEngine::new());
        engines.insert(MarketPair::EthUsdc, MatchingEngine::new());

        Self { engines,balance_sender }
    }



//////////////////////////////////////////////

fn calculate_required_lock(order: &Order, market: &MarketPair) -> (String, f64) {

        let (quote_asset,base_asset)= match market{
            MarketPair::SolUsdc=>("USD","SOL"),
            MarketPair::BtcUsdc=>("USD","BTC"),
            MarketPair::EthUsdc=>("USD","ETH")

        };

    match order.side {

        //if side is buyer then then definitle the asset buyer use asset to place order is quote (usd)
        Side::Buy => {
            // Buying base asset with quote asset -> Lock quote asset (price * quantity)
            let quote_amount = order.price * order.quantity;
            (quote_asset.to_string(), quote_amount)
        }
           //if side is Seller will use base(SOl/BTC/ETH to create order) so base asset
        Side::Sell => {
            // Selling base asset -> Lock base asset (just the quantity) 
            (base_asset.to_string(), order.quantity)
        }
    }
}

////////////////////////////////////method with new wal///
pub async fn new_with_snapshot_and_recovery(
        snapshot_path: &str, 
        wal_path: &str,
        balance_sender: mpsc::Sender<BalanceCommand>,
    ) -> io::Result<(Self, Wal)> {
        
        // 1. Initialize the ExchangeServer instance
        // (Make sure to populate your engines HashMap with your supported markets, e.g., SolUsdc, BtcUsdc)
        // let mut server = Self {
        //     engines: HashMap::new(), 
        //     balance_sender,
        // };

     let mut server = Self::new(balance_sender);
          
        let mut last_seq = 0;

        // 2. Load the latest snapshot if it exists on disk
        if let Some(snapshot) = ExchangeSnapshot::load(snapshot_path)? {
            println!("Restored snapshot up to sequence ID: {}", snapshot.seq);
            
            //Rebuild the order books in RAM
            last_seq = snapshot.seq;
            let user_balances= snapshot.user_balances.clone(); 
                 server.restore_from_snapshot(snapshot); //orderbook restoring.

//Note: we updated oderbook by restore_from_snapshot  method but balance handler by the Balance workers
//so we used below command to restore it

let (resp_tx, resp_rx) = tokio::sync::oneshot::channel::<Result<(), String>>();

// Send snapshot's user balances to the background balance worker
server.balance_sender
    .send(BalanceCommand::RestoreSnapshot { 
        balances: user_balances,
        resp: resp_tx, // 👈 Include the oneshot sender here
    })
    .await
    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

// Await confirmation from the balance worker before proceeding
resp_rx.await
    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?
    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

println!("Snapshot user balances successfully sent and loaded by balance worker.");

        }


        ///////////////Now we chck the Wal for the Gap(might be snapshot taken at 10000 order that we set to take at every 1000 ordesnapsht)
        /// but at 15000 orders we got server crashed in that cash 10000 order and balances will recover from the snapshot and remaining 5000 from the wal
        /// and from wal we take out the commands one by one

        //Recovering from Gap (if we took snapshot at fix interval, then remaing capture from wal becuase wall contiue capture ordrbook and balances)
        // 3. Recover all WAL entries from disk
        let all_entries: Vec<WallEntry> = Wal::recover(wal_path)?;

        // 4. Filter WAL entries to only replay those *newer* than the snapshot baseline (The Gap)
        let gap_entries: Vec<WallEntry> = all_entries
            .into_iter()
            .filter(|entry| entry.seq > last_seq)
            .collect();

        println!("Replaying {} gap events from WAL...", gap_entries.len());

        // 5. Replay gap commands into matching engines and balance worker (RAM ONLY)
        for entry in gap_entries {
            match entry.command {
                EngineCommand::PlaceOrder { market, order } => {

                //we have calculate_required_lock function whoch return asset, and lock amount based on buy and sell side dynamically determines it
                let (asset_identifier, lock_amount)=Self::calculate_required_lock(&order, &market);
                 let user_id=order.user_id;

                    if let Some(engine) = server.engines.get_mut(&market) {
                        // 1. CRITICAL: Insert into active_orders so sequence tracking works!
                     engine.active_orders.insert(order.id, order.clone());
                        let _ = engine.order_book.add_order(order);


                    }

             //Balance Update only in-memory without ul
                   
                    let _= server.balance_sender.send(BalanceCommand::ReplayLockFunds{
                        user_id,
                        asset_identifier,
                        amount:lock_amount}).await;

                },
                EngineCommand::CancelOrder { market, id } => {

                    if let Some(engine) = server.engines.get_mut(&market) {
                    if let Some(order)=engine.active_orders.get(&id){

                let (asset_identifier, lock_amount)=Self::calculate_required_lock(&order, &market);
                 let user_id=order.user_id;

                 let _= server.balance_sender.send(BalanceCommand::ReplayUnlockFunds{
                        user_id,
                        asset_identifier,
                        amount:lock_amount}).await;

                        }
                        engine.active_orders.remove(&id);
                        engine.order_book.cancel_order(id);
                    }

                


                }

            EngineCommand::ModifyOrder { market, id, new_price, new_quantity } => {
                    // Handle order modification replay if applicable
               //////////////////////////////////////////////////////////////////
               /// 
        if let Some(engine) = server.engines.get_mut(&market) {

 //at the restarting the ystem after crash we have to add again the order on the orderbook, becuase
 //we are modifying the order on the orderbook,so first place it on the orderbook, and during the place order, weare inserting the order in the active_order hashmap, so we can get it, later we writing modifying order as: 
 // 1. SAFETY CHECK: If the order book was wiped by a snapshot and missing this order,
        // pull it from `active_orders` or re-add it so modification can succeed.
        // Check your engine's active_orders cache directly
        // Check engine's active_orders cache directly using standard HashMap methods
        if let Some(existing_order) = engine.active_orders.get(&id) {
            // Re-hydrate the order back into the order book if it was missed due to a snapshot boundary
            // (Replace `add_order` with whatever method your OrderBook uses to insert/add an order)
            let _ = engine.order_book.add_order(existing_order.clone());
        }
   
let modified_result = match engine.order_book.modify_order(id, new_price, new_quantity) {
    Ok(res) => res,
    Err(err_msg) => {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, err_msg));
    }
};

let (order, cost_diff, is_increase) = match modified_result {
    Some(res) => res,
    None => {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Order not found in order book"
        ));
    }
};


            let modified_order= order.clone();

            //if fund need to adjut then below code

            let asset_symbol = match order.side {
                Side::Buy => "USD".to_string(),
                Side::Sell => match market {
                    MarketPair::SolUsdc => "SOL".to_string(),
                    MarketPair::BtcUsdc => "BTC".to_string(),
                    MarketPair::EthUsdc => "ETH".to_string(),
                },
            };

            //  // Dispatch command to BalanceWorker via channel
            // let (resp_tx, resp_rx) = oneshot::channel();

          println!("REPLAY MODIFY DEBUG -> User: {}, Asset: {}, IsIncrease: {}, CostDiff: {}", 
    order.user_id, asset_symbol, is_increase, cost_diff);   
        if is_increase {
                // Lock additional funds
    
                 let _ = server.balance_sender.send(BalanceCommand::ReplayLockFunds {
                    user_id: order.user_id,
                    asset_identifier: asset_symbol.clone(),
                    amount: cost_diff,}).await;
                   
                
            } else {
                // Unlock excess funds
               
                   let _=  server.balance_sender.send(BalanceCommand::ReplayUnlockFunds {
                    user_id: order.user_id,
                    asset_identifier: asset_symbol.clone(),
                    amount: cost_diff,}).await;
                    
                };

                
            

            //replace the old order by this new modified order in the active_order as well
            engine.active_orders.insert(id,modified_order);

            };
        }

        

            ///////////////////////////////////////////////////////////////
            // 2. Coordinate balance lock adjustments with the Balance Worker
            //cost dif, calculation comes from the orderbook method modiffy order
       
            //////////////////////////////////////////////////////
        
               
            

               
               ///////////////////////////////////////////////////////////////////// 


                
                EngineCommand::Deposit { user_id, asset, amount } => {
                    //  Send REPLAY command: Updates RAM wallets only, skips Supabase!
                    let _ = server.balance_sender
                        .send(BalanceCommand::ReplayDeposit {
                            user_id,
                            asset_identifier: asset,
                            amount,
                        })
                        .await;
                }
                EngineCommand::Withdraw { user_id, asset, amount } => {
                    // 👈 Send REPLAY command: Updates RAM wallets only, skips Supabase!
                    let _ = server.balance_sender
                        .send(BalanceCommand::ReplayWithdraw {
                            user_id,
                            asset_identifier: asset,
                            amount,
                        })
                        .await;
                }
                EngineCommand::SettleTrade { 
                    trade_id, market, buyer_order_id, seller_order_id, 
                    buyer_id, seller_id, base_asset, quote_asset, 
                    trade_quantity, execution_price, buyer_limit_price 
                } => {
                    // 👈 Send REPLAY command: Updates RAM wallets only, skips Supabase!
                    let _ = server.balance_sender
                        .send(BalanceCommand::ReplaySettleTrade {
                            trade_id,
                            market,
                            buyer_order_id,
                            seller_order_id,
                            buyer_id,
                            seller_id,
                            base_asset,
                            quote_asset,
                            trade_quantity,
                            execution_price,
                            buyer_limit_price,
                        })
                        .await;
                }
            }
        }

        // 6. Open the WAL file in append mode for future live writes
        let wal = Wal::new(wal_path)?;
        println!("WAL successfully opened in append mode. Server recovery complete!");


   // ==========================================
    // 5. Whne restart system: RECALCULATE SEQUENCE NUMBERS FOR ALL MARKETS and updates their extsequnce number for orders
    // ==========================================

    //here running on the each engine the recalculation method and increasing the pointer next_sequnce_number by 1 for each market orders
        for (market_pair, engine) in server.engines.iter_mut() {
        engine.recalculate_sequence_number();
        println!("Market {:?} initialized with next sequence number: {}", market_pair, engine.next_sequence_number);
    }

        Ok((server, wal))
    }


/////////////////

    /// Create a snapshot and safely clear/truncate the old WAL file
    pub async fn create_snapshot(&self, snapshot_path: &str, wal_path: &str) -> io::Result<()> {
        let mut markets_data = HashMap::new();

        //order id definer inside the snapshot
        let mut max_seq = 0;

        //in the Engines we have 2 things marketpair, and matching engine
        //we named market pair market, and matchingEngine as engine
        for (market, engine) in &self.engines {

            //get updated the max_seq
            if engine.next_sequence_number > max_seq {
                max_seq = engine.next_sequence_number;
            }

            
            let bids = engine.order_book.get_bids(); 
            let asks = engine.order_book.get_asks();

            let market_key = format!("{:?}", market); //tunr marketpair into strings

            //insert in the mepty hashmap the entie engine ingredients
            markets_data.insert(
                market_key, 
                MarketSnapshotData { bids, asks },
            );
        }


        // 👈 1. ASK THE BALANCE WORKER FOR CURRENT BALANCES(taking balances from worker)
        let (tx, rx) = tokio::sync::oneshot::channel();
        
        //we send the message to worker function to take the netire balances hashmap of user(where Hashmap<userid, Hashmap<AssetSymbol,AssetBalance>>)
        //exist and we select user id and take his balance
        //for sending message we use workes command option GetSnapshot
        self.balance_sender
            .send(BalanceCommand::GetSnapshot { reply_to: tx })
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

            //and we get the users balance back from the worker function
        // 👈 2. AWAIT THE WORKER'S RESPONSE
        let current_balances = rx.await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        // 👈 3. INCLUDE BALANCES IN THE SNAPSHOT STRUCT
        let snapshot = ExchangeSnapshot {
            seq: max_seq,
            markets: markets_data,
            user_balances: current_balances, // Saved to disk!
        };

        //Then saving th snapshot in the file
        ExchangeSnapshot::save(snapshot_path, &snapshot)?;
        println!("Snapshot successfully created at sequence ID: {}", max_seq);

        // Clear/truncate the old WAL file
        let _fresh_wal = std::fs::File::create(wal_path)?;
        println!("Old WAL data cleared successfully. Starting fresh.");

        Ok(())
        // //making ExchangeSnapShot form engine+adding current balances
        // let snapshot = ExchangeSnapshot {
        //     sequence_id: max_seq,
        //     markets: markets_data,
        //     user_balances:current_balances.clone()  //in the snapshot we are saving the balances (Hashmap<user_di, Hashmap<sting, f64>>)
        // };

        // //creating snapshot
        // // 1. Save snapshot first (safety check)
        // ExchangeSnapshot::save(snapshot_path, &snapshot)?;
        // println!("Snapshot successfully created at sequence ID: {}", max_seq);

        // // 2. Clear/truncate the old WAL file since everything is now baked into the snapshot
        // let _fresh_wal = std::fs::File::create(wal_path)?;
        // println!("Old WAL data cleared successfully. Starting fresh.");

        // Ok(())
    }

    //We take data from snapshot to RAM. to restore the engine (market + orderbooks)

    /// Helper to restore engine state from a loaded snapshot
    fn restore_from_snapshot(&mut self, snapshot: ExchangeSnapshot) {
        for (market_str, market_data) in snapshot.markets {
            let market = match market_str.as_str() {
                "SolUsdc" => MarketPair::SolUsdc,
                "BtcUsdc" => MarketPair::BtcUsdc,
                "EthUsdc" => MarketPair::EthUsdc,
                _ => continue,
            };

            //we get engine rspect to the market
            //then update: next_sequence_number
            //

            //selected engine
            if let Some(engine) = self.engines.get_mut(&market) {
                
                //inserting the order for bids side and ask side 
                for order in market_data.bids {
                    //why we update next_sequnece_number= becuase when system starts it starts from 1, but in the snapshot we have last order id=10 restored
                    //when the order.id comes it is 10>=1 then it reupdate the next_sequence_number= 10+1
                    if order.id >= engine.next_sequence_number {
                        engine.next_sequence_number = order.id + 1;
                    }

                    //adding order to the ordebook
                    let _ = engine.order_book.add_order(order);
                }
                for order in market_data.asks {
                    if order.id >= engine.next_sequence_number {
                        engine.next_sequence_number = order.id + 1;
                    }
                    let _ = engine.order_book.add_order(order);
                }
            }
        }
    }


    /// Utility to find the highest active sequence number across all engines
    /// we iterate on the engine values
    fn get_max_sequence_id(&self) -> u64 {
        self.engines.values()
            .map(|engine| engine.next_sequence_number)
            .max()
            .unwrap_or(0)
    }

    //RESUMING ORDER COMMANDS FROM WAL AFTER THE SNAPSHOT (GAP ORDER COMMANDS),apply_restored_command
    /// Internal helper to rebuild state during startup without rewriting to WAL
//     fn apply_restored_command(&mut self, entry: WallEntry) {
//     let WallEntry { seq, command } = entry;
    
//     match command {
//         EngineCommand::PlaceOrder { market, order } => {
//             if let Some(engine) = self.engines.get_mut(&market) {
//                 // You can use `seq` directly from the WalEntry here if needed!
//                 if seq >= engine.next_sequence_number {
//                     engine.next_sequence_number = seq + 1;
//                 }
//                 let _trades = engine.order_book.add_order(order);
//             }
//         }
//         EngineCommand::CancelOrder { market, id } => {
//             if let Some(engine) = self.engines.get_mut(&market) {
//                 let _ = engine.order_book.cancel_order(id);
//             }
//         }
//         EngineCommand::ModifyOrder { market, id, new_price, new_quantity } => {
//             if let Some(engine) = self.engines.get_mut(&market) {
//                 let _ = engine.order_book.modify_order(id, new_price, new_quantity);
//             }
//         }
//     }
// }
/////////////////////////////////////////////////////
pub async fn place_order(&mut self, market: MarketPair, mut order: Order, wal: &mut Wal) -> io::Result<Result<(), String>> {
        let mut assigned_id = 0;
        let mut trades_to_settle = Vec::new();

        // 1. Critical section: Mutate engine state, assign ID, log to WAL, and match
        {
            let engine = match self.engines.get_mut(&market) {
                Some(eng) => eng,
                None => return Ok(Err("Market not found".to_string())),
            };

            order.id = engine.next_sequence_number;
            engine.next_sequence_number += 1;
            assigned_id = order.id;

            // Cache the order so we can lookup user_id and price during settlement
            engine.active_orders.insert(assigned_id, order.clone());

            //prepared ordr command
            let command = EngineCommand::PlaceOrder {
                market,
                order: order.clone(),
            };
            wal.append(&command)?;
            println!(" here comes order : {:?}", &command);

            let saved_order= order.clone();

            //Run matching engine //then we process the order in the orderbook method add order which retuns processed order,that we save in the trades vector
            let trades = engine.order_book.add_order(order);
            println!("Market {:?} processed order. Trades: {:?}", market, trades);

            // Collect trades along with needed order metadata for settlement
            //here we running loop on the trades(executed order), then select the buy/sell order from active order we saved using trade buyer/seller id
            //becuase before trade we have seller order and buyer order if those order excuted we have to settle them
            
            //we use buyer or seller id and pick the buyer or seller order that has beed executed
            for trade in trades {



           //Update Buyer order 

           // Safely fetch or fallback for Buyer Order
                let mut buyer_order = if trade.buyer_order_id == saved_order.id {
                    saved_order.clone()
                } else {
                    engine.active_orders.get(&trade.buyer_order_id)
                        .cloned()
                        .unwrap_or_else(|| saved_order.clone()) // Fallback if missing
                };

                // let mut buyer_order = engine.active_orders.get(&trade.buyer_order_id)
                //     .cloned()
                //     .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Buyer order metadata missing"))?;
                
                //check buyer order id
               buyer_order.remaining_quantity-=trade.quantity;
                let buyer_clone=buyer_order.clone();
                let buyer_fully_filled= buyer_order.remaining_quantity<=f64::EPSILON;

                //Update seller order
                // let mut seller_order = engine.active_orders.get(&trade.seller_order_id)
                //     .cloned()
                //     .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Seller order metadata missing"))?;

              // Safely fetch or fallback for Seller Order
                let mut seller_order = if trade.seller_order_id == saved_order.id {
                    saved_order.clone()
                } else {
                    engine.active_orders.get(&trade.seller_order_id)
                        .cloned()
                        .unwrap_or_else(|| saved_order.clone()) // Fallback if missing
                };


                seller_order.remaining_quantity-=trade.quantity;
                let seller_clone=seller_order.clone();
                let seller_fully_filled= seller_order.remaining_quantity<=f64::EPSILON;


                //now we have executed order which before execution was buyer order and seller order
                //sofor the settlement we need buyer order,seller order and executed order as well    
                trades_to_settle.push((trade.clone(), buyer_clone, seller_clone));

                //we remove the fully filled orders from the active_order Vectos

                if buyer_fully_filled{
                    engine.active_orders.remove(&trade.buyer_order_id);
                    
                    
                }

                if seller_fully_filled{
                    //remove the executed order from the active_order
                engine.active_orders.remove(&trade.seller_order_id);

                }
                
            }
        }

        // 2. Asynchronously settle trades with the balance worker (outside the engine lock block)
        for (trade, buyer_order, seller_order) in trades_to_settle {
            let (resp_tx, resp_rx) = oneshot::channel();


            // Determine base and quote assets dynamically based on MarketPair
            let (base_asset, quote_asset) = match market {
                MarketPair::SolUsdc => ("SOL", "USD"),
                MarketPair::BtcUsdc => ("BTC", "USD"),
                MarketPair::EthUsdc => ("ETH", "USD"),
            };



            let settle_cmd = BalanceCommand::SettleTrade {
                trade_id:trade.trade_id,
                market,
                buyer_order_id:buyer_order.id,
                seller_order_id:seller_order.id,
                buyer_id: buyer_order.user_id,
                seller_id: seller_order.user_id,
                base_asset: base_asset.to_string(),
                quote_asset: quote_asset.to_string(),
                trade_quantity: trade.quantity,
                execution_price: trade.price,
                buyer_limit_price: buyer_order.price,
                resp: resp_tx,
            };

            // Send command to workers.rs//here sender wecreated inside this fle sp not need to take it from global state
            //settlment mps sender we create in the struct Exchange server struct 
            self.balance_sender.send(settle_cmd).await
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "Balance worker channel closed"))?;

            // Await ledger settlement confirmation
            match resp_rx.await {
                Ok(Ok(_)) => {
                    println!("Successfully settled trade: {} {} @ {}", trade.quantity, base_asset, trade.price);
                }
                Ok(Err(err)) => {
                    eprintln!("Ledger settlement failed: {}", err);
                    return Ok(Err(err));
                }
                Err(_) => {
                    return Ok(Err("Balance worker dropped response channel".to_string()));
                }
            }
        }

        // 3. Optional automatic snapshot trigger
        if assigned_id % 10_000 == 0 {
            let wal_path = "exchange.wal";
            let snapshot_file = "exchange.snapshot";
            println!("Threshold reached! Triggering automatic snapshot...");
            self.create_snapshot(snapshot_file, wal_path).await?;
        }

        Ok(Ok(()))
    }

////////////////////////////////////////////////////////////////////
   

 //////// CANCEL ORDER //////////////////////////////



    pub async fn cancelling_order(
        &mut self,
        market: MarketPair,
        id: u64,
        user_id: u32,
        wal: &mut Wal,
    ) -> io::Result<Result<(), String>> {
       
        // 1).First take slect engine for market user sending from all markets

        // 1. Critical section: Mutate engine state, remove from OrderBook & active_orders cache
        {
            let engine = match self.engines.get_mut(&market) {
                Some(eng) => eng,
                None => return Ok(Err("Market not found".to_string())),
            };

            //2. Checking order existis, if exist belongs to user or not

            //1).check order exist or not
            let existing_order= match engine.active_orders.get(&id){
                Some(order)=>order,
                None=>return Ok(Err("Order not found already filled/canceled".to_string())),
            };

            //2).verify ownership
            if existing_order.user_id!=user_id{
            return Ok(Err("Unauthorized: You do not own this order".to_string()))
            };


            let order= existing_order.clone();

            //3).checking if order is not filled fully
            if existing_order.remaining_quantity<=0.0{
                engine.active_orders.remove(&id); // Clean up if it lingered
                return Ok(Err("Order is fully excuted or fully filled before".to_string()))

            }

            //////////////////////////////////
           // unlockfunds for cancel order


            // Determine asset symbol and amount to unlock
            let (asset_symbol, unlock_amount) = match order.side {
                Side::Buy => {
                    // For Buy orders, locked quote currency (e.g. USD) = remaining_quantity * limit_price
                    ("USD", order.remaining_quantity * order.price)
                }
                Side::Sell => {
                    // For Sell orders, locked base currency = remaining_quantity
                    let base = match market {
                        MarketPair::SolUsdc => "SOL",
                        MarketPair::BtcUsdc => "BTC",
                        MarketPair::EthUsdc => "ETH",
                    };
                    (base, order.remaining_quantity)
                }
            };


            //sending the command message in the oneshot channel

            let (resp_tx, resp_rx) = oneshot::channel();

            //prepared command
            let unlock_cmd = BalanceCommand::UnlockFunds {
                user_id: order.user_id,
                asset_identifier:asset_symbol.to_string(),
                amount: unlock_amount,
                resp: resp_tx,
            };
//taking the 
            // Send command across channel to BalanceWorker
            self.balance_sender
                .send(unlock_cmd)
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "Balance worker channel closed"))?;

            // Await confirmation from worker
            match resp_rx.await {
                Ok(Ok(_)) => {
                    println!("Successfully unlocked {} {} for cancelled order #{}", unlock_amount, asset_symbol, id);
                },
                Ok(Err(err)) => {
                    eprintln!("Ledger unlock failed: {}", err);
                    return Ok(Err(err));
                }
                Err(_) => {
                    return Ok(Err("Balance worker dropped response channel".to_string()));
                }
            }




            ///////////////////////////////////////////

                engine.active_orders.remove(&id);
                let command = EngineCommand::CancelOrder { market, id };
                engine.order_book.cancel_order(id);
                wal.append(&command)?;

        

        Ok(Ok(()))
        }
    }

//////////////////////////////////////////////////////////////////
pub async fn modifying_order(
        &mut self,
        market: MarketPair,
        id: u64,
        new_price: f64,
        new_quantity: f64,
        user_id: u32,
        wal: &mut Wal,
    ) -> io::Result<Result<(), String>> {
        let mut modified_result = None;

        // 1. Critical section: Validate ownership and invoke orderbook modify method
        {
            let engine = match self.engines.get_mut(&market) {
                Some(eng) => eng,
                None => return Ok(Err("Market not found".to_string())),
            };

            //1). validating the if order exist or not
            let existing_order= match engine.active_orders.get(&id){
                Some(order)=>order,
                None=>return Ok(Err("order not exist either completly filled or got canceled".to_string()))
            };

            //2). Validating the user own this order or not
        if  existing_order.user_id!=user_id{
            return Ok(Err("you do not own the order".to_string()))
        }

         //3).checking if order is not filled fully
            if existing_order.remaining_quantity<=0.0{
                engine.active_orders.remove(&id); // Clean up if it lingered
                return Ok(Err("Order is fully excuted or fully filled before".to_string()))
            }

            /////////////////////////////////////////////////////////////
            // Invoke OrderBook modify method
            match engine.order_book.modify_order(id, new_price, new_quantity) {
                Ok(Some(res)) => {
                    modified_result = Some(res); // (modified_order, cost_diff, is_increase)
                }
                Ok(None) => {
                    return Ok(Err("Order not found in order book".to_string()));
                }
                Err(err_msg) => {
                    return Ok(Err(err_msg));
                }
            }

            //INSERTING THE ORDER IN THE ACTIVE ORDER 
            //If modification succeeded in orderbook, update active_orders cache and WAL
            if let Some((ref updated_order, _, _)) = modified_result {
                engine.active_orders.insert(id, updated_order.clone());

                let command = EngineCommand::ModifyOrder {
                    market,
                    id,
                    new_price,
                    new_quantity,
                };
                wal.append(&command)?;
            }
        }

        // 2. Coordinate balance lock adjustments with the Balance Worker
        if let Some((order, cost_diff, is_increase)) = modified_result {
            if cost_diff <= 0.0 {
                return Ok(Ok(())); // No fund adjustment needed
            }

            let asset_symbol = match order.side {
                Side::Buy => "USD".to_string(),
                Side::Sell => match market {
                    MarketPair::SolUsdc => "SOL".to_string(),
                    MarketPair::BtcUsdc => "BTC".to_string(),
                    MarketPair::EthUsdc => "ETH".to_string(),
                },
            };

             // Dispatch command to BalanceWorker via channel
            let (resp_tx, resp_rx) = oneshot::channel();

            let balance_cmd = if is_increase {
                // Lock additional funds
                BalanceCommand::LockFunds {
                    user_id: order.user_id,
                    asset_identifier: asset_symbol.clone(),
                    amount: cost_diff,
                    resp: resp_tx, // or proper oneshot channel setup
                }
            } else {
                // Unlock excess funds
                BalanceCommand::UnlockFunds {
                    user_id: order.user_id,
                    asset_identifier: asset_symbol.clone(),
                    amount: cost_diff,
                    resp: resp_tx,
                }
            };

            let mut final_cmd = balance_cmd;
            // set resp_tx properly in balance_cmd depending on struct definition
            
            // (Assuming standard channel execution pattern as used in cancel)
            // let _ = resp_tx; // placeholder for channel send

 self.balance_sender
                .send(final_cmd)
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "Balance worker channel closed"))?;


        }

        Ok(Ok(()))
    }

////////////////////////////////////////////////////////////////////////////////////////

pub async fn getallorders(&self, user_id:u32)->Result<Vec<PublicOrderDto>,String>{
    //we do not get the market, we just send the user id
 let mut user_orders= Vec::new();

//     pub struct MatchingEngine {
//     pub order_book: OrderBook,
//     pub next_sequence_number: u64, // <-- Order sequence lives here! we use it to give order id
//     pub active_orders:HashMap<u64,Order>
// }

   for engine in self.engines.values(){

    for order in engine.active_orders.values(){
        if order.user_id==user_id{
            user_orders.push(PublicOrderDto{
                id:order.id,
                price:order.price,
                quantity:order.quantity,
                remaining_quantity: order.remaining_quantity,
                side: format!("{:?}",order.side)

            })
        }
    }

   }

   Ok(user_orders)

}

// async fn getorder(&mut self, id:u32, user_id:u32)->Result<Order,String>{

//     for engine in self.engine.active


// }


}
 


//  pub async fn cancel_order(&mut self, market: MarketPair, order_id: u64, wal: &mut Wal) -> io::Result<Result<(), String>> {
//         let mut cancelled_order = None;

//         // 1. Critical Section: Mutate engine state, remove from book, and log to WAL
//         {
//             let engine = match self.engines.get_mut(&market) {
//                 Some(eng) => eng,
//                 None => return Ok(Err("Market not found".to_string())),
//             };

//             // Remove from the matching engine's order book
//             cancelled_order = engine.order_book.cancel_order(order_id);

//             if let Some(ref order) = cancelled_order {
//                 // Remove from active orders cache
//                 engine.active_orders.remove(&order_id);

//                 // Append cancellation to WAL for crash recovery durability
//                 let command = EngineCommand::CancelOrder { market, id };
//                 wal.append(&command)?;
//             }
//         }

//         // 2. If found, calculate remaining locked funds and dispatch unlock command to the balance worker
//         if let Some(order) = cancelled_order {
//             let remaining_qty = order.remaining_quantity;
            
//             // If the order was completely filled before cancellation, nothing is left to unlock
//             if remaining_qty <= 0.0 {
//                 return Ok(Err("Order is already fully filled".to_string()));
//             }

//             // Determine asset symbol and amount to unlock
//             let (asset_symbol, unlock_amount) = match order.side {
//                 Side::Buy => {
//                     // For Buy orders, we locked USD (quote asset) based on limit price * remaining quantity
//                     ("USD", remaining_qty * order.price)
//                 }
//                 Side::Sell => {
//                     // For Sell orders, we locked base asset (SOL/BTC/ETH) based on remaining quantity
//                     let base = match market {
//                         MarketPair::SolUsdc => "SOL",
//                         MarketPair::BtcUsdc => "BTC",
//                         MarketPair::EthUsdc => "ETH",
//                     };
//                     (base, remaining_qty)
//                 }
//             };

//             // Create oneshot channel for asynchronous synchronization
//             let (resp_tx, resp_rx) = oneshot::channel();

//             let unlock_cmd = BalanceCommand::UnlockFunds {
//                 user_id: order.user_id,
//                 asset_symbol: asset_symbol.to_string(),
//                 amount: unlock_amount,
//                 resp: resp_tx,
//             };

//             // Send to balance worker actor
//             self.balance_sender.send(unlock_cmd).await
//                 .map_err(|_| io::Error::new(io::ErrorKind::Other, "Balance worker channel closed"))?;

//             // Await ledger confirmation
//             match resp_rx.await {
//                 Ok(Ok(_)) => {
//                     println!("Successfully unlocked {} {} for cancelled order #{}", unlock_amount, asset_symbol, order_id);
//                 }
//                 Ok(Err(err)) => {
//                     eprintln!("Ledger unlock failed: {}", err);
//                     return Ok(Err(err));
//                 }
//                 Err(_) => {
//                     return Ok(Err("Balance worker dropped response channel".to_string()));
//                 }
//             }
//         } else {
//             return Ok(Err("Order ID not found in active order book".to_string()));
//         }

//         Ok(Ok(()))
//     }
    
    
  
  
   



    ////////////////////////////// Live order modification with WAL logging
 









