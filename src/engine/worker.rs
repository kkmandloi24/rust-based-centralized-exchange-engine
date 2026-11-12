use tokio::sync::{mpsc,oneshot};
use crate::engine::{MatchingEngine, MarketPair};
use crate::engine::command::{MarketCommand,OrderAck};
use crate::ledger::BalanceCommand;
use crate::wal::{Wal, EngineCommand,ExchangeSnapshot,MarketSnapshotData,WallEntry};
use crate::models::{Order,Trade,Side,UserOrdersResponse,PublicOrderDto};
use std::io;
use std::fs::File;
use std::io::{ Write, Read};
use tokio::sync::broadcast;
use crate::ws::messages::MarketEvent;


pub struct MarketWorker {
    pub market_pair: MarketPair,
    pub receiver: mpsc::Receiver<MarketCommand>, //receiving the command/message from the API end point handlers
    pub engine: MatchingEngine,       // Exclusive ownership! No Mutex needed.
    pub wal: Wal,                     // Exclusive WAL instance
    pub balance_sender: mpsc::Sender<BalanceCommand>, // send message or To talk to the ledger worker to settle balances
    pub broadcast_tx: broadcast::Sender<MarketEvent>, // <-- Add this here
}

impl MarketWorker {
    pub async fn run(mut self) {
        println!("Market worker loop started for: {:?}", self.market_pair);

        // Infinite background loop processing commands sequentially (strict price-time priority)
        while let Some(command) = self.receiver.recv().await {
            match command {
                MarketCommand::PlaceOrder { order, respond_to } => {
                      
                      let side= order.side;
                      let price=order.price;

                    let result = self.place_order( order ).await;
                    let _ = respond_to.send(result);

                    let order_id=self.engine.next_sequence_number;

                    let total_quantity= self.engine.order_book.get_quantity_at_price(side,price);
                
                    ////////////////broadcasting the event for websockes///
                    let event= MarketEvent::Update { 
                        side: if side==Side::Buy{"buy".to_string()}else{"sell".to_string()},
                        price,
                        total_quantity,
                        last_update_id:order_id 
                        //last_updated_id here is the event id we need for the event to publish on the web socket
                        //we assigning last_updated_id/event_id using order_id arrangement, becuase we have incemtnal number next_sequmctial numer so we used it here
                    };

                        // 3. BROADCAST TO ALL WEBSOCKET CLIENTS LISTENING TO THIS MARKET
                        // .send() returns the number of active receivers, we ignore the Result if no clients are connected
                        let _ = self.broadcast_tx.send(event);

                },
                MarketCommand::CancelOrder { order_id,user_id, respond_to } => {
                    //here we handle the cancel order i the last before we tke out broadcasting variable for websocket first

                    let existing_order= match self.engine.active_orders.get(&order_id){
                        Some(order)=>order.clone(),
                        None=>{println!("order not found");
                        let _ = respond_to.send(Err("Order not found".into()));
                        continue;}
                    };

                     let side= existing_order.side;
                    let price=existing_order.price;

                //we cancel the order in the last becuase we need that order detail for the broadcasting on orderbook like price quantity etc
                    let result = self.cancelling_order(order_id,user_id).await;
                    let _ = respond_to.send(result.clone());

                    // If cancellation failed in engine, don't broadcast
                    if result.is_err(){
                        continue;
                    }
                    
                    //last_updated_id=Assinging this cancel event id by the next_sequence_number of engine
                   // 3. Increment sequence number //becuase browser and socket should track the cancela s new event so giving it new id
                    let current_sequence = self.engine.next_sequence_number;
                    self.engine.next_sequence_number += 1;

                    let total_quantity= self.engine.order_book.get_quantity_at_price(side,price);

                    //boradcasting the event
                    let event= MarketEvent::Cancel{
                        side:if side==Side::Buy{"buy".to_string()}else{"sell".to_string()},
                        price,
                        total_quantity,
                        last_updated_id:current_sequence
                    };

                    let _ = self.broadcast_tx.send(event);

                },

            
                MarketCommand::ModifyOrder { order_id, new_price, new_quantity, user_id,respond_to } => {
                  // 1. Get old order details before modifying so we can update both old and new price levels if needed  
                    
                 let old_order= match self.engine.active_orders.get(&order_id){
                    Some(order)=>order.clone(),
                    None=>{
                        println!("order not found");
                        let _= respond_to.send(Err("order not found".into()));
                        continue;
                    }
                 };

            
                    //Handling order modification
                    let result = self.modifying_order(order_id, new_price, new_quantity,user_id).await;
                    let _ = respond_to.send(result.clone());

                    //if modification not happen stop the execution (stop broadcasting)
                    if result.is_err(){
                        continue;
                    }

                    // 3. Increment sequence number
                    let seq_old = self.engine.next_sequence_number;
                    self.engine.next_sequence_number += 1;

                    // 4. Broadcast change for the old price level (volume decreased/removed)
                    let old_qty = self.engine.order_book.get_quantity_at_price(old_order.side, old_order.price);
                    let event_old = MarketEvent::Update {
                        side: if old_order.side == Side::Buy { "buy".to_string() } else { "sell".to_string() },
                        price: old_order.price,
                        total_quantity: old_qty,
                        last_update_id: seq_old,
                    };
                    let _ = self.broadcast_tx.send(event_old);

                    //IF PRICE CHANGE THEN WE PLACE NEW ORDER

                    // 5. If price changed, broadcast change for the new price level as well
                    if old_order.price != new_price {
                        let seq_new = self.engine.next_sequence_number;
                        self.engine.next_sequence_number += 1;

                        let new_qty = self.engine.order_book.get_quantity_at_price(old_order.side, new_price);
                        let event_new = MarketEvent::Update {
                            side: if old_order.side == Side::Buy { "buy".to_string() } else { "sell".to_string() },
                            price: new_price,
                            total_quantity: new_qty,
                            last_update_id: seq_new,
                        };
                        let _ = self.broadcast_tx.send(event_new);
                    }

                ///////////////////////////////
                
                    


                },

                

//this comand we use for use to check the users active otders
            MarketCommand::GetOrdersForUser { user_id, resp } => {
    let mut user_orders = Vec::new();
    
    for order in self.engine.active_orders.values() {
        if order.user_id == user_id {
            user_orders.push(PublicOrderDto {
                id: order.id,
                price: order.price,
                quantity: order.quantity,
                remaining_quantity: order.remaining_quantity,
                side: format!("{:?}", order.side),
            });
        }
    }
    
    let _ = resp.send(user_orders);
},

MarketCommand::GetSnapshot { resp }=>{
    // 1. Extract aggregated bids and asks from your order book structure methods
let bids= self.engine.order_book.get_bids_snapshot();// Returns Vec<(f64, f64)>
let asks=self.engine.order_book.get_asks_snapshot();// Returns Vec<(f64, f64)>

//get sequence (event id) with reference to next_sequence_number
let current_sequence= self.engine.next_sequence_number;

let snapshot_event= MarketEvent::Snapshot{
        bids, // [(price, size), ...]
        asks,
        last_update_id: current_sequence,

};

let _= resp.send(snapshot_event);
},

}
}
}



   ///////////////////////PLACE ORDER METHOD////////////////////////////////////



pub async fn place_order(&mut self, mut order: Order) -> Result<OrderAck, String> {
    let mut assigned_id = 0;
    let mut trades_to_settle: Vec<(Trade, Order, Order)> = Vec::new();

    // 1. Critical section: Mutate engine, write to WAL, and collect trades
    {
        let engine = &mut self.engine;
        let market = &self.market_pair;

        order.id = engine.next_sequence_number;
        engine.next_sequence_number += 1;
        assigned_id = order.id;

        engine.active_orders.insert(assigned_id, order.clone());

        let command = EngineCommand::PlaceOrder {
            market: market.clone(),
            order: order.clone(),
        };
        
        // Append to WAL
        self.wal.append(&command).map_err(|e| e.to_string())?;
        println!("here comes order : {:?}", &command);

        let trades = engine.order_book.add_order(order.clone());
        println!("Market {:?} processed order. Trades: {:?}", market, trades);

        for trade in trades {
            // Retrieve buyer and seller orders involved in this trade from engine.active_orders 
            // (or from your order book match result) and push them into trades_to_settle:
            // Example:
            // let buyer_order = engine.active_orders.get(&trade.buyer_order_id).cloned().unwrap();
            // let seller_order = engine.active_orders.get(&trade.seller_order_id).cloned().unwrap();
            // trades_to_settle.push((trade, buyer_order, seller_order));
        }
    } // 👈 CRITICAL: Engine borrow block ends here so `self` can be accessed freely below!

    // 2. Asynchronously settle trades with the balance worker
    for (trade, buyer_order, seller_order) in trades_to_settle {
        let (resp_tx, resp_rx) = oneshot::channel();

        let (base_asset, quote_asset) = match self.market_pair {
            MarketPair::SolUsdc => ("SOL", "USD"),
            MarketPair::BtcUsdc => ("BTC", "USD"),
            MarketPair::EthUsdc => ("ETH", "USD"),
        };

        let settle_cmd = BalanceCommand::SettleTrade {
            trade_id: trade.trade_id,
            market: self.market_pair.clone(),
            buyer_order_id: buyer_order.id,
            seller_order_id: seller_order.id,
            buyer_id: buyer_order.user_id,
            seller_id: seller_order.user_id,
            base_asset: base_asset.to_string(),
            quote_asset: quote_asset.to_string(),
            trade_quantity: trade.quantity,
            execution_price: trade.price,
            buyer_limit_price: buyer_order.price,
            resp: resp_tx,
        };

        self.balance_sender.send(settle_cmd).await
            .map_err(|_| "Balance worker channel closed".to_string())?;

        match resp_rx.await {
            Ok(Ok(_)) => {
                println!("Successfully settled trade: {} {} @ {}", trade.quantity, base_asset, trade.price);
            }
            Ok(Err(err)) => {
                eprintln!("Ledger settlement failed: {}", err);
                return Err(err);
            }
            Err(_) => {
                return Err("Balance worker dropped response channel".to_string());
            }
        }
    }

    // 3. Optional automatic snapshot trigger (every 10,000 orders)
    if assigned_id % 10_000 == 0 {
        println!("Threshold reached! Triggering automatic snapshot...");
        self.create_snapshot().await.map_err(|e| e.to_string())?;
    }

    // 4. Return the successfully populated OrderAck
    Ok(OrderAck {
        order_id: assigned_id,
        market_pair: format!("{:?}", self.market_pair),
    })
}


////////////////////////////////////////////
    //Old Place_order method
    // pub async fn place_order(&mut self,  mut order: Order) -> io::Result<Result<(), String>> {
    //     let mut assigned_id = 0;
    //     let mut trades_to_settle = Vec::new();

    //     // 1. Critical section: Mutate engine state, assign ID, log to WAL, and match

    //     {

    //         let engine= &mut self.engine;
    //         let market= &mut self.market_pair;

    //         order.id = engine.next_sequence_number;
    //         engine.next_sequence_number += 1;
    //         assigned_id = order.id;

    //         // Cache the order so we can lookup user_id and price during settlement(during settlement we lost the identityof the order so we saved here)
    //         engine.active_orders.insert(assigned_id, order.clone());

    //         //prepared ordr command
    //         let command = EngineCommand::PlaceOrder {
    //             market:market.clone(),
    //             order: order.clone(),
    //         };

    //         self.wal.append(&command)?;
    //         println!(" here comes order : {:?}", &command);

    //         //saved order before sending to orderbook
    //         let saved_order= order.clone();

    //         //Run matching engine: //then we process the order in the orderbook method add order which retuns processed order,that we save in the trades vector
    //         let trades = engine.order_book.add_order(order);
    //         println!("Market {:?} processed order. Trades: {:?}", market, trades);

    //         // Collect trades along with needed order metadata for settlement
    //         //here we running loop on the trades(executed order), then select the buy/sell order from active order we saved using trade buyer/seller id
    //         //becuase before trade we have seller order and buyer order if those order excuted we have to settle them
            
    //         //we use buyer or seller id and pick the buyer or seller order that has beed executed
    //         for trade in trades {

    //            // taking executed order and backtrace the buyer and seller order details
    //            //so we can settle their balances and for that we need the order details


    //             //Now we need all the metadata all info of buyers order an seller order which gone fro matching, below
    //             //so we could settle it further with seller buyer info we could not settle
    //        //Update Buyer order , in the traded order we saved buyerorder_id and seller_id 

    //        // Safely fetch or fallback for Buyer Order
    //             let mut buyer_order = if trade.buyer_order_id == saved_order.id {
    //                 saved_order.clone()
    //             } else {

    //                 //if not then we take out traded buyer order definitiation from the active_order
    //                 engine.active_orders.get(&trade.buyer_order_id)
    //                     .cloned()
    //                     .unwrap_or_else(|| saved_order.clone()) // Fallback if missing
    //             };

    //             // let mut buyer_order = engine.active_orders.get(&trade.buyer_order_id)
    //             //     .cloned()
    //             //     .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Buyer order metadata missing"))?;
                
    //             //check buyer order id
    //            buyer_order.remaining_quantity-=trade.quantity;
    //             let buyer_clone=buyer_order.clone();
    //             //asuming if fuly filled then what it islike
    //             let buyer_fully_filled= buyer_order.remaining_quantity<=f64::EPSILON;
    //             //f64::EPSILON give lowest positive number before negative (in fractions0.000001 like this)

    //             //Update seller order
    //             // let mut seller_order = engine.active_orders.get(&trade.seller_order_id)
    //             //     .cloned()
    //             //     .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Seller order metadata missing"))?;

    //           // Safely fetch or fallback for Seller Order
    //             let mut seller_order = if trade.seller_order_id == saved_order.id {
    //                 saved_order.clone()
    //             } else {
    //                 engine.active_orders.get(&trade.seller_order_id)
    //                     .cloned()
    //                     .unwrap_or_else(|| saved_order.clone()) // Fallback if missing
    //             };


    //             seller_order.remaining_quantity-=trade.quantity;
    //             let seller_clone=seller_order.clone();
    //             let seller_fully_filled= seller_order.remaining_quantity<=f64::EPSILON;


    //             //now we have executed order which before execution was buyer order and seller order
    //             //sofor the settlement we need buyer order,seller order , and matching of both we get trades(executed order),
    //             //we push it in the vector called  trades_to_settle and 
    //             trades_to_settle.push((trade.clone(), buyer_clone, seller_clone));

    //             //Now we update our active orders
    //             //we remove the fully filled orders from the active_order Vectos
    //             if buyer_fully_filled{
    //                 engine.active_orders.remove(&trade.buyer_order_id);
                    
                    
    //             }

    //             if seller_fully_filled{
    //                 //remove the executed order from the active_order
    //             engine.active_orders.remove(&trade.seller_order_id);

    //             }
                
    //         }
    //     }

    //     //Now run loop on vector where we saved executed order and mased executed from byer and seller order, so we can process for the settlement
    //     // 2. Asynchronously settle trades with the balance worker (outside the engine lock block)
    //     for (trade, buyer_order, seller_order) in trades_to_settle {
    //         let (resp_tx, resp_rx) = oneshot::channel();


    //         // Determine base and quote assets dynamically based on MarketPair
    //         let (base_asset, quote_asset) = match market {
    //             MarketPair::SolUsdc => ("SOL", "USD"),
    //             MarketPair::BtcUsdc => ("BTC", "USD"),
    //             MarketPair::EthUsdc => ("ETH", "USD"),
    //         };



    //         let settle_cmd = BalanceCommand::SettleTrade {
    //             trade_id:trade.trade_id,
    //             market:self.market_pair,
    //             buyer_order_id:buyer_order.id,
    //             seller_order_id:seller_order.id,
    //             buyer_id: buyer_order.user_id,
    //             seller_id: seller_order.user_id,
    //             base_asset: base_asset.to_string(),
    //             quote_asset: quote_asset.to_string(),
    //             trade_quantity: trade.quantity,
    //             execution_price: trade.price,
    //             buyer_limit_price: buyer_order.price,
    //             resp: resp_tx,
    //         };

    //         // Send command to workers.rs//here sender wecreated inside this fle sp not need to take it from global state
    //         //settlment mps sender we create in the struct Exchange server struct 
    //         self.balance_sender.send(settle_cmd).await
    //             .map_err(|_| io::Error::new(io::ErrorKind::Other, "Balance worker channel closed"))?;

    //         // Await ledger settlement confirmation
    //         match resp_rx.await {
    //             Ok(Ok(_)) => {
    //                 println!("Successfully settled trade: {} {} @ {}", trade.quantity, base_asset, trade.price);
    //             }
    //             Ok(Err(err)) => {
    //                 eprintln!("Ledger settlement failed: {}", err);
    //                 return Ok(Err(err));
    //             }
    //             Err(_) => {
    //                 return Ok(Err("Balance worker dropped response channel".to_string()));
    //             }
    //         }
    //     }

    //     // 3. Optional automatic snapshot trigger
    //     if assigned_id % 10_000 == 0 {
    //         let wal_path = "exchange.wal";
    //         let snapshot_file = "exchange.snapshot";
    //         println!("Threshold reached! Triggering automatic snapshot...");
    //         self.create_snapshot(snapshot_file, wal_path).await?;
    //     }

    //     Ok(Ok(()))
    // }

    //CANCELING ORDER METHOD

     pub async fn cancelling_order(
    &mut self,
    id: u64,
    user_id: u32,
) -> Result<(), String> {
    let mut order_to_cancel = None;
    let mut unlock_amount = 0.0;
    let mut asset_symbol = String::new();

    // 1. Critical section: Validate ownership, check execution, and remove from engine state
    {
        let engine = &mut self.engine;

        let existing_order = match engine.active_orders.get(&id) {
            Some(order) => order,
            None => return Err("Order not found, already filled or canceled".to_string()),
        };

        // 2. Verify ownership
        if existing_order.user_id != user_id {
            return Err("Unauthorized: You do not own this order".to_string());
        }

        // 3. Check if order is fully filled
        if existing_order.remaining_quantity <= f64::EPSILON {
            engine.active_orders.remove(&id);
            return Err("Order is fully executed or filled before".to_string());
        }

        order_to_cancel = Some(existing_order.clone());
        let order = existing_order;

        // Determine asset symbol and amount to unlock
        let (symbol, amount) = match order.side {
            Side::Buy => {
                ("USD", order.remaining_quantity * order.price)
            }
            Side::Sell => {
                let base = match self.market_pair {
                    MarketPair::SolUsdc => "SOL",
                    MarketPair::BtcUsdc => "BTC",
                    MarketPair::EthUsdc => "ETH",
                   // adjust if any other variant
                    };
                (base, order.remaining_quantity)
            }
        };

        unlock_amount = amount;
        asset_symbol = symbol.to_string();

        engine.active_orders.remove(&id);
        engine.order_book.cancel_order(id);
    } // <-- Engine borrow block ends here

    let order = order_to_cancel.ok_or("Order metadata missing".to_string())?;

    // 4. Asynchronously unlock funds with the balance worker
    let (resp_tx, resp_rx) = oneshot::channel();
    let unlock_cmd = BalanceCommand::UnlockFunds {
        user_id: order.user_id,
        asset_identifier: asset_symbol.clone(),
        amount: unlock_amount,
        resp: resp_tx,
    };

    self.balance_sender
        .send(unlock_cmd)
        .await
        .map_err(|_| "Balance worker channel closed".to_string())?;

    match resp_rx.await {
        Ok(Ok(_)) => {
            println!("Successfully unlocked {} {} for cancelled order #{}", unlock_amount, asset_symbol, id);
        }
        Ok(Err(err)) => {
            eprintln!("Ledger unlock failed: {}", err);
            return Err(err);
        }
        Err(_) => {
            return Err("Balance worker dropped response channel".to_string());
        }
    }

    // 5. Append to WAL (map io::Error to String)
    let command = EngineCommand::CancelOrder { 
        market: self.market_pair.clone(), 
        id, 
    };
    self.wal.append(&command).map_err(|e| e.to_string())?;

    Ok(())
}


   ///////////////MODIFY ORDER ////////////
   
// 
pub async fn modifying_order(
    &mut self,
    id: u64,
    new_price: f64,
    new_quantity: f64,
    user_id: u32,
) -> Result<OrderAck, String> {
    let mut modified_result = None;
    let mut assigned_seq=0;

    // 1. Critical section: Validate ownership and invoke orderbook modify method
    {
        let engine = &mut self.engine;

        // 1) Validate if order exists
        let existing_order = match engine.active_orders.get(&id) {
            Some(order) => order,
            None => return Err("Order does not exist, either completely filled or canceled".to_string()),
        };

        // 2) Validate if the user owns this order
        if existing_order.user_id != user_id {
            return Err("Unauthorized: You do not own this order".to_string());
        }

        // 3) Check if order is fully filled
        if existing_order.remaining_quantity <= f64::EPSILON {
            engine.active_orders.remove(&id);
            return Err("Order is fully executed or filled before".to_string());
        }

        // Invoke OrderBook modify method
        match engine.order_book.modify_order(id, new_price, new_quantity) {
            Ok(Some(res)) => {
                modified_result = Some(res); // (modified_order, cost_diff, is_increase)
            }
            Ok(None) => {
                return Err("Order not found in order book".to_string());
            }
            Err(err_msg) => {
                return Err(err_msg);
            }
        }



        // If modification succeeded in orderbook, update active_orders cache and WAL
        if let Some((ref updated_order, _, _)) = modified_result {
            engine.active_orders.insert(id, updated_order.clone());

             // CRITICAL FIX: Advance sequence number for the modification event stream
            // assigned_seq = engine.next_sequence_number;
            // engine.next_sequence_number += 1;

            let command = EngineCommand::ModifyOrder {
                market: self.market_pair.clone(),
                id,
                new_price,
                new_quantity,
            };
            self.wal.append(&command).map_err(|e| e.to_string())?;
        }
    } // End of critical section block

    // 2. Coordinate balance lock adjustments with the Balance Worker
    if let Some((order, cost_diff, is_increase)) = modified_result {
        if cost_diff > 0.0 {
            let asset_symbol = match order.side {
                Side::Buy => "USD".to_string(),
                Side::Sell => match self.market_pair {
                    MarketPair::SolUsdc => "SOL".to_string(),
                    MarketPair::BtcUsdc => "BTC".to_string(),
                    MarketPair::EthUsdc => "ETH".to_string(),
                },
            };

            let (resp_tx, resp_rx) = oneshot::channel();

            let balance_cmd = if is_increase {
                BalanceCommand::LockFunds {
                    user_id: order.user_id,
                    asset_identifier: asset_symbol.clone(),
                    amount: cost_diff,
                    resp: resp_tx,
                }
            } else {
                BalanceCommand::UnlockFunds {
                    user_id: order.user_id,
                    asset_identifier: asset_symbol.clone(),
                    amount: cost_diff,
                    resp: resp_tx,
                }
            };

            self.balance_sender
                .send(balance_cmd)
                .await
                .map_err(|_| "Balance worker channel closed".to_string())?;

            match resp_rx.await {
                Ok(Ok(_)) => {
                    println!("Successfully adjusted funds by {} for modified order #{}", cost_diff, id);
                }
                Ok(Err(err)) => {
                    eprintln!("Ledger fund adjustment failed: {}", err);
                    return Err(err);
                }
                Err(_) => {
                    return Err("Balance worker dropped response channel".to_string());
                }
            }
        }
    }

    Ok(OrderAck {
        order_id: id,
        market_pair: format!("{:?}", self.market_pair),
    })
}


/////////////////////////////////////////CREATE SNAPSHOT//////////////////

    pub async fn create_snapshot(&mut self) -> io::Result<()> {
        let snapshot_path = format!("{:?}.snapshot", self.market_pair).to_lowercase();
        let wal_path = format!("{:?}.wal", self.market_pair).to_lowercase();

        let max_seq = self.engine.next_sequence_number;
        let bids = self.engine.order_book.get_bids();
        let asks = self.engine.order_book.get_asks();

        let market_data = MarketSnapshotData { bids, asks };

        // 1. Ask the central Balance Worker for current balances
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.balance_sender
            .send(BalanceCommand::GetSnapshot { reply_to: tx })
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let current_balances = rx.await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        // 2. Build the snapshot struct for this specific market
        let snapshot = ExchangeSnapshot {
            seq: max_seq,
            market_pair: self.market_pair.clone(),
            market_data,
            user_balances: current_balances,
        };

        // 3. Save snapshot to disk
        ExchangeSnapshot::save(&snapshot_path, &snapshot)?;
        println!("Snapshot successfully created for {:?} at sequence ID: {}", self.market_pair, max_seq);

        // 4. Clear/truncate this market's WAL file
        let _fresh_wal = std::fs::File::create(&wal_path)?;
        println!("WAL cleared for {:?}. Starting fresh.", self.market_pair);

        Ok(())
    }
    

    ////////////////////////////////////////////////////////////////
    //////////////Recover Market Engine ////////////////
//     pub async fn recover_market_engine(
//     market_pair: &MarketPair,
//     balance_sender: &mpsc::Sender<BalanceCommand>,
// ) -> io::Result<(MatchingEngine, Wal)> {


//     let snapshot_path = format!("{:?}.snapshot", market_pair).to_lowercase();
//     let wal_path = format!("{:?}.wal", market_pair).to_lowercase();

//     let mut engine = MatchingEngine::new();
//     let mut last_seq = 0;

//     // 1. Load per-market snapshot if it exists on disk
//     if let Some(snapshot) = ExchangeSnapshot::load(&snapshot_path)? {
//         println!("Restored snapshot for {:?} up to sequence ID: {}", market_pair, snapshot.seq);
        
//         last_seq = snapshot.seq;

//         // Restore bids into order book from snapshot
//         for order in snapshot.market_data.bids { // adjust based on your snapshot struct
//             if order.id >= engine.next_sequence_number {
//                 engine.next_sequence_number = order.id + 1;
//             }
//             engine.active_orders.insert(order.id, order.clone());
//             let _ = engine.order_book.add_order(order);
//         }

//         // Restore asks into order book
//         for order in snapshot.market_data.asks {
//             if order.id >= engine.next_sequence_number {
//                 engine.next_sequence_number = order.id + 1;
//             }
//             engine.active_orders.insert(order.id, order.clone());
//             let _ = engine.order_book.add_order(order);
//         }

//         // NOTE: Since user balances are global across markets, we send them to the balance worker. 
//         // (To avoid sending the same balances multiple times if you have multiple markets, 
//         // you can either send it once globally or let the balance worker handle idempotent restoration).
//         let (resp_tx, resp_rx) = tokio::sync::oneshot::channel::<Result<(), String>>();
//         let _ = balance_sender.send(BalanceCommand::RestoreSnapshot { 
//             balances: snapshot.user_balances,
//             resp: resp_tx, 
//         }).await;

//         let _ = resp_rx.await;
//     }

//     // 2. Recover WAL entries and replay the gap (entries > last_seq)
//     let all_entries: Vec<WallEntry> = Wal::recover(&wal_path).unwrap_or_default();
//     let gap_entries: Vec<WallEntry> = all_entries
//         .into_iter()
//         .filter(|entry| entry.seq > last_seq)
//         .collect();

//     println!("Replaying {} gap events for {:?}...", gap_entries.len(), market_pair);

//    ///recovering the command from the wal(gap) &and settling the in-memory balance with respective command
//    for entry in gap_entries {
//         match entry.command {
//             EngineCommand::PlaceOrder { market, order } => {
//                 if market == *market_pair {
//                     if order.id >= engine.next_sequence_number {
//                         engine.next_sequence_number = order.id + 1;
//                     }
//                     engine.active_orders.insert(order.id, order.clone());
//                     let _ = engine.order_book.add_order(order.clone());

//                     // Replay balance lock in RAM
//                     let (asset_identifier, lock_amount) = MarketWorker::calculate_required_lock(&order, &market);
//                     let _ = balance_sender.send(BalanceCommand::ReplayLockFunds {
//                         user_id: order.user_id,
//                         asset_identifier,
//                         amount: lock_amount,
//                     }).await;
//                 }
//             }
//             EngineCommand::CancelOrder { market, id } => {
//                 if market == *market_pair {
//                     if let Some(order) = engine.active_orders.get(&id) {
//                         let (asset_identifier, lock_amount) = MarketWorker::calculate_required_lock(order, &market);
//                         let _ = balance_sender.send(BalanceCommand::ReplayUnlockFunds {
//                             user_id: order.user_id,
//                             asset_identifier,
//                             amount: lock_amount,
//                         }).await;
//                     }
//                     engine.active_orders.remove(&id);
//                     engine.order_book.cancel_order(id);
//                 }
//             }
//             EngineCommand::ModifyOrder { market, id, new_price, new_quantity } => {
//                 if market == *market_pair {
//                     if let Some(existing_order) = engine.active_orders.get(&id) {
//                         let _ = engine.order_book.add_order(existing_order.clone());
//                     }

//                     if let Ok(Some((updated_order, cost_diff, is_increase))) = engine.order_book.modify_order(id, new_price, new_quantity) {
//                         let asset_symbol = match updated_order.side {
//                             Side::Buy => "USD".to_string(),
//                             Side::Sell => match market {
//                                 MarketPair::SolUsdc => "SOL".to_string(),
//                                 MarketPair::BtcUsdc => "BTC".to_string(),
//                                 MarketPair::EthUsdc => "ETH".to_string(),
//                             },
//                         };

//                         if cost_diff > 0.0 {
//                             if is_increase {
//                                 let _ = balance_sender.send(BalanceCommand::ReplayLockFunds {
//                                     user_id: updated_order.user_id,
//                                     asset_identifier: asset_symbol,
//                                     amount: cost_diff,
//                                 }).await;
//                             } else {
//                                 let _ = balance_sender.send(BalanceCommand::ReplayUnlockFunds {
//                                     user_id: updated_order.user_id,
//                                     asset_identifier: asset_symbol,
//                                     amount: cost_diff,
//                                 }).await;
//                             }
//                         }

//                         engine.active_orders.insert(id, updated_order);
//                     }
//                 }
//             }
//             EngineCommand::SettleTrade { 
//                 trade_id, market, buyer_order_id, seller_order_id, 
//                 buyer_id, seller_id, base_asset, quote_asset, 
//                 trade_quantity, execution_price, buyer_limit_price 
//             } => {
//                 if market == *market_pair {
//                     let _ = balance_sender.send(BalanceCommand::ReplaySettleTrade {
//                         trade_id,
//                         market,
//                         buyer_order_id,
//                         seller_order_id,
//                         buyer_id,
//                         seller_id,
//                         base_asset,
//                         quote_asset,
//                         trade_quantity,
//                         execution_price,
//                         buyer_limit_price,
//                     }).await;
//                 }
//             }
//             _ => {}
//         }
//     } 

//     engine.recalculate_sequence_number();
//     // //initiating the wal
//     let wal = Wal::new(&wal_path)?;
//     Ok((engine, wal))
// }



pub async fn recover_market_engine(
    market_pair: &MarketPair,
    balance_sender: &mpsc::Sender<BalanceCommand>,
) -> io::Result<(MatchingEngine, Wal)> {
    let snapshot_path = format!("{:?}.snapshot", market_pair).to_lowercase();
    let wal_path = format!("{:?}.wal", market_pair).to_lowercase();

    // 1. Initialize or open the WAL file FIRST so we don't truncate/destroy it later
    let wal = Wal::new(&wal_path)?;

    let mut engine = MatchingEngine::new();
    let mut last_seq = 0;

    // 2. Load per-market snapshot if it exists on disk
    if let Some(snapshot) = ExchangeSnapshot::load(&snapshot_path)? {
        println!("Restored snapshot for {:?} up to sequence ID: {}", market_pair, snapshot.seq);
        
        last_seq = snapshot.seq;

        // Restore bids into order book from snapshot
        for order in snapshot.market_data.bids {
            if order.id >= engine.next_sequence_number {
                engine.next_sequence_number = order.id + 1;
            }
            engine.active_orders.insert(order.id, order.clone());
            let _ = engine.order_book.add_order(order);
        }

        // Restore asks into order book
        for order in snapshot.market_data.asks {
            if order.id >= engine.next_sequence_number {
                engine.next_sequence_number = order.id + 1;
            }
            engine.active_orders.insert(order.id, order.clone());
            let _ = engine.order_book.add_order(order);
        }

        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel::<Result<(), String>>();
        let _ = balance_sender.send(BalanceCommand::RestoreSnapshot { 
            balances: snapshot.user_balances,
            resp: resp_tx, 
        }).await;

        let _ = resp_rx.await;
    }

    // 3. Recover WAL entries and replay the gap (entries > last_seq)
    let all_entries: Vec<WallEntry> = Wal::recover(&wal_path).unwrap_or_default();
    let gap_entries: Vec<WallEntry> = all_entries
        .into_iter()
        .filter(|entry| entry.seq > last_seq)
        .collect();

    println!("Replaying {} gap events for {:?}...", gap_entries.len(), market_pair);

    for entry in gap_entries {
        match entry.command {
            EngineCommand::PlaceOrder { market, order } => {
                if market == *market_pair {
                    if order.id >= engine.next_sequence_number {
                        engine.next_sequence_number = order.id + 1;
                    }
                    engine.active_orders.insert(order.id, order.clone());
                    let _ = engine.order_book.add_order(order.clone());

                    let (asset_identifier, lock_amount) = MarketWorker::calculate_required_lock(&order, &market);
                    let _ = balance_sender.send(BalanceCommand::ReplayLockFunds {
                        user_id: order.user_id,
                        asset_identifier,
                        amount: lock_amount,
                    }).await;
                }
            }
            EngineCommand::CancelOrder { market, id } => {
                if market == *market_pair {
                    if let Some(order) = engine.active_orders.get(&id) {
                        let (asset_identifier, lock_amount) = MarketWorker::calculate_required_lock(order, &market);
                        let _ = balance_sender.send(BalanceCommand::ReplayUnlockFunds {
                            user_id: order.user_id,
                            asset_identifier,
                            amount: lock_amount,
                        }).await;
                    }
                    engine.active_orders.remove(&id);
                    engine.order_book.cancel_order(id);
                }
            }
            EngineCommand::ModifyOrder { market, id, new_price, new_quantity } => {
                if market == *market_pair {
                    if let Some(existing_order) = engine.active_orders.get(&id) {
                        let _ = engine.order_book.add_order(existing_order.clone());
                    }

                    if let Ok(Some((updated_order, cost_diff, is_increase))) = engine.order_book.modify_order(id, new_price, new_quantity) {
                        let asset_symbol = match updated_order.side {
                            Side::Buy => "USD".to_string(),
                            Side::Sell => match market {
                                MarketPair::SolUsdc => "SOL".to_string(),
                                MarketPair::BtcUsdc => "BTC".to_string(),
                                MarketPair::EthUsdc => "ETH".to_string(),
                            },
                        };

                        if cost_diff > 0.0 {
                            if is_increase {
                                let _ = balance_sender.send(BalanceCommand::ReplayLockFunds {
                                    user_id: updated_order.user_id,
                                    asset_identifier: asset_symbol,
                                    amount: cost_diff,
                                }).await;
                            } else {
                                let _ = balance_sender.send(BalanceCommand::ReplayUnlockFunds {
                                    user_id: updated_order.user_id,
                                    asset_identifier: asset_symbol,
                                    amount: cost_diff,
                                }).await;
                            }
                        }

                        engine.active_orders.insert(id, updated_order);
                    }
                }
            }
            EngineCommand::SettleTrade { 
                trade_id, market, buyer_order_id, seller_order_id, 
                buyer_id, seller_id, base_asset, quote_asset, 
                trade_quantity, execution_price, buyer_limit_price 
            } => {
                if market == *market_pair {
                    let _ = balance_sender.send(BalanceCommand::ReplaySettleTrade {
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
                    }).await;
                }
            }
            _ => {}
        }
    } 

    // REMOVED engine.recalculate_sequence_number() here 
    // because next_sequence_number is already safely maintained via `order.id + 1` 
    // during snapshot and WAL replay.

    println!("Recovery complete for {:?}. Next sequence ID: {}", market_pair, engine.next_sequence_number);

    Ok((engine, wal))
}
    
    ///////////////////////////////////////////////////////////////
    /// Helper functions
    
pub fn calculate_required_lock(order: &Order, market: &MarketPair) -> (String, f64) {

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

////////GET ALL MARKET ORDER///////////////////

// pub async fn getallorders(&self, user_id:u32)->Result<Vec<PublicOrderDto>,String>{
//     //we do not get the market, we just send the user id
//  let mut user_orders= Vec::new();

// //     pub struct MatchingEngine {
// //     pub order_book: OrderBook,
// //     pub next_sequence_number: u64, // <-- Order sequence lives here! we use it to give order id
// //     pub active_orders:HashMap<u64,Order>
// // }

//    for engine in self.engines.values(){

//     for order in engine.active_orders.values(){
//         if order.user_id==user_id{
//             user_orders.push(PublicOrderDto{
//                 id:order.id,
//                 price:order.price,
//                 quantity:order.quantity,
//                 remaining_quantity: order.remaining_quantity,
//                 side: format!("{:?}",order.side)

//             })
//         }
//     }

//    }

//    Ok(user_orders)

// }

}