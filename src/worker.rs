use crate::db::{get_asset_registry, resolve_asset_symbol, AssetBalance, AssetInfo, AssetSymbol, DbStore};
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};
use sqlx::PgPool;
use crate::engine::{MarketPair};

/// Response payload containing updated balance details and metadata
#[derive(Debug, Clone)]
pub struct BalanceOpResult {
    pub updated_balance: AssetBalance,
    pub asset_info: AssetInfo,
}

// /// All operations supported by the balance worker
// pub enum BalanceCommand {
//     Deposit {
//         user_id: u32,
//         asset_identifier: String,
//         amount: f64,
//         payment_type: crate::db::Type,
//         resp: oneshot::Sender<Result<BalanceOpResult, String>>,
//     },
//     GetSingleBalance {
//         user_id: u32,
//         asset_identifier: String,
//         resp: oneshot::Sender<Result<BalanceOpResult, String>>,
//     },
//     GetAllBalances {
//         user_id: u32,
//         resp: oneshot::Sender<HashMap<AssetSymbol, AssetBalance>>,
//     },
//     LockFunds{
//         user_id: u32,
//         asset_identifier: String,
//         amount: f64,
//         resp: oneshot::Sender<Result<(), String>>,
//     },
//     UnlockFunds {
//         user_id: u32,
//         asset_identifier: String,
//         amount: f64,
//         resp: oneshot::Sender<Result<(), String>>,
//     },
   
//     // ... existing variants (LockFunds, UnlockFunds, SettleTrade, etc.) ...
//     /// Modifies locked funds when an order is updated.
//     /// `diff`: The absolute difference in required funds.
//     /// `is_increase`: true if we need to lock more, false if we need to unlock excess.
//     ModifyOrderLocks {
//         user_id: u32,
//         asset_identifier: String,
//         diff: f64,
//         is_increase: bool,
//         resp: oneshot::Sender<Result<(), String>>,
//     },

//     SettleTrade {
//         trade_id:u64,
//         market: MarketPair,
//         buyer_order_id:u64,
//         seller_order_id:u64,
//         buyer_id: u32,
//         seller_id: u32,
//         base_asset: String,   // e.g., "BTC"
//         quote_asset: String,  // e.g., "USD"
//         trade_quantity: f64,
//         execution_price: f64,
//         buyer_limit_price: f64, // Needed to unlock price difference if buyer limit > execution price
//         resp: oneshot::Sender<Result<(), String>>,
//     },
//     // 👇 Add these to communicate with your worker during snapshots/recovery
//     GetSnapshot { 
//         reply_to: tokio::sync::oneshot::Sender<HashMap<u32, HashMap<String, f64>>> 
//     },
//     RestoreSnapshot { 
//         balances: HashMap<u32, HashMap<String, f64>> 
//     },
// }


////new command with replay options: 

pub enum BalanceCommand {
    // =========================================================================
    // 1. LIVE DATABASE-PERSISTING COMMANDS (Hits Supabase first, then updates RAM)
    // Used during normal runtime when handling user API requests.
    // =========================================================================
    Deposit {
        user_id: u32,
        asset_identifier: String,
        amount: f64,
        resp: oneshot::Sender<Result<BalanceOpResult, String>>,
    },
    Withdraw {
        user_id: u32,
        asset_identifier: String,
        amount: f64,
        resp: oneshot::Sender<Result<BalanceOpResult, String>>,
    },
    SettleTrade {
        trade_id: u64,
        market: MarketPair,
        buyer_order_id: u64,
        seller_order_id: u64,
        buyer_id: u32,
        seller_id: u32,
        base_asset: String,
        quote_asset: String,
        trade_quantity: f64,
        execution_price: f64,
        buyer_limit_price: f64,
        resp: oneshot::Sender<Result<(), String>>,
    },

    
    // =========================================================================
    // 3. STANDARD IN-MEMORY OPERATIONS (RAM ONLY - LIVE & RECOVERY)
    // Because these never touched Supabase, they can be used directly anytime.
    // =========================================================================
    GetSingleBalance {
        user_id: u32,
        asset_identifier: String,
        resp: oneshot::Sender<Result<BalanceOpResult, String>>,
    },
    GetAllBalances {
        user_id: u32,
        resp: oneshot::Sender<HashMap<AssetSymbol, AssetBalance>>,
    },
    LockFunds {
        user_id: u32,
        asset_identifier: String,
        amount: f64,
        resp: oneshot::Sender<Result<(), String>>,
    },
    UnlockFunds {
        user_id: u32,
        asset_identifier: String,
        amount: f64,
        resp: oneshot::Sender<Result<(), String>>,
    },
    ModifyOrderLocks {
        user_id: u32,
        asset_identifier: String,
        diff: f64,
        is_increase: bool,
        resp: oneshot::Sender<Result<(), String>>,
    },
    // =========================================================================
    // 2. REPLAY / RECOVERY COMMANDS (RAM ONLY - NO SUPABASE)
    // Used exclusively during startup WAL gap recovery to rebuild RAM state safely.
    // Notice they don't need oneshot response channels since no HTTP client is waiting.
    // =========================================================================
    ReplayDeposit {
        user_id: u32,
        asset_identifier: String,
        amount: f64,
    },
    ReplayWithdraw {
        user_id: u32,
        asset_identifier: String,
        amount: f64,
    },
    ReplaySettleTrade {
        trade_id: u64,
        market: MarketPair,
        buyer_order_id: u64,
        seller_order_id: u64,
        buyer_id: u32,
        seller_id: u32,
        base_asset: String,
        quote_asset: String,
        trade_quantity: f64,
        execution_price: f64,
        buyer_limit_price: f64,
    },
    ReplayLockFunds{
        user_id: u32,
        asset_identifier: String,
        amount: f64,

    },

      ReplayUnlockFunds {
        user_id: u32,
        asset_identifier: String,
        amount: f64,
    },
    //  ReplayModifyOrderLocks {
    //     user_id: u32,
    //     asset_identifier: String,
    //     diff: f64,
    //     is_increase: bool,
   
    // },


    // =========================================================================
    // 4. SYSTEM & SNAPSHOT COMMANDS
    // =========================================================================
    GetSnapshot { 
        reply_to: oneshot::Sender<HashMap<u32, HashMap<AssetSymbol, AssetBalance>>> 
    },
    
RestoreSnapshot { 
    balances: HashMap<u32, HashMap<AssetSymbol, AssetBalance>>,
    resp: oneshot::Sender<Result<(), String>>,
},
   
}

//WE GET WALLET READY WITH ASSETS DECIMAL PRECISION AND AVAILABLE AND LOCKED BALANCE

/// Wallet creation function:  Generates a default zero-balance wallet for all registered assets
fn default_user_balances() -> HashMap<AssetSymbol, AssetBalance> {
    let mut wallet = HashMap::new();
    let registry = get_asset_registry();

    //adding on each asset the assetdecimal formating
    for asset in registry {
        let formatted_zero = format!("{:.1$}", 0.0, asset.decimals as usize);
        wallet.insert(
            asset.symbol,
            AssetBalance {
                available: formatted_zero.clone(),
                locked: formatted_zero,
            },
        );
    }
    wallet
}

//If user is coming with id=1 check in the db isuser has wallet with all assets format
//if not then create the wallet

/// Helper inside worker loop
fn get_or_create_user_wallet(
    db: &mut DbStore,
    user_id: u32,
) -> &mut HashMap<AssetSymbol, AssetBalance> {
    //first we check bu user-id entry if the user wallet in the db if not er tun default wallet creation
    db.entry(user_id).or_insert_with(default_user_balances)
}

//MAKING SINGLE THREADED Reciver (handlers points which recive request)
/// Dedicated single-threaded worker loop processing operations sequentially
pub async fn start_balance_worker(pool:PgPool, mut receiver: mpsc::Receiver<BalanceCommand>) {
    let mut db: DbStore = HashMap::new();

    while let Some(cmd) = receiver.recv().await {
        match cmd {
            BalanceCommand::Deposit {
                user_id,
                asset_identifier,
                amount,
                resp,
                ..
            } => {

                //validation: if amount is 0 or less than 0 then we send err to the mpsc channle and skip loop
                if amount<=0.0{
                    let _= resp.send(Err("Deposit must be greater than 0".to_string()));
                    continue; //skip the the command//not perfrom deposit further means no execute further code;
                }


                //UPDATING THE DATABASE
                // Persist to Supabase first what ever user ledger first we change in the DB then in inmemory
                // we save the subapaseledger changes in varibale first and use it conditonal as if it happens then do in memory changes

                let db_result= db_deposit(&pool, user_id, &asset_identifier,amount).await;
                //db_result will return either Om or erro so we use it in next things if it s is okay then we update the 
                //in mempry db


                let res= match db_result{
                  Ok(_)=>{
                    let asset_info = resolve_asset_symbol(&asset_identifier).expect("not supported the asset");
                        // .ok_or_else(|| "We do not support this asset".to_string())?;


                    let wallet = get_or_create_user_wallet(&mut db, user_id);
                    let balance_entry = wallet
                        .get_mut(&asset_info.symbol).expect("Asset not supported");
                        // .ok_or_else(|| "Asset balance entry not found".to_string())?;



                    let current_available: f64 = balance_entry.available.parse().unwrap_or(0.0);
                    let new_available = current_available + amount;

                    balance_entry.available =
                        format!("{:.1$}", new_available, asset_info.decimals as usize); //turning into string the balance

                    Ok(BalanceOpResult {
                        updated_balance: (*balance_entry).clone(),
                        asset_info,

                  } )
                   }
                  Err(e) => Err(e.to_string()), 

                };
               
                let _ = resp.send(res);

            }



            BalanceCommand::GetSingleBalance {
                user_id,
                asset_identifier,
                resp,
            } => {
                
                let res = (|| {
                    let asset_info = resolve_asset_symbol(&asset_identifier).expect("not supported the asset");
                        // .ok_or_else(|| "Not supported the asset".to_string())?;

                    let wallet = get_or_create_user_wallet(&mut db, user_id);
                    let balance_entry = wallet
                        .get(&asset_info.symbol)
                        .cloned()
                        .unwrap_or_else(|| {
                            let zero = format!("{:.1$}", 0.0, asset_info.decimals as usize);
                            AssetBalance {
                                available: zero.clone(),
                                locked: zero,
                            }
                        });

                    Ok(BalanceOpResult {
                        updated_balance: balance_entry,
                        asset_info,
                    })
                })();

                let _ = resp.send(res);
            },

            BalanceCommand::GetAllBalances { user_id, resp } => {
                let wallet = get_or_create_user_wallet(&mut db, user_id).clone();
                let _ = resp.send(wallet);
            },

            ////////////////comands for lock and unlock//////
    BalanceCommand::LockFunds { user_id, asset_identifier, amount, resp } => {
    
    if amount<=0.0{
        let _ =resp.send(Err("Lock amount must be greater than 0".to_string()));
        continue;
    }
    

        let asset_info = resolve_asset_symbol(&asset_identifier).expect("not supported the asset");
            // .ok_or_else(|| "Unsupported asset".to_string())?;

        let wallet = get_or_create_user_wallet(&mut db, user_id);
        let balance_entry = wallet
            .get_mut(&asset_info.symbol).expect("wallet does not have this entry");
            // .ok_or_else(|| "Wallet entry not found".to_string())?;

        let available: f64 = balance_entry.available.parse().unwrap_or(0.0);

        if available < amount {
        let _ = resp.send(Err(format!("Insufficient available balancein memory you have: {}, and you need: {}",
        balance_entry.available, amount)));
            continue;
        }

        //SAVING THE MONEY IN DB
        let db_result= db_lock_funds(&pool, user_id, &asset_identifier, amount).await;

        let res= match db_result{
            Ok(_)=>{

           let new_locked=balance_entry.locked.parse().unwrap_or(0.0)+amount;
           let new_available=balance_entry.available.parse().unwrap_or(0.0)-amount;   

           balance_entry.locked=format!("{:.1$}", new_locked, asset_info.decimals as usize);
           balance_entry.available=format!("{:.1$}",new_available, asset_info.decimals as usize);
       Ok(())

            }
            Err(e)=>Err(e.to_string()),
        };

        let _ = resp.send(res);
        
    }
    

BalanceCommand::UnlockFunds { user_id, asset_identifier, amount, resp } => {
    if amount <= 0.0 {
                    let _ = resp.send(Err("Lock amount must be greater than zero".to_string()));
                    continue;
                }


    let asset_info= resolve_asset_symbol(&asset_identifier).expect("not supported the asset");
    // .ok_or_else(|| "we do not support this asset".to_string())?;

    let wallet= get_or_create_user_wallet(&mut db, user_id);
    let balance_entry= wallet.get_mut(&asset_info.symbol).expect("not supported the entry");
    // .ok_or_else(|| "Wallet does not have this asset entries".to_string())?;

    let locked = balance_entry.locked.parse().unwrap_or(0.0);

    if amount>locked {
        let _= resp.send(Err(format!("we could not unlock this amount, your locked : {} and you want:{}", locked, amount)));
        continue; //we are inside the infinte loop so we skip this loop
    }

    let db_result= db_unlock_funds(&pool, user_id, &asset_identifier,amount ).await;

    let res= match db_result{
        Ok(_)=>{

        let new_locked= balance_entry.locked.parse().unwrap_or(0.0)-amount;    
        let new_available= balance_entry.available.parse().unwrap_or(0.0)+amount;
        balance_entry.locked=format!("{:.1$}", new_locked, asset_info.decimals as usize);
        balance_entry.available=format!("{:.1$}",new_available, asset_info.decimals as usize);
        Ok(())
        
        }
        Err(e)=>Err(e.to_string()),
    };

    let _ =resp.send(res);

}


BalanceCommand::ModifyOrderLocks { 
    user_id, asset_identifier, diff, is_increase, resp }=>{

if diff<=0.0{
    let _= resp.send(Err("lock modification difference must be greather than 0".to_string()));
    continue;
}

    let asset_info=resolve_asset_symbol(&asset_identifier).expect("not supported the asset");
    // .ok_or_else(|| "asset not supported".to_string())?;

    let wallet= get_or_create_user_wallet(&mut db, user_id);
    let balance_entry=wallet.get_mut(&asset_info.symbol).expect("not available balance entry");
    // .ok_or_else(|| "Wallet does not have this asset balance entry".to_string())?;

    let available= balance_entry.available.parse().unwrap_or(0.0);
    let locked= balance_entry.locked.parse().unwrap_or(0.0);


//Validation
if is_increase && available<diff{
    let _= resp.send(Err(format!("You do not have enoug available amount:{}, to increase the lock amount:{}", available, diff)));
    continue;
}

if !is_increase && locked<diff{
    let _= resp.send(Err(format!("The amount you want to release is {} and it is more then the amount you locked: {}",diff, locked)));
     continue;
}

//DB ENTRy

let db_result= db_modify_order_locks(&pool, user_id, &asset_identifier, diff,is_increase).await;

let res= match db_result{
    Ok(_)=>{

        //if locking increase
    if is_increase{
        let new_locked= balance_entry.locked.parse().unwrap_or(0.0)+diff;
        let new_available=balance_entry.available.parse().unwrap_or(0.0)-diff;

        balance_entry.locked= format!("{:.1$}", new_locked, asset_info.decimals as usize);
        balance_entry.available= format!("{:.1$}", new_available, asset_info.decimals as usize);
    } else{
        let new_locked= balance_entry.locked.parse().unwrap_or(0.0)-diff;
        let new_available=balance_entry.available.parse().unwrap_or(0.0)+diff;

        balance_entry.locked= format!("{:.1$}", new_locked, asset_info.decimals as usize);
        balance_entry.available= format!("{:.1$}", new_available, asset_info.decimals as usize);

    }
//if locking decrease(order_amount)

        Ok(())

    }
    Err(e)=>Err(e.to_string()),
};

let _= resp.send(res);
}


///////////////////////////////////////////////////////////////////

BalanceCommand::SettleTrade {
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
    resp,
} => {

    let market_str = format!("{:?}", market);

    let db_result:Result<(), sqlx::Error> = db_settle_trade(
    &pool, 
    trade_id as i64,          // Cast u64 to i64
    &market_str,    // Convert MarketPair reference to &str / String
    buyer_id,                 // u32
    seller_id,                // u32
    buyer_order_id as i64,    // Cast u64 to i64
    seller_order_id as i64,   // Cast u64 to i64
    &base_asset,              // &str
    &quote_asset,             // &str
    trade_quantity,           // f64
    execution_price,          // f64
    buyer_limit_price,        // f64
    ).await;



    // let db_result= db_settle_trade(buyer_id, seller_id,base_asset, quote_asset,
    // trade_quantity,execution_price,buyer_limit_price).await;

// let db_result = db_settle_trade(
//     &pool,             // 1. pool reference
//             // 2. trade_id (i64)
//         // 3. market (&str)
//     buyer_id,          // 4. buyer_id (u32)
//     seller_id,         // 5. seller_id (u32)
//     buyer_order_id,    // 6. buyer_order_id (i64)
//     seller_order_id,   // 7. seller_order_id (i64)
//     &base_asset,       // 8. base_asset (&str)
//     &quote_asset,      // 9. quote_asset (&str)
//     trade_quantity,    // 10. trade_quantity (f64)
//     execution_price,   // 11. execution_price (f64)
//     buyer_limit_price, // 12. buyer_limit_price (f64)
// ).await;



    let res=match db_result{
        Ok(_)=>{

            // 1. Resolve asset symbols first
let base_asset_info = resolve_asset_symbol(&base_asset).expect("not supported asset");
let quote_asset_info = resolve_asset_symbol(&quote_asset).expect("not supported asset");

let buyer_amount = execution_price * trade_quantity;
let diff = (buyer_limit_price - execution_price) * trade_quantity;

// --- BUYER WALLET UPDATES ---
{
    let buyer_wallet = get_or_create_user_wallet(&mut db, buyer_id);
    
    // Update buyer base entry (available)
    let buyer_base_entry = buyer_wallet.get_mut(&base_asset_info.symbol).expect("Unsupported asset");
    let buyer_new_base_available = buyer_base_entry.available.parse().unwrap_or(0.0) + trade_quantity;
    buyer_base_entry.available = format!("{:.1$}", buyer_new_base_available, base_asset_info.decimals as usize);

    // Update buyer quote entry (locked & available)
    let buyer_quote_entry = buyer_wallet.get_mut(&quote_asset_info.symbol).expect("unsupported asset");
    let buyer_new_quote_locked = buyer_quote_entry.locked.parse().unwrap_or(0.0) - buyer_amount - diff;
    let buyer_new_quote_available = buyer_quote_entry.available.parse().unwrap_or(0.0) + diff;
    
    buyer_quote_entry.locked = format!("{:.1$}", buyer_new_quote_locked, quote_asset_info.decimals as usize);
    buyer_quote_entry.available = format!("{:.1$}", buyer_new_quote_available, quote_asset_info.decimals as usize);
} // buyer_wallet mutable borrow drops here completely

// --- SELLER WALLET UPDATES ---
{
    let seller_wallet = get_or_create_user_wallet(&mut db, seller_id);

    // Update seller base entry (locked)
    let seller_base_entry = seller_wallet.get_mut(&base_asset_info.symbol).expect("unsupported asset");
    let seller_new_base_locked = seller_base_entry.locked.parse().unwrap_or(0.0) - trade_quantity;
    seller_base_entry.locked = format!("{:.1$}", seller_new_base_locked, base_asset_info.decimals as usize);

    // Update seller quote entry (available)
    let seller_quote_entry = seller_wallet.get_mut(&quote_asset_info.symbol).expect("unsupported asset");
    let new_seller_quote_available = seller_quote_entry.available.parse().unwrap_or(0.0) + buyer_amount;
    seller_quote_entry.available = format!("{:.1$}", new_seller_quote_available, quote_asset_info.decimals as usize);
} // seller_wallet mutable borrow drops here completely

Ok(())



        }
        Err(e)=>Err(e.to_string()),

    };

    let _= resp.send(res);

 },

 BalanceCommand::ReplayDeposit{
                user_id,
                asset_identifier,
                amount,

                ..
            } => {
                
                    let asset_info = resolve_asset_symbol(&asset_identifier).expect("not supported the asset");
                        // .ok_or_else(|| "We do not support this asset".to_string())?;
                    let wallet = get_or_create_user_wallet(&mut db, user_id);
                    let balance_entry = wallet
                        .get_mut(&asset_info.symbol).expect("Asset not supported");
                        // .ok_or_else(|| "Asset balance entry not found".to_string())?;

                    let current_available: f64 = balance_entry.available.parse().unwrap_or(0.0);
                    let new_available = current_available + amount;

                    balance_entry.available =
                        format!("{:.1$}", new_available, asset_info.decimals as usize); //turning into string the balance
                
                   },


                   BalanceCommand::ReplayWithdraw{user_id, asset_identifier, amount,..}=>{
                    // Update RAM balance subtraction without calling Supabase
                   },

    BalanceCommand::ReplaySettleTrade{trade_id,
                            market,
                            buyer_order_id,
                            seller_order_id,
                            buyer_id,
                            seller_id,
                            base_asset,
                            quote_asset,
                            trade_quantity,
                            execution_price,
                            buyer_limit_price,}=>{

let market_str = format!("{:?}", market);
 // 1. Resolve asset symbols first
let base_asset_info = resolve_asset_symbol(&base_asset).expect("not supported asset");
let quote_asset_info = resolve_asset_symbol(&quote_asset).expect("not supported asset");

let buyer_amount = execution_price * trade_quantity;
let diff = (buyer_limit_price - execution_price) * trade_quantity;

// --- BUYER WALLET UPDATES ---
{
    let buyer_wallet = get_or_create_user_wallet(&mut db, buyer_id);
    
    // Update buyer base entry (available)
    let buyer_base_entry = buyer_wallet.get_mut(&base_asset_info.symbol).expect("Unsupported asset");
    let buyer_new_base_available = buyer_base_entry.available.parse().unwrap_or(0.0) + trade_quantity;
    buyer_base_entry.available = format!("{:.1$}", buyer_new_base_available, base_asset_info.decimals as usize);

    // Update buyer quote entry (locked & available)
    let buyer_quote_entry = buyer_wallet.get_mut(&quote_asset_info.symbol).expect("unsupported asset");
    let buyer_new_quote_locked = buyer_quote_entry.locked.parse().unwrap_or(0.0) - buyer_amount - diff;
    let buyer_new_quote_available = buyer_quote_entry.available.parse().unwrap_or(0.0) + diff;
    
    buyer_quote_entry.locked = format!("{:.1$}", buyer_new_quote_locked, quote_asset_info.decimals as usize);
    buyer_quote_entry.available = format!("{:.1$}", buyer_new_quote_available, quote_asset_info.decimals as usize);
} // buyer_wallet mutable borrow drops here completely

// --- SELLER WALLET UPDATES ---
{
    let seller_wallet = get_or_create_user_wallet(&mut db, seller_id);

    // Update seller base entry (locked)
    let seller_base_entry = seller_wallet.get_mut(&base_asset_info.symbol).expect("unsupported asset");
    let seller_new_base_locked = seller_base_entry.locked.parse().unwrap_or(0.0) - trade_quantity;
    seller_base_entry.locked = format!("{:.1$}", seller_new_base_locked, base_asset_info.decimals as usize);

    // Update seller quote entry (available)
    let seller_quote_entry = seller_wallet.get_mut(&quote_asset_info.symbol).expect("unsupported asset");
    let new_seller_quote_available = seller_quote_entry.available.parse().unwrap_or(0.0) + buyer_amount;
    seller_quote_entry.available = format!("{:.1$}", new_seller_quote_available, quote_asset_info.decimals as usize);
} // seller_wallet mutable borrow drops here completely

},
// ==========================================
// 3. SYSTEM PATH
// ==========================================
BalanceCommand::GetSnapshot { reply_to } => {
                let string_db = db.clone();
    

let _ = reply_to.send(string_db);
    
            }
            BalanceCommand::RestoreSnapshot { balances,resp } => {
           
                
                let user_balances = balances;
                //actually this in-memory DB that was empty when server crashed, the we resore it
                //nd this balances we send from the snapshot rrecover method to this workers(before the crash we saved this balance in snapshot, so taking it back)
        let _=resp.send(Ok(()));
              },

BalanceCommand::ReplayLockFunds{user_id,
        asset_identifier,
        amount}=>{

            println!("{:?} {:?} {:?}",user_id,asset_identifier,amount);


    // if amount<=0.0{
    //     let _ =resp.send(Err("Lock amount must be greater than 0".to_string()));
    //     continue;
    // }
    

        let asset_info = resolve_asset_symbol(&asset_identifier).expect("not supported the asset");
            // .ok_or_else(|| "Unsupported asset".to_string())?;

        let wallet = get_or_create_user_wallet(&mut db, user_id);
        // let balance_entry = wallet
        //     .get_mut(&asset_info.symbol).expect("wallet does not have this entry");
        //     // .ok_or_else(|| "Wallet entry not found".to_string())?;

        //if wallet is not ready thenwe manuallycreate it like this below otherwise gives error wallet not (if at this stage wallet not exist)
        let balance_entry = wallet.entry(asset_info.symbol.clone()).or_insert_with(|| {
        // Replace 'BalanceEntry' with your actual struct name
        AssetBalance { 
            available: "0.0".to_string(),
            locked: "0.0".to_string(),
        }
    });

        let available: f64 = balance_entry.available.parse().unwrap_or(0.0);

        // if available < amount {
        // let _ = resp.send(Err(format!("Insufficient available balancein memory you have: {}, and you need: {}",
        // balance_entry.available, amount)));
        //     continue;
        // }


           let new_locked=balance_entry.locked.parse().unwrap_or(0.0)+amount;
           let new_available=balance_entry.available.parse().unwrap_or(0.0)-amount;   

           balance_entry.locked=format!("{:.1$}", new_locked, asset_info.decimals as usize);
           balance_entry.available=format!("{:.1$}",new_available, asset_info.decimals as usize);
     
            
        },

    BalanceCommand::ReplayUnlockFunds { user_id, asset_identifier, amount }=>{

// if amount <= 0.0 {
//                     let _ = resp.send(Err("Lock amount must be greater than zero".to_string()));
//                     continue;
//                 }

    let asset_info= resolve_asset_symbol(&asset_identifier).expect("not supported the asset");
    // .ok_or_else(|| "we do not support this asset".to_string())?;

    let wallet= get_or_create_user_wallet(&mut db, user_id);
    let balance_entry= wallet.get_mut(&asset_info.symbol).expect("not supported the entry");
    // .ok_or_else(|| "Wallet does not have this asset entries".to_string())?;

    let locked = balance_entry.locked.parse().unwrap_or(0.0);

        let new_locked= balance_entry.locked.parse().unwrap_or(0.0)-amount;    
        let new_available= balance_entry.available.parse().unwrap_or(0.0)+amount;
        balance_entry.locked=format!("{:.1$}", new_locked, asset_info.decimals as usize);
        balance_entry.available=format!("{:.1$}",new_available, asset_info.decimals as usize);
       
        
   
   


        }

        //for unmatched command below empty 

            _ => {}
            

    }
              

            }



 }




    



//QUERY FUNCTIOn TO UPDATE SUPABASE LEDGER BEFORE UPDTING INMEMORY BALANCE OF USER
//we used these below function for specific commed before updating the in memory balance of user


// --- 1. Deposit ---
async fn db_deposit(pool: &PgPool, user_id: u32, asset: &str, amount: f64) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO user_balances (user_id, asset, available, locked)
        VALUES ($1, $2, $3, 0.0)
        ON CONFLICT (user_id, asset) 
        DO UPDATE SET available = user_balances.available + EXCLUDED.available,
                      updated_at = NOW()
        "#
    )
    .bind(user_id as i32) // Postgres usually expects standard i32/i64 for IDs
    .bind(asset)
    .bind(amount)
    .execute(pool)
    .await?;
    Ok(())
}

// --- 2. Lock Funds ---
async fn db_lock_funds(pool: &PgPool, user_id: u32, asset: &str, amount: f64) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE user_balances
        SET available = available - $1,
            locked = locked + $1,
            updated_at = NOW()
        WHERE user_id = $2 AND asset = $3 AND available >= $1
        "#
    )
    .bind(amount)
    .bind(user_id as i32)
    .bind(asset)
    .execute(pool)
    .await?;
    Ok(())
}

// --- 3. Unlock Funds ---
async fn db_unlock_funds(pool: &PgPool, user_id: u32, asset: &str, amount: f64) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE user_balances
        SET locked = locked - $1,
            available = available + $1,
            updated_at = NOW()
        WHERE user_id = $2 AND asset = $3 AND locked >= $1
        "#
    )
    .bind(amount)
    .bind(user_id as i32)
    .bind(asset)
    .execute(pool)
    .await?;
    Ok(())
}

// --- 4. Modify Order Locks ---
async fn db_modify_order_locks(
    pool: &PgPool,
    user_id: u32,
    asset: &str,
    diff: f64,
    is_increase: bool,
) -> Result<(), sqlx::Error> {
    if is_increase {
        sqlx::query(
            r#"
            UPDATE user_balances
            SET available = available - $1,
                locked = locked + $1,
                updated_at = NOW()
            WHERE user_id = $2 AND asset = $3 AND available >= $1
            "#
        )
        .bind(diff)
        .bind(user_id as i32)
        .bind(asset)
        .execute(pool).await?;
    } else {
        sqlx::query(
            r#"
            UPDATE user_balances
            SET locked = locked - $1,
                available = available + $1,
                updated_at = NOW()
            WHERE user_id = $2 AND asset = $3 AND locked >= $1
            "#
        )
        .bind(diff)
        .bind(user_id as i32)
        .bind(asset)
        .execute(pool).await?;
    }
    Ok(())
}

// --- 5. Settle Trade ---
async fn db_settle_trade(
    pool: &PgPool,
    trade_id: i64,
    market: &str,
    buyer_id: u32,
    seller_id: u32,
    buyer_order_id: i64,
    seller_order_id: i64,
    base_asset: &str,
    quote_asset: &str,
    trade_quantity: f64,
    execution_price: f64,
    buyer_limit_price: f64,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    
    let total_cost = trade_quantity * execution_price;
    let max_buyer_lock_cost = trade_quantity * buyer_limit_price;
    let price_diff_refund = (buyer_limit_price - execution_price) * trade_quantity;

    // 1. Deduct max buyer lock cost
    sqlx::query(
        r#"
        UPDATE user_balances 
        SET locked = locked - $1, updated_at = NOW()
        WHERE user_id = $2 AND asset = $3 AND locked >= $1
        "#
    )
    .bind(max_buyer_lock_cost)
    .bind(buyer_id as i32)
    .bind(quote_asset)
    .execute(&mut *tx).await?;

    // 2. Refund price difference if any
    if price_diff_refund > 0.0 {
        sqlx::query(
            r#"
            UPDATE user_balances 
            SET locked = locked - $1, available = available + $1, updated_at = NOW()
            WHERE user_id = $2 AND asset = $3 AND locked >= $1
            "#
        )
        .bind(price_diff_refund)
        .bind(buyer_id as i32)
        .bind(quote_asset)
        .execute(&mut *tx).await?;
    }

    // 3. Credit base asset to buyer
    sqlx::query(
        r#"
        INSERT INTO user_balances (user_id, asset, available, locked)
        VALUES ($1, $2, $3, 0.0)
        ON CONFLICT (user_id, asset) 
        DO UPDATE SET available = user_balances.available + EXCLUDED.available, updated_at = NOW()
        "#
    )
    .bind(buyer_id as i32)
    .bind(base_asset)
    .bind(trade_quantity)
    .execute(&mut *tx).await?;

    // 4. Credit quote asset to seller
    sqlx::query(
        r#"
        INSERT INTO user_balances (user_id, asset, available, locked)
        VALUES ($1, $2, $3, 0.0)
        ON CONFLICT (user_id, asset) 
        DO UPDATE SET available = user_balances.available + EXCLUDED.available, updated_at = NOW()
        "#
    )
    .bind(seller_id as i32)
    .bind(quote_asset)
    .bind(total_cost)
    // .execute(&mut *tx).api_call_or_bind? // or execute(&mut *tx).await?;
    .execute(&mut *tx).await?;

    // 5. Deduct base asset from seller lock
    sqlx::query(
        r#"
        UPDATE user_balances 
        SET locked = locked - $1, updated_at = NOW()
        WHERE user_id = $2 AND asset = $3 AND locked >= $1
        "#
    )
    .bind(trade_quantity)
    .bind(seller_id as i32)
    .bind(base_asset)
    .execute(&mut *tx).await?;

    // 6. Insert trade record
    sqlx::query(
        r#"
        INSERT INTO trades (trade_id, market, buyer_order_id, seller_order_id, price, quantity)
        VALUES ($1, $2, $3, $4, $5, $6)
        "#
    )
    .bind(trade_id)
    .bind(market)
    .bind(buyer_order_id)
    .bind(seller_order_id)
    .bind(execution_price)
    .bind(trade_quantity)
    .execute(&mut *tx).await?;

    tx.commit().await?;
    Ok(())
}

//////////////old
// async fn db_settle_trade(
//     pool: &PgPool,
//     trade_id: i64,
//     market: &str,
//     buyer_id: u32,
//     seller_id: u32,
//     base_asset: &str,
//     quote_asset: &str,
//     trade_quantity: f64,
//     execution_price: f64,
// ) -> Result<(), sqlx::Error> {
//     let mut tx = pool.begin().await?;
//     let total_cost = trade_quantity * execution_price;

//     // 1. Credit base asset to Buyer's available balance
//     sqlx::query!(
//         r#"
//         INSERT INTO user_balances (user_id, asset, available, locked)
//         VALUES ($1, $2, $3, 0.0)
//         ON CONFLICT (user_id, asset) 
//         DO UPDATE SET available = user_balances.available + EXCLUDED.available, updated_at = NOW()
//         "#,
//         buyer_id as i32, base_asset, trade_quantity
//     ).execute(&mut *tx).await?;

//     // 2. Credit quote asset (earnings) to Seller's available balance
//     sqlx::query!(
//         r#"
//         INSERT INTO user_balances (user_id, asset, available, locked)
//         VALUES ($1, $2, $3, 0.0)
//         ON CONFLICT (user_id, asset) 
//         DO UPDATE SET available = user_balances.available + EXCLUDED.available, updated_at = NOW()
//         "#,
//         seller_id as i32, quote_asset, total_cost
//     ).execute(&mut *tx).await?;

//     // 3. Deduct base asset from Seller's available balance
//     sqlx::query!(
//         r#"
//         UPDATE user_balances 
//         SET available = available - $1, updated_at = NOW()
//         WHERE user_id = $2 AND asset = $3
//         "#,
//         trade_quantity, seller_id as i32, base_asset
//     ).execute(&mut *tx).await?;

//     tx.commit().await?;
//     Ok(())
// }