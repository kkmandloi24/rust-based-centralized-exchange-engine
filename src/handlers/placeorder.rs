
use crate::models::{SignupRequest,SignupResponse,ClientResponse, Order,Side,OrderType, OrderStatus};
use actix_web::{web, HttpMessage,HttpRequest, HttpResponse, Responder};
use crate::db::AppState;
use serde::{Serialize, Deserialize};

use crate::engine::{MarketPair,MatchingEngine,ExchangeServer};
use crate::wal::{Wal};

use tokio::sync::oneshot;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::BalanceCommand;


#[derive(Deserialize)]
pub struct OrderRequest {
    pub market: String,       // e.g., "SolUsdc"
    pub side: Side,           // Buy or Sell
    pub order_type: OrderType,// Limit or Market
    pub price: f64,
    pub quantity: f64,
}


pub async fn place_order_handler(
    state: web::Data<AppState>,
    body: web::Json<OrderRequest>,
    req: HttpRequest
) -> impl Responder {



   
    let user_id = match req.extensions().get::<u32>().copied() {
        Some(id) => id,
        None => return HttpResponse::Unauthorized().json("Unauthorized"),
    };

    // 1. Determine base and quote assets
    let (base_asset, quote_asset) = match body.market.to_lowercase().as_str() {
        "solusdc" => ("SOL", "USD"),
        "btcusdc" => ("BTC", "USD"),
        "ethusdc" => ("ETH", "USD"),
        _ => return HttpResponse::BadRequest().json("Invalid or unsupported market pair"),
    };

    


    //we use quoate(USD) asset to buy the baseasset(SOl/BTC/ETH) of seller
//kisko lock karna hai vo batata hai below code
    // 2. Calculate required asset and amount to lock
    let (asset_to_lock, lock_amount) = match body.side {
        Side::Buy => (quote_asset, body.price * body.quantity),
        Side::Sell => (base_asset, body.quantity),
    };

    // 3. Request background balance worker to safely lock funds (Pre-trade Risk Check)
    let (resp_tx, resp_rx) = oneshot::channel();
    let _ = state.balance_tx.send(BalanceCommand::LockFunds {
        user_id,
        asset_identifier: asset_to_lock.to_string(), //it tells which asset we are locking(USD/SOl?TC/ETH absed on theisde)
        amount: lock_amount,
        resp: resp_tx,
    }).await;

    match resp_rx.await {
        Ok(Ok(_)) => { /* Funds successfully locked! */ }
        Ok(Err(err_msg)) => return HttpResponse::BadRequest().json(err_msg), //comunicatin success not lcked balance
        Err(_) => return HttpResponse::InternalServerError().json("Balance worker communication failed"),
    }

    // 4. Construct the Order
    let current_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    let order = Order {
        id: 0,   //by default given 0 becuase matching engine give the order id
        user_id,
        price: body.price,
        quantity: body.quantity,
        remaining_quantity: body.quantity,
        order_type: body.order_type,
        side: body.side,
        status: OrderStatus::New,
        timestamp: current_timestamp,
    };

    //now we send the order in the exchange server with marketpair pair and order 

    let market_pair = match body.market.to_lowercase().as_str() {
        "solusdc" => MarketPair::SolUsdc,
        "btcusdc" => MarketPair::BtcUsdc,
        "ethusdc" => MarketPair::EthUsdc,
        _ => unreachable!(),
    };

    // 5. Access the shared server and WAL directly from AppState (No re-initializing!)
    let mut server = state.server.lock().unwrap();
    let mut wal = state.wal.lock().unwrap();

    let place_result = match server.place_order(market_pair, order, &mut wal).await {
        Ok(res) => res,
        Err(io_err) => {
            return HttpResponse::InternalServerError().json(format!("WAL/IO error: {}", io_err));
        }
    };

    if let Err(engine_err) = place_result {
        // SAFETY ROLLBACK: Unlock funds if matching engine fails
        let (rollback_tx, rollback_rx) = oneshot::channel();
        let _ = state.balance_tx.send(BalanceCommand::UnlockFunds {
            user_id,
            asset_identifier: asset_to_lock.to_string(),
            amount: lock_amount,
            resp: rollback_tx,
        }).await;
        let _ = rollback_rx.await;

        return HttpResponse::BadRequest().json(format!("Matching engine error: {}", engine_err));
    }

    

    // if let Err(e) = server.place_order(market_pair, order, &mut wal).await {
    //     // SAFETY ROLLBACK: Unlock funds if matching engine fails
    //     let (rollback_tx, rollback_rx) = oneshot::channel();
    //     let _ = state.balance_tx.send(BalanceCommand::UnlockFunds {
    //         user_id,
    //         asset_identifier: asset_to_lock.to_string(),
    //         amount: lock_amount,
    //         resp: rollback_tx,
    //     }).await;
    //     let _ = rollback_rx.await;

    //     return HttpResponse::InternalServerError().json(format!("Matching engine error: {}", e));
    // }

    // 6. Return success response
    HttpResponse::Ok().json(serde_json::json!({
        "status": "Success",
        "message": "Order placed and funds locked successfully",
        "user_id": user_id,
        "market": body.market,
        "side": body.side,
        "price": body.price,
        "quantity": body.quantity,
       
    }))
}



