// src/handlers/order.rs (or placeorder.rs)
use tokio::sync::oneshot;
use actix_web::{web, HttpResponse, Responder, HttpRequest, HttpMessage};
use serde::Deserialize;
use crate::db::AppState;
use crate::engine::MarketPair;
use crate::engine::command::MarketCommand;


#[derive(Deserialize)]
pub struct CancelOrderRequest {
    pub market: String,
    pub order_id: u64,

}


pub async fn cancel_order_handler(
    app_state: web::Data<AppState>,
    req: HttpRequest,
    req_body: web::Json<CancelOrderRequest>,
) -> impl Responder {
    // 1. Extract authenticated user_id from request extensions (set by auth middleware)
    let user_id = match req.extensions().get::<u32>() {
        Some(id) => *id,
        None => return HttpResponse::Unauthorized().json("Unauthorized access"),
    };

    // 2. Parse the market string into your MarketPair enum
    let market_pair = match req_body.market.to_lowercase().as_str() {
        "solusdc" => MarketPair::SolUsdc,
        "btcusdc" => MarketPair::BtcUsdc,
        "ethusdc" => MarketPair::EthUsdc,
        _ => return HttpResponse::BadRequest().json("Invalid or unsupported market pair"),
    };

    // 3. Look up the mpsc sender for this specific market worker from the DashMap
    let sender = match app_state.market_senders.get(&market_pair) {
        Some(entry) => entry.value().clone(),
        None => return HttpResponse::BadRequest().json(format!("No active worker found for market: {:?}", market_pair)),
    };

    // 4. Create a oneshot channel to receive the worker's Result<(), String> response
    let (resp_tx, resp_rx) = oneshot::channel();

    // 5. Construct the command to send to the MarketWorker actor
    let cmd = MarketCommand::CancelOrder {
        order_id: req_body.order_id,
        user_id,
        respond_to: resp_tx,
    };

    // 6. Send the command into the actor's mailbox channel
    if let Err(_) = sender.send(cmd).await {
        return HttpResponse::InternalServerError().json("Market worker channel closed");
    }

    // 7. Await the response from the MarketWorker actor
    match resp_rx.await {
        Ok(Ok(())) => HttpResponse::Ok().json(serde_json::json!({
            "status": "success",
            "message": format!("Order #{} successfully cancelled and funds unlocked", req_body.order_id)
        })),
        //workersend but cancelling_order method gives error
        Ok(Err(err_msg)) => HttpResponse::BadRequest().json(serde_json::json!({
            "status": "error",
            "message": err_msg
        })),
        Err(_) => HttpResponse::InternalServerError().json(serde_json::json!({
            "status": "error",
            "message": "Market worker dropped response channel"
        })),
    }
}


//////////////////

// pub async fn cancel_order_handler(
//     app_state: web::Data<AppState>,
//     req: HttpRequest,
//     req_body: web::Json<CancelOrderRequest>,
// ) -> impl Responder {
//     // 1. Extract authenticated user_id from request extensions (set by auth middleware)
//     let user_id = match req.extensions().get::<u32>() {
//         Some(id) => *id,
//         None => return HttpResponse::Unauthorized().json("Unauthorized access"),
//     };


//     ///////////////////////////////////////////////////////////////

// //for cancel order we have unlock the func cmand
// // let (quote, base)= match body.market.to_lowercase().as_str(){
// //     "solusdc"=>("USD","SOL"),
// //     "ethusdc"=>("USD","ETH"),
// //     "btcusdc"=>("USD","BTC"),

// //         _ => return HttpResponse::BadRequest().json("Invalid or unsupported market pair"),
// //     };
// // };

// ////////////////////////////////////////////////////












// ////////////////////////////////////////////////

//     // 2. Lock app state locks for server & WAL
//     // let mut server = app_state.server.lock().unwrap();
//     // let mut wal = app_state.wal.lock().unwrap();

//     // 3. Invoke ExchangeServer cancel_order method
//     match MarketWorker::cancelling_order(req_body.market, req_body.order_id, user_id).await {
//         Ok(Ok(_)) => HttpResponse::Ok().json(serde_json::json!({
//             "status": "success",
//             "message": format!("Order #{} successfully cancelled and funds unlocked", req_body.order_id)
//         })),
//         Ok(Err(err_msg)) => HttpResponse::BadRequest().json(serde_json::json!({
//             "status": "error",
//             "message": err_msg
//         })),
//         Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
//             "status": "error",
//             "message": format!("Internal server error: {}", e)
//         })),
//     }
// }