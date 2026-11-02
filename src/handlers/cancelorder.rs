// src/handlers/order.rs (or placeorder.rs)

use actix_web::{web, HttpResponse, Responder, HttpRequest, HttpMessage};
use serde::Deserialize;
use crate::db::AppState;
use crate::engine::MarketPair;

#[derive(Deserialize)]
pub struct CancelOrderRequest {
    pub market: MarketPair,
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


    ///////////////////////////////////////////////////////////////

//for cancel order we have unlock the func cmand
// let (quote, base)= match body.market.to_lowercase().as_str(){
//     "solusdc"=>("USD","SOL"),
//     "ethusdc"=>("USD","ETH"),
//     "btcusdc"=>("USD","BTC"),

//         _ => return HttpResponse::BadRequest().json("Invalid or unsupported market pair"),
//     };
// };



////////////////////////////////////////////////

    // 2. Lock app state locks for server & WAL
    let mut server = app_state.server.lock().unwrap();
    let mut wal = app_state.wal.lock().unwrap();

    // 3. Invoke ExchangeServer cancel_order method
    match server.cancelling_order(req_body.market, req_body.order_id, user_id, &mut wal).await {
        Ok(Ok(_)) => HttpResponse::Ok().json(serde_json::json!({
            "status": "success",
            "message": format!("Order #{} successfully cancelled and funds unlocked", req_body.order_id)
        })),
        Ok(Err(err_msg)) => HttpResponse::BadRequest().json(serde_json::json!({
            "status": "error",
            "message": err_msg
        })),
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "status": "error",
            "message": format!("Internal server error: {}", e)
        })),
    }
}