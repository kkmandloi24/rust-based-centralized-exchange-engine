
use actix_web::{web, HttpMessage, HttpRequest, HttpResponse, Responder};
use chrono::Utc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::oneshot;

use crate::wal::{EngineCommand};
use crate::db::{AppState, Status, TransactionRecord, Type};
use crate::models::wallet::{
    BalanceResponse, DepositRequest, ErrorResponse, OnrampRequest, SingleAssetResponse,
    TransactionResponse,
};
use crate::worker::BalanceCommand;

// 1. ALL ASSETS BALANCES HANDLER FUNCTION
pub async fn all_assets_ballances(
    req: HttpRequest,
    app_state: web::Data<AppState>,
) -> impl Responder {
    let user_id = match req.extensions().get::<u32>().copied() {
        Some(id) => id,
        None => return HttpResponse::Unauthorized().json("Unauthorized"),
    };

    let (tx, rx) = oneshot::channel();

    if app_state
        .balance_tx
        .send(BalanceCommand::GetAllBalances { user_id, resp: tx })
        .await
        .is_err()
    {
        
        return HttpResponse::InternalServerError().json("Worker unavailable");
    }

    match rx.await {
        Ok(balances) => HttpResponse::Ok().json(BalanceResponse { user_id, balances }),
        Err(_) => HttpResponse::InternalServerError().json("Channel closed"),
    }
}

// 2. ON-RAMP (USD DEPOSIT)
pub async fn on_ramp(
    body: web::Json<OnrampRequest>,
    req: HttpRequest,
    app_state: web::Data<AppState>,
) -> impl Responder {
    let user_id = match req.extensions().get::<u32>().copied() {
        Some(id) => id,
        None => return HttpResponse::Unauthorized().json("Unauthorized"),
    };

    let amount = body.amount as f64;
    if amount <= 0.0 {
        return HttpResponse::BadRequest().json(ErrorResponse {
            message: "Amount must be greater than 0".to_string(),
        });
    }

    let (tx, rx) = oneshot::channel();

    let cmd = BalanceCommand::Deposit {
        user_id,
        asset_identifier: "USD".to_string(),
        amount,
        resp: tx,
    };

    if app_state.balance_tx.send(cmd).await.is_err() {
        return HttpResponse::InternalServerError().json("Worker unavailable");
    }

    let op_result = match rx.await {
        Ok(Ok(res)) => res,
        Ok(Err(err_msg)) => return HttpResponse::NotFound().json(err_msg),
        Err(_) => return HttpResponse::InternalServerError().json("Channel closed"),
    };

    // Building Transaction Logging
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
    let txt_id = format!("tx_{}_{}", now, rand::random::<u16>() % 1000);
    let transaction_record = TransactionRecord {
        id: txt_id.clone(),
        user_id,
        payment_type: Type::OnRamp,
        asset_symbol: op_result.asset_info.symbol,
        payment_method: None,
        amount: amount as usize,
        status: Status::Successful,
        created_at: Utc::now().to_rfc3339(),
    };

    {
        let mut transaction_log_db = app_state.txn_log_db.lock().unwrap();
        transaction_log_db.add_transaction(user_id, &transaction_record);
    }

    HttpResponse::Ok().json(TransactionResponse {
        transaction_log: transaction_record,
        user_id,
        asset: op_result.asset_info,
        balance: op_result.updated_balance,
    })
}




// 3. DEPOSIT ASSETS (ANY ASSET)
pub async fn deposit_assets(
    req: HttpRequest,
    body: web::Json<DepositRequest>,
    app_state: web::Data<AppState>,
) -> impl Responder {
    let user_id = match req.extensions().get::<u32>().copied() {
        Some(id) => id,
        None => return HttpResponse::Unauthorized().json("Unauthorized"),
    };

    let amount = body.amount as f64;
    if amount <= 0.0 {
        return HttpResponse::BadRequest().json(ErrorResponse {
            message: "Amount must be greater than 0".to_string(),
        });
    }

    let (tx, rx) = oneshot::channel();

    let cmd = BalanceCommand::Deposit {
        user_id,
        asset_identifier: body.asset.clone(),
        amount,
        // payment_type: Type::Deposit,
        resp: tx,
    };

    //Command for the wal
    let command= EngineCommand::Deposit { user_id, asset: body.asset.clone(), amount };

    if app_state.balance_tx.send(cmd).await.is_err() {
        return HttpResponse::InternalServerError().json("Worker unavailable");


        }

        {
            let mut wal= app_state.wal.lock().unwrap_or_else(|e| e.into_inner());;
            if let Err(e)= wal.append(&command){
                eprintln!("Failed to append deposit to WAL: {}", e);
            return HttpResponse::InternalServerError().finish();
        }
            }
    


    let op_result = match rx.await {
        Ok(Ok(res)) => res,
        Ok(Err(err_msg)) => return HttpResponse::NotFound().json(err_msg),
        Err(_) => return HttpResponse::InternalServerError().json("Channel closed"),
    };

    // Transaction Logging
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
    let txt_id = format!("tx_{}_{}", now, rand::random::<u16>() % 1000);
    let transaction_record = TransactionRecord {
        id: txt_id.clone(),
        user_id,
        payment_type: Type::Deposit,
        asset_symbol: op_result.asset_info.symbol,
        payment_method: None,
        amount: amount as usize,
        status: Status::Successful,
        created_at: Utc::now().to_rfc3339(),
    };

    {
        let mut transaction_log_db = app_state.txn_log_db.lock().unwrap();
        transaction_log_db.add_transaction(user_id, &transaction_record);
    }

    HttpResponse::Ok().json(TransactionResponse {
        transaction_log: transaction_record,
        user_id,
        asset: op_result.asset_info,
        balance: op_result.updated_balance,
    })
}

// 4. GET SINGLE ASSET BALANCE
pub async fn get_asset_balance(
    req: HttpRequest,
    app_state: web::Data<AppState>,
    path: web::Path<String>,
) -> impl Responder {
    let user_id = match req.extensions().get::<u32>().copied() {
        Some(id) => id,
        None => return HttpResponse::Unauthorized().json("Unauthorized"),
    };

    let (tx, rx) = oneshot::channel();

    let cmd = BalanceCommand::GetSingleBalance {
        user_id,
        asset_identifier: path.into_inner(),
        resp: tx,
    };

    if app_state.balance_tx.send(cmd).await.is_err() {
        return HttpResponse::InternalServerError().json("Worker unavailable");
    }

    match rx.await {
        Ok(Ok(res)) => HttpResponse::Ok().json(SingleAssetResponse {
            user_id,
            asset: res.asset_info,
            balance: res.updated_balance,
        }),
        Ok(Err(err_msg)) => HttpResponse::NotFound().json(err_msg),
        Err(_) => HttpResponse::InternalServerError().json("Channel closed"),
    }
}



////////////////////////////////////////////////////////////////////////////////////////

// use actix_web::{web, HttpRequest, HttpResponse, Responder, HttpMessage};
// use crate::db::{AppState, get_or_create_user_wallet,resolve_asset_symbol,TransactionRecord,Type,AssetSymbol,Status};
// use crate::models::wallet::{BalanceResponse,DepositRequest,TransactionResponse,OnrampRequest,ErrorResponse,SingleAssetResponse};
// use chrono::Utc;
// use std::time::{SystemTime,UNIX_EPOCH};


// pub async fn all_assets_ballances(
//    req: HttpRequest,
//    app_state: web::Data<AppState>,
// ) -> impl Responder {
//    // 1. Get user_id from middleware extensions
//    let user_id = match req.extensions().get::<u32>().copied() {
//        Some(id) => id,
//        None => return HttpResponse::Unauthorized().json("Unauthorized"),
//    };


//    // 2. Lock the database safely
//    let mut db = match app_state.db.lock() {
//        Ok(db) => db,
//        Err(_) => {
//            return HttpResponse::InternalServerError().json("Database lock error");
//        }
//    };


//    // 3. Clone the HashMap so it owns the data independently of the lock
//    let wallet = get_or_create_user_wallet(&mut db, user_id).clone();


//    // 4. Return response (the lock `db` automatically unlocks here)
//    HttpResponse::Ok().json(BalanceResponse {
//        user_id,
//        balances: wallet,
//    })
// }


// ////ONRAMP

// pub async fn on_ramp(body: web::Json<OnrampRequest>,req: HttpRequest, app_state: web::Data<AppState>)->impl Responder{
//    let user_id= match req.extensions().get::<u32>().copied(){
//        Some(id)=>id,
//          None => return HttpResponse::Unauthorized().json("Unauthorized"),


//    };

// //becuase body send numeric value
//   //  let amount:f64= body.amount.parse().unwrap_or(0.0); //this works for string
//    let amount: f64 = body.amount as f64;


//    if amount <=0.0 {
//        return HttpResponse::BadRequest().json(ErrorResponse{
//            message:"Amount must be greatr than 0". to_string(),
//        })
//    };


//    //Asset USD selection


//    // let asset_info= match resolve_asset_symbol("USD"){
//    //     Some(asset)=>asset,
//    //     None=>HttpResponse::InternalServerError().json("USD asset not configured"),
//    // };




//      let asset_info = match resolve_asset_symbol("USD") {
//        Some(asset) => asset,
//        None => return HttpResponse::NotFound().json("Not suported the asset")
//    };


// //Lock DB
//    let mut db= app_state.db.lock().unwrap();


//    ///now we check if the user has wallet /if not create it
//    let user_wallet= get_or_create_user_wallet(&mut db,user_id);




//   // 1. Get mutable reference to the user's USD balance struct
// let usd_balance = match user_wallet.get_mut(&asset_info.symbol) {
//    Some(balance) => balance,
//    None => return HttpResponse::NotFound().json("USD balance entry not found"),
// };


// // 2. Parse current available String to f64
// let current_available: f64 = usd_balance.available.parse().unwrap_or(0.0);


// // 3. Add body amount (if body.amount is f64)
// let new_available = current_available + amount;


// // 4. Update the struct's available field in-place
// usd_balance.available = format!("{:.1$}", new_available, asset_info.decimals as usize);


// // 5. Clone the updated balance AFTER the modification
// let updated_balance = usd_balance.clone();


// //curren time
// let now=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
// //   tx_${Date.now()}_${Math.floor(Math.random() * 1000)} used js techninc and creted the transaction id
// let txt_id= format!("tx_{},_ {}",now, rand::random::<u16>()%1000);
//  let transaction_record= TransactionRecord{
//    id:txt_id.clone(),
//    user_id,
//    payment_type:Type::Deposit,
//    asset_symbol:asset_info.symbol,
//    payment_method:None,
//    amount:amount as usize ,
//    status:Status::Successful,
//    created_at:Utc::now().to_rfc3339(),
//  };


// //making the transaction record for user
// let mut transaction_log_db= app_state.txn_log_db.lock().unwrap();


// transaction_log_db.add_transaction(user_id, &transaction_record);


// //testing in console we get all transaction of. user using get_trnsactionmethod
// let records= transaction_log_db.get_transaction(user_id,&txt_id);
// println!("users 1's all transaction: {:?}",records);



// // 6. Return response
// HttpResponse::Ok().json(TransactionResponse {
//    transaction_log: transaction_record,
//    user_id,
//    asset: asset_info,
//    balance: updated_balance,
// })


// }


// pub async fn deposit_assets(req:HttpRequest,body: web::Json<DepositRequest>, app_state: web::Data<AppState>)->
// impl Responder{


//    let user_id= match req.extensions().get::<u32>().copied(){
//        Some(id)=>id,
//          None => return HttpResponse::Unauthorized().json("Unauthorized"),


//    };


//    let asset= &body.asset;
//   //  let amount=body.amount.parse().unwrap_or(0.0);
//   let amount:f64= body.amount as f64;
  
//    if amount <=0.0{
//        return HttpResponse::BadRequest().json(ErrorResponse{
//            message:"Amount must be greatr than 0". to_string(),
//        })


//    };


//    let asset_info= match resolve_asset_symbol(&asset){
//        Some(a)=>a,
//        None=>return HttpResponse::NotFound().json("We do not support this asset")
//    };


// let mut db= app_state.db.lock().unwrap();
// let user_wallet= get_or_create_user_wallet(&mut db, user_id);


// let user_balance=match user_wallet.get_mut(&asset_info.symbol){
//    Some(b)=>b,
//    None => return HttpResponse::NotFound().json("USD balance entry not found")
// };


// let user_available_balance:f64= user_balance.available.parse().unwrap_or(0.0);


// let user_final_balance= user_available_balance + amount;


// //updating balance in format, in the user_balance
// user_balance.available= format!("{:.1$}",user_final_balance, asset_info.decimals as usize);


// let updated_balance=user_balance.clone();

// /////transaction log for deposit

// //curren time
// let now=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
// //   tx_${Date.now()}_${Math.floor(Math.random() * 1000)} used js techninc and creted the transaction id
// let txt_id= format!("tx_{},_ {}",now, rand::random::<u16>()%1000);
//  let transaction_record= TransactionRecord{
//    id:txt_id.clone(),
//    user_id,
//    payment_type:Type::Deposit,
//    asset_symbol:asset_info.symbol,
//    payment_method:None,
//    amount:amount as usize ,
//    status:Status::Successful,
//    created_at:Utc::now().to_rfc3339(),
//  };


// //making the transaction record for user
// let mut transaction_log_db= app_state.txn_log_db.lock().unwrap();

// transaction_log_db.add_transaction(user_id, &transaction_record);
// //testing in console we get all transaction of. user using get_trnsactionmethod
// let records= transaction_log_db.get_transaction(user_id,&txt_id);
// println!("users 1's all transaction: {:?}",records);


// // 6. Return response
// HttpResponse::Ok().json(TransactionResponse {
//    transaction_log: transaction_record,
//    user_id,
//    asset: asset_info,
//    balance: updated_balance,
// })

// // HttpResponse::Ok().json(TransactionResponse { transaction_log: (), user_id, asset: (), balance: () }Response{
   
// //    user_id,
// //    asset:target_asset,
// //    balance: updated_balance,


// // })


// }



// //Get a spscific asse Balance

// pub async fn get_asset_balance(
//     req: HttpRequest,
//     app_state: web::Data<AppState>,
//     path: web::Path<String>,
// ) -> impl Responder {
//     let asset_info = match resolve_asset_symbol(&path) {
//         Some(asset) => asset,
//         None => return HttpResponse::NotFound().json("Not supported the asset"),
//     };

//     let user_id = match req.extensions().get::<u32>().copied() {
//         Some(id) => id,
//         None => return HttpResponse::Unauthorized().json("Unauthorized"),
//     };

//     let mut db = match app_state.db.lock() {
//         Ok(db) => db,
//         Err(_) => return HttpResponse::InternalServerError().json("Database lock error"),
//     };

//     let balance = get_or_create_user_wallet(&mut db, user_id)
//         .get(&asset_info.symbol)
//         .clone()
//         .unwrap();

//    HttpResponse::Ok().json(SingleAssetResponse{
//         user_id: user_id,
//         asset: asset_info,
//         balance: balance.clone() // Converts f64 to String
//     })
// }