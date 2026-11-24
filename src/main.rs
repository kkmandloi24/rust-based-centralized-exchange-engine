
mod models;
mod engine;
mod utils;
mod middlewares;
mod handlers;
mod db;
mod ledger;
mod wal;
mod ws;

use dashmap::DashMap;
use engine::{MarketPair, MatchingEngine, };
use engine::worker::MarketWorker;
use wal::Wal;
use std::sync::Arc;
use tokio::sync::{mpsc,broadcast};

use actix_web::{web, App, HttpServer};
use actix_web::middleware::from_fn;
use db::AppState;
use ledger::{start_balance_worker, BalanceCommand};
use sqlx::postgres::PgPoolOptions;
use std::env;
use std::time::Duration;




////////////////////////////////////////

//Testing main functionwith postgresql and its working

// #[actix_web::main]
// async fn main() {
//     dotenvy::dotenv().ok();

//     println!("Testing Supabase PostgreSQL connection...");

//     if let Err(error) = test_database_connection().await {
//         eprintln!("Failed to connect to database: {error:#?}");
//     }
// }

// async fn test_database_connection() -> Result<(), sqlx::Error> {
//     let database_url = std::env::var("DATABASE_URL")
//         .expect("DATABASE_URL is missing from .env");

//     let mut connection = PgConnection::connect(&database_url).await?;

//     println!("Connected to Supabase PostgreSQL successfully!");

//     let result: (i32,) = sqlx::query_as("SELECT 1")
//         .fetch_one(&mut connection)
//         .await?;

//     println!("Test query result: {}", result.0);

//     connection.close().await?;

//     Ok(())
// }

///////////////////////////////////////2



/////////////////////////////3
// #[actix_web::main]
// async fn main() -> std::io::Result<()> {

// dotenvy::dotenv().ok();

//     //DB connection
// let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set in .env");


// println!("Connecting to Supabase PostgreSQL...");
// let db_pool = PgPoolOptions::new()
//     .max_connections(15)                            // Low max connections for Supabase pooler
//     .min_connections(5)
//     .acquire_timeout(Duration::from_secs(10))     // Fail fast if connection is starved
//     .idle_timeout(Duration::from_secs(300))        // Drop idle sockets quickly
//     .max_lifetime(Duration::from_secs(60))        // Rotate connection to prevent pooler locks
//     .test_before_acquire(false)                   // Prevents pre-flight ping stalls
//     .connect(&database_url)                       // Pass &String which derefs to &str
//     .await;

// let db_pool = match db_pool {
//     Ok(pool) => {
//         println!("Successfully connected to Supabase!");
//         pool
//     }
//     Err(e) => {
//         eprintln!("Failed to connect to database: {:?}", e);
//         panic!("failed to connect Db: {:?}", e);
//     }
// };


//     // 1. Create the MPSC channel for the balance worker first
//     let (balance_tx, balance_rx) = mpsc::channel(10_000);

//     // 2. Start the background balance worker actor loop/thread
//     tokio::spawn(start_balance_worker(db_pool.clone(), balance_rx));

//     // 3. Define persistence paths
//     let snapshot_file = "exchange.snapshot";
//     let wal_path = "exchange.wal";

    


// let supported_markets= vec![
//     MarketPair::SolUsdc,
//     MarketPair::BtcUsdc,
//      MarketPair::EthUsdc
// ];

// //creating empty Dahsmap
// let market_senders = DashMap::new();

// for market_pair in supported_markets{
// // Create an individual mailbox channel for this market
//     let (market_tx, market_rx)=mpsc::channel(10_000);

//     // Store sender handle in our route map of dhashmap
//     market_senders.insert(market_pair.clone(), market_tx);

//     // TODO: (Optional recovery) Here you can load snapshots/WAL per market if needed, 
//         // e.g., let (engine, wal) = recover_market(&market_pair).await;

// // Call the recovery function passing balance_tx
//     let (engine, wal) = match MarketWorker::recover_market_engine(&market_pair, &balance_tx).await {
//         Ok((eng, w)) => {
//             println!("Successfully recovered and synced engine/WAL for {:?}", market_pair);
//             (eng, w)
//         }
//         Err(e) => {
//             println!("No recovery data for {:?} ({}), initializing fresh...", market_pair, e);
//             let eng = MatchingEngine::new();
//             let wal_path = format!("{:?}.wal", market_pair).to_lowercase();
//             let w = Wal::new(&wal_path)?;
//             (eng, w)
//         }
//     };

//         //////////////////////////

//         // let engine = engine::MatchingEngine::new();
//         // let wal = Wal::new(&wal_path)?;

//         let worker= MarketWorker{
//             market_pair,
//             receiver:market_rx,
//             engine,
//             wal,
//             balance_sender:balance_tx.clone()

//         };

//         // Spawn the worker loop as an independent background Tokio task for each market
//         tokio::spawn(worker.run());
//         println!("Spawned actor worker for market: {:?}", market_pair);

// }


//     // 5. Initialize single AppState containing the server, WAL, and balance sender
//     let app_state = Arc::new(AppState::init(balance_tx, market_senders, db_pool));

//     println!("Starting Actix-web server on 127.0.0.1:3001...");

//     // 6. Start Actix-web HTTP server
//     HttpServer::new(move || {
//         App::new()
//             .app_data(web::Data::from(app_state.clone()))
//             .service(
//                 web::scope("/api/v1/auth")
//                     .route("/signup", web::post().to(handlers::auth::sign_up))
//                     .route("/login", web::post().to(handlers::auth::login)),
//             )
//             .service(
//                 web::scope("/api/v1")
//                     .wrap(from_fn(middlewares::authentication::protect))
//                     .route("getallbalances", web::get().to(handlers::wallet::all_assets_ballances))
//                     .route("/getbalance/{asset}", web::get().to(handlers::wallet::get_asset_balance))
//                     .route("/on-ramp", web::post().to(handlers::wallet::on_ramp))
//                     .route("/deposit-assets", web::post().to(handlers::wallet::deposit_assets))
//                     .route("/placeorder", web::post().to(handlers::placeorder::place_order_handler))
//                     .route("/cancelorder", web::post().to(handlers::cancelorder::cancel_order_handler))
//                     .route("/modifyorder",web::post().to(handlers::modifyorder::modify_order_handler))
//                     .route("/getallorders",web::get().to(handlers::getallorders::getallorders_handler))
                    
//             )
//     })
//     .bind(("127.0.0.1", 3001))?
//     .run()
//     .await
// }

/////////////////////////////////////////////

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenvy::dotenv().ok();

    // DB connection
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set in .env");

    println!("Connecting to Supabase PostgreSQL...");
    let db_pool = PgPoolOptions::new()
        .max_connections(10)
        .min_connections(1)
        .acquire_timeout(Duration::from_secs(30))
        .idle_timeout(Duration::from_secs(300))
        .max_lifetime(Duration::from_secs(1800))
        .test_before_acquire(false)
        .connect(&database_url)
        .await;

    let db_pool = match db_pool {
        Ok(pool) => {
            println!("Successfully connected to Supabase!");
            pool
        }
        Err(e) => {
            eprintln!("Failed to connect to database: {:?}", e);
            panic!("failed to connect Db: {:?}", e);
        }
    };

    // 1. Create the MPSC channel for the balance worker first
    let (balance_tx, balance_rx) = mpsc::channel(10_000);

    // 2. Define global balance persistence paths (Isolated from market WALs)
    let balance_snapshot_path = "balance_snapshot.json".to_string();
    let balance_wal_path = "balance.wal".to_string();

    // 3. Start the background balance worker actor loop with snapshot and WAL paths
    tokio::spawn(start_balance_worker(
        db_pool.clone(), 
        balance_rx, 
        balance_snapshot_path, 
        balance_wal_path
    ));

    let supported_markets = vec![
        MarketPair::SolUsdc,
        MarketPair::BtcUsdc,
        MarketPair::EthUsdc,
    ];

    // Creating empty DashMap for wokers
    let market_senders = DashMap::new();

    //creating intiating Dhasmap for market channel later we create individually for each market
    let market_channel= DashMap::new();

    for market_pair in supported_markets {
        // Create an individual mailbox channel for this market
        let (market_tx, market_rx) = mpsc::channel(10_000);
        let (tx, _rx)=broadcast::channel(1000);

        // Store sender handle in our route map
        market_senders.insert(market_pair.clone(), market_tx);

        //websocket channel communication for sending updated orderbook to browser
        market_channel.insert(market_pair.clone(), tx.clone()); // Buffer capacity of 1000 messages

        // Call the recovery function passing balance_tx
        let (engine, wal) = match MarketWorker::recover_market_engine(&market_pair, &balance_tx).await {
            Ok((eng, w)) => {
                println!("Successfully recovered and synced engine/WAL for {:?}", market_pair);
                (eng, w)
            }
            Err(e) => {
                println!("No recovery data for {:?} ({}), initializing fresh...", market_pair, e);
                let eng = MatchingEngine::new();
                let wal_path = format!("{:?}.wal", market_pair).to_lowercase();
                let w = Wal::new(&wal_path)?;
                (eng, w)
            }
        };

        let worker = MarketWorker {
            market_pair,
            receiver: market_rx,
            engine,
            wal,
            balance_sender: balance_tx.clone(),
            broadcast_tx:tx
        };

        // Spawn the worker loop as an independent background Tokio task for each market
        tokio::spawn(worker.run());
        println!("Spawned actor worker for market: {:?}", market_pair);
    }

    // 5. Initialize single AppState containing the server, WAL, and balance sender
    let app_state = Arc::new(AppState::init(balance_tx, market_senders, market_channel,db_pool ));

    println!("Starting Actix-web server on 127.0.0.1:3001...");

    // 6. Start Actix-web HTTP server
    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::from(app_state.clone()))
            // Public WebSocket endpoint (outside auth middleware)
            .route("/api/v1/ws-market", web::get().to(ws::handler::market_ws_handler))
            .service(
                web::scope("/api/v1/auth")
                    .route("/signup", web::post().to(handlers::auth::sign_up))
                    .route("/login", web::post().to(handlers::auth::login)),
            )
            
            .service(
                web::scope("/api/v1")
                    .wrap(from_fn(middlewares::authentication::protect))
                    .route("/getallbalances", web::get().to(handlers::wallet::all_assets_ballances))
                    .route("/getbalance/{asset}", web::get().to(handlers::wallet::get_asset_balance))
                    .route("/on-ramp", web::post().to(handlers::wallet::on_ramp))
                    .route("/deposit-assets", web::post().to(handlers::wallet::deposit_assets))
                    .route("/placeorder", web::post().to(handlers::placeorder::place_order_handler))
                    .route("/cancelorder", web::post().to(handlers::cancelorder::cancel_order_handler))
                    .route("/modifyorder", web::post().to(handlers::modifyorder::modify_order_handler))
                    .route("/getallorders", web::get().to(handlers::getallorders::getallorders_handler))
                    // .route("/ws-market",web::get().to(ws::handler::market_ws_handler))
            )
    })
    .bind(("127.0.0.1", 3001))?
    .run()
    .await
}




//old code

// mod models;
// mod engine;

// use models::{Order, Side, OrderType, OrderStatus};
// use engine::{OrderBook,MarketPair,MatchingEngine,ExchangeServer};
// use crate::wal::Wal;
// use std::sync::{Arc, Mutex};



// ////////////////////////

// use std::collections::HashMap;
//  use tokio::sync::mpsc;

// // mod models;
// mod utils;
// mod middlewares;
// mod handlers;
// mod db;
// mod worker;
// mod wal;


// use actix_web::{ web, get,post, App, HttpServer,HttpResponse,HttpRequest,HttpMessage, Responder,};
// use actix_web::middleware::from_fn;
// use db::{AppState,TransactionDb};
// // use models::auth::ClientResponse;
// use worker::{start_balance_worker, BalanceCommand};


// #[actix_web::main]
// async fn main() -> std::io::Result<()> {
//          let mut orderbook = OrderBook::new();


//     let wal_path = "exchange.wal";
//     let snapshot_file = "exchange.snapshot";
    

//     // Boot up server and automatically recover past states
//     // let (mut server, mut wal) = ExchangeServer::new_with_recovery(wal_path)?;
//     let (mut server, mut wal) = ExchangeServer::new_with_snapshot_and_recovery(snapshot_file, wal_path,)?;

//     // Example live order coming in from an API client:
//     // let new_order = Order {
//     //     id: 0, // Will be overwritten by ExchangeServer sequence generator
//     //     price: 150.50,
//     //     quantity: 10.0,
//     //     remaining_quantity: 10.0,
//     //     order_type: OrderType::Limit,
//     //     side: Side::Buy,
//     //     status: OrderStatus::New,
//     //     timestamp: std::time::SystemTime::now()
//     //         .duration_since(std::time::UNIX_EPOCH)
//     //         .unwrap()
//     //         .as_nanos() as u64,
//     // };

//     // // 1. Place a Sell Order (Ask) at $100 for 2.0 BTC
//     // let sell_order = Order {
//     //     id: 1,
//     //     price: 100.0,
//     //     quantity: 2.0,
//     //     remaining_quantity: 2.0,
//     //     side: Side::Sell,
//     //     order_type: OrderType::Limit,
//     //     status: OrderStatus::New,
//     //     timestamp: 1,
//     // };

//     //     println!("Adding sell order...");
//     // let trades = orderbook.add_order(sell_order);
//     // println!("Trades generated: {:?}", trades);
   


//      // 2. Place a matching Buy Order (Bid) at $100 for 1.5 BTC
//     // let buy_order = Order {
//     //     id: 2,
//     //     price: 100.0,
//     //     quantity: 1.5,
//     //     remaining_quantity: 1.5,
//     //     side: Side::Buy,
//     //     order_type: OrderType::Limit,
//     //     status: OrderStatus::New,
//     //     timestamp: 2,
//     // };

//     //     println!("Adding matching buy order...");
//     // let trades = orderbook.add_order(buy_order);
//     // println!("Trades generated: {:?}", trades);


// // Place the order securely through the server (handles WAL + Engine)
//     server.place_order(MarketPair::SolUsdc, new_order, &mut wal).await;
   

//   println!("here is the orderbook {:?}",orderbook);

//    dotenvy::dotenv().ok();

//    // 2. Recover or initialize exchange state once
//    let snapshot_file = "exchange.snapshot";
//    let wal_path = "exchange.wal";
//    let (server, wal) = match ExchangeServer::new_with_snapshot_and_recovery(snapshot_file, wal_path, balance_tx) {
//        Ok((srv, w)) => {
//            println!("Successfully recovered exchange state from disk.");
//            (srv, w)
//        }
//        Err(e) => {
//            println!("No snapshot found ({}), initializing fresh exchange...", e);
//            let srv = ExchangeServer::new(balance_tx);
//            let w = Wal::new(wal_path)?;
//            (srv, w)
//        }
//    };

//    // 3. Start the background balance worker channel (Actor Model)
//    let (balance_tx, balance_rx) = mpsc::channel(1000);
//    tokio::spawn(start_balance_worker(balance_rx));

//    // 4. Initialize single AppState
// //    let app_state = Arc::new(AppState::init(balance_tx, server, wal));
//    let app_state = Arc::new(AppState::init(balance_tx, server, wal));

//    println!("Starting Actix-web server on 127.0.0.1:3001...");
//     // println!("Starting Actix-web server on 127.0.0.1:8080...");

//    HttpServer::new(move|| {
//        App::new()
//        .app_data(web::Data::from(app_state.clone()))
//             // .app_data(server_state.clone()) //using app data to making the data available  global 
//            // .wrap(from_fn(my_middleware))
//            .service(
//                web::scope("/api/v1/auth")
//                    .route("/signup", web::post().to(handlers::auth::sign_up))
//                    .route("/login", web::post().to(handlers::auth::login)), )
//            .service(
//                web::scope("/api/v1")
//                .wrap(from_fn(middlewares::authentication::protect))
//                .route("getallbalances",web::get().to(handlers::wallet::all_assets_ballances))
//                .route("/getbalance/{asset}", web::get().to(handlers::wallet::get_asset_balance))
//                .route("/on-ramp", web::post().to(handlers::wallet::on_ramp))
//                .route("/deposit-assets", web::post().to(handlers::wallet::deposit_assets))
//                .route("/placeorder", web::post().to(handlers::placeorder::place_order_handler))
             
//            )


//            })
//    .bind(("127.0.0.1", 3001))?
//    .run().await?;
// //  server.create_snapshot(snapshot_file,wal_path)?;
//  // Lock the server from AppState to run the snapshot
// // app_state.server.lock().unwrap().create_snapshot(snapshot_file, wal_path)?;
// Ok(())
//           }