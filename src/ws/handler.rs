use actix_web::{web, Error, HttpRequest, HttpResponse};
use actix_ws::Message;
use futures_util::StreamExt;
use std::sync::Arc;
use crate::ws::messages::{MarketEvent,ClientCommand};
// Assuming MarketPair and AppState are defined in your project
use crate::AppState; 
use crate::engine::{ MarketPair};
use crate::engine::command::{MarketCommand};

pub async fn market_ws_handler(
    req: HttpRequest,
    stream: web::Payload,
    state: web::Data<AppState>,
) -> Result<HttpResponse, Error> {

    // THIS LINE WILL PRINT IF THE ROUTE IS REACHED
    println!(">>> HIT MARKET WS HANDLER FOR PATH: {}", req.path());
    // 1. Perform the HTTP-to-WebSocket handshake
    // let (res, mut session, mut msg_stream) = actix_ws::handle(&req, stream)?;
    let (res, mut session, mut msg_stream) = match actix_ws::handle(&req, stream) {
        Ok(tuple) => tuple,
        Err(e) => {
            eprintln!("WebSocket handshake rejected: {:?}", e);
            return Err(e);
        }
    };

    // 2. Spawn an asynchronous task to manage the individual client connection
    actix_web::rt::spawn(async move {
        let mut active_receiver: Option<tokio::sync::broadcast::Receiver<MarketEvent>> = None;

        // 3. Listen for incoming client frames (e.g. subscription commands)
        // while let Some(Ok(msg)) = msg_stream.next().await {
        //     match msg {
        //         Message::Text(text) => {
        //             // Parse the incoming JSON command from the React frontend
        //             if let Ok(command) = serde_json::from_str::<ClientCommand>(&text) {
        //                 if command.action == "subscribe" {
        //                     // Look up the market's broadcast sender in your DashMap
        //                     if let Some(sender) = state.market_channel.get(&command.market) {
        //                         active_receiver = Some(sender.subscribe());
        //                         let _ = session.text("Subscribed successfully").await;
        //                     } else {
        //                         let _ = session.text("Market not found").await;
        //                     }
        //                 }
        //             }
        //         }
        //         Message::Ping(bytes) => {
        //             if session.pong(&bytes).await.is_err() {
        //                 break;
        //             }
        //         }
        //         Message::Close(_) => break,
        //         _ => {}
        //     }

        //     // If a channel subscription is active, check for new market events to stream
        //     if let Some(ref mut rx) = active_receiver {
        //         // Non-blocking check or asynchronous select can be structured here
        //         // to forward events from `rx.recv().await` straight to `session.text(...)`
        //     }
        // }

        loop {
            tokio::select! {
                // 1. Handle incoming messages FROM the client (Browser -> Server)
                Some(msg_result) = msg_stream.next() => {
                    match msg_result {
                        Ok(Message::Text(text)) => {
                            if let Ok(command) = serde_json::from_str::<ClientCommand>(&text) {
                                if command.action == "subscribe" {

                                //changing the marker enum command 
                                let market_enum = match command.market.to_uppercase().as_str() {
    "SOLUSDC" | "SOL-USDC" => MarketPair::SolUsdc,
    "BTCUSDC" | "BTC-USDC" => MarketPair::BtcUsdc,
    "ETHUSDC" | "ETH-USDC" => MarketPair::EthUsdc,
    _ => {
        // Handle invalid market error or continue
        continue;
    }
};


// 1. Get the broadcast sender for live updates
    if let Some(broadcast_tx) = state.market_channel.get(&market_enum) {
        // Subscribe to live broadcast *first* so you don't miss anything
        active_receiver = Some(broadcast_tx.subscribe());
        let _ = session.text("Subscribed successfully").await;

        // 2. GET THE MARKET WORKER COMMAND SENDER (Assuming you store this in AppState too)
        // e.g., state.market_command_senders.get(&market_enum)
        if let Some(worker_tx) = state.market_senders.get(&market_enum) {
            // Create a oneshot channel for the snapshot
            let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();

            // Send GetSnapshot command to the MarketWorker
            if worker_tx.send(MarketCommand::GetSnapshot { resp: resp_tx }).await.is_ok() {
                // Await the snapshot response from the worker
                if let Ok(snapshot_event) = resp_rx.await {
                    // Serialize and push the snapshot down the Actix WebSocket session
                    if let Ok(json) = serde_json::to_string(&snapshot_event) {
                        let _ = session.text(json).await;
                    }
                }
            }
        }
    } else {
        let _ = session.text("Market not found").await;
    }

                                    // if let Some(sender) = state.market_channel.get(&market_enum) {
                                    //     active_receiver = Some(sender.subscribe());
                                    //     let _ = session.text("Subscribed successfully").await;
                                    // } else {
                                    //     let _ = session.text("Market not found").await;
                                    // }
                                }
                            }
                        }
                        Ok(Message::Ping(bytes)) => {
                            if session.pong(&bytes).await.is_err() {
                                break;
                            }
                        }
                        Ok(Message::Close(_)) => break,
                        Err(_) => break,
                        _ => {}
                    }
                },

                ///////////////////////////////////////////
                // 2. Handle incoming events FROM the broadcast channel (Server -> Browser)
                res = async {
                    match &mut active_receiver {
                        Some(rx) => rx.recv().await,
                        // If the client hasn't subscribed to a market yet, block this branch forever
                        None => std::future::pending().await,
                    }
                } => {
                    match res {
                        Ok(event) => {
                            // Serialize the MarketEvent enum into a JSON string
                            if let Ok(json) = serde_json::to_string(&event) {
                                // Push the JSON string down the WebSocket TCP socket to the browser
                                if session.text(json).await.is_err() {
                                    break; // Client disconnected or network error
                                }
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                            // Client is too slow! Handle lag if needed
                            eprintln!("Client lagged behind by {} messages", skipped);
                        }
                        Err(_) => break,
                    }
                }
            }
        }

        // Clean close when client disconnects
        let _ = session.close(None).await;
    });

    Ok(res)
}