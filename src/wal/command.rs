
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;

//DATA MODELS
use serde::{Serialize, Deserialize};
use crate::engine::{MatchingEngine,MarketPair};
use crate::models::Order;


// use crate::wal::{Wal, WallEntry}; // Adjust paths to your project


// #[derive(Debug, Clone, Serialize,Deserialize)]
// pub enum EngineCommand {
//     PlaceOrder {
//         market: MarketPair, // or MarketPair
//         order: Order,   // Passes your entire Order struct!
//     },
//     CancelOrder {
//         market: MarketPair,
//         id: u64,
//     },


//     ModifyOrder {
//         market: MarketPair,
//         id: u64,
//         new_price: f64,
//         new_quantity: f64,
//     },
//     Deposit {  user_id: u32, asset: String, amount: f64 },       // 👈 ADD THIS
//     Withdraw { user_id: u32, asset: String, amount: f64 },      // 👈 ADD THIS
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
//     }
// }


//TWO WALS


/// 01). Wal for the order and executed trade

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EngineCommand {
    PlaceOrder {
        market: MarketPair,
        order: Order,
    },
    CancelOrder {
        market: MarketPair,
        id: u64,
    },
    ModifyOrder {
        market: MarketPair,
        id: u64,
        new_price: f64,
        new_quantity: f64,
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
    },
}




pub struct Wal {
    writer: io::BufWriter<File>,
    next_seq: u64,
}
/////////////////////////////////////////////////

//We want to save the commands in the wal with command and sequence nuber so we can tract sequncee so we crated WallEntry struct


#[derive(Debug, Clone, Serialize,Deserialize)]
pub struct WallEntry {
    pub seq: u64,
    pub command: EngineCommand,
}


////////////////////////////////////////////


impl Wal {
    /// Initialize or open an existing WAL file in Hard Disk, 
    /// ensuring `next_seq` continues correctly from where it left off.
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path_ref = path.as_ref();

        // 1. Recover existing entries first to find the last sequence number
        let existing_entries = Self::recover(path_ref)?;

        // 2. Determine the next sequence number:
        // If entries exist, take the last entry's sequence + 1. Otherwise, start at 1.
        let next_seq = existing_entries
            .last()
            .map(|entry| entry.seq + 1)
            .unwrap_or(1);

        // 3. Open the file in append mode (create if it doesn't exist)
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path_ref)?;

        let writer = io::BufWriter::new(file);

        println!("WAL initialized successfully. Resuming from sequence: {}", next_seq);

       let wal= Self {
            writer,
            next_seq,
        };

        // Return both the wal instance and the entries to replay
        Ok(wal)
    }
    // //CREATING FILE IN HARD DISC
    // ///STEP 1). Initialize or open an existing WAL file in Hard Disk
    // /// if file alredy exist then pth;URL :AsRef give refrence switch either it can string or &str or refrence of Path
    // pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
    //     let file = OpenOptions::new()
    //         .create(true)
    //         .append(true)
    //         .open(path)?;
            
    //     Ok(Self {
    //         writer: io::BufWriter::new(file),
    //         next_seq: 1,
    //     })
    

    ///STEP 2). INSERTING A FILE IN THE BOTTOM OF FILE:  Append a command to disk and ensure it's flushed to hardware
    pub fn append(&mut self, command: &EngineCommand) -> io::Result<()> {
       //here we serialize command and write in the bottom of the created file

        let seq = self.next_seq;

        let entry = WallEntry { seq, command:command.clone(), };

        //serialize the incoming orderCommand into serde_son string types
        let serialized = serde_json::to_string(&entry)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            //if error then wmap_err takes in error=e then handle it
       

        // 2. Write the JSON text, followed by a newline character (\n) so each order has its own line    
        self.writer.write_all(serialized.as_bytes())?;
        self.writer.write_all(b"\n")?; //write each comand in seprate lines
        
        // Flush buffer to disk to guarantee durability on crash
        self.writer.flush()?;

        // 4. Force the Operating System to physically write it to the hardware disk/SSD(before crash send all to disc/harddisc)
        self.writer.get_ref().sync_all()?; 

        self.next_seq += 1;
        println!("APPLYING TO WAL -> Seq: {}, Command: {:?}", seq, command);
        
        Ok(())
    }


    //STEP 3: READ AND RECOVER FILE 
    /// Replay the WAL from disk to restore matching engine state on startup
    pub fn recover<P: AsRef<Path>>(path: P) -> io::Result<Vec<WallEntry>> {
        if !path.as_ref().exists() {
            return Ok(Vec::new()); // No WAL exists yet (fresh start)
        }

        let file = File::open(path)?;
        let reader = BufReader::new(file);
        //creating empty vectore so we an save all command here after reading fro hard disc
        let mut commands:Vec<WallEntry> = Vec::new();

        //now rading line by line the commands 
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            // reading the line & making from line to command, and transforming in human readbale struct
            let command:WallEntry = serde_json::from_str(&line)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            //then push in the vectore
            println!("RECOVERED FROM DISK -> Seq: {}, Command: {:?}", command.seq, command.command);            commands.push(command);
        }

        

        Ok(commands)
    }

    //function that help to read recovered entries
    // NEW: Constructor that takes the entries we already read in memory
   pub fn from_recovered_entries(path: &str, all_entries: Vec<WallEntry>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;

        let writer = io::BufWriter::new(file);

        let next_seq = match all_entries.last() {
            Some(last) => last.seq + 1,
            None => 1,
        };

        println!("WAL initialized successfully. Resuming from sequence: {}", next_seq);

        Ok(Self { writer, next_seq })
    }

}


//////////////


pub async fn recover_market_engine(
    market_pair: &MarketPair,
) -> io::Result<(MatchingEngine, Wal)> {
    let _snapshot_path = format!("{:?}.snapshot", market_pair).to_lowercase();
    let wal_path = format!("{:?}.wal", market_pair).to_lowercase();

    let mut engine = MatchingEngine::new();
    let last_seq = 0;

    // 1. Load per-market snapshot if it exists
    // (Assuming you have a per-market snapshot loader, or adapt to your snapshot struct)
    /*
    if let Some(snapshot) = MarketSnapshot::load(&snapshot_path)? {
        last_seq = snapshot.seq;
        engine.active_orders = snapshot.active_orders;
        // Rebuild orderbook from snapshot active orders...
        println!("Restored snapshot for {:?} up to seq: {}", market_pair, last_seq);
    }
    */

    // 2. Recover WAL entries for this specific market
    let all_entries: Vec<WallEntry> = Wal::recover(&wal_path).unwrap_or_default();
    let entries= all_entries.clone();

    // 3. Filter gap entries newer than snapshot
    let gap_entries: Vec<WallEntry> = entries.clone()
        .into_iter()
        .filter(|entry| entry.seq > last_seq)
        .collect();

    // println!("Replaying {} gap events for {:?}...", gap_entries.len(), market_pair);

    // 4. Replay gap commands into this market's engine
    for entry in gap_entries {
        match entry.command {
            EngineCommand::PlaceOrder { market, order } => {
                if market == *market_pair {
                    engine.active_orders.insert(order.id, order.clone());
                    let _ = engine.order_book.add_order(order);
                }
            }
            EngineCommand::CancelOrder { market, id } => {
                if market == *market_pair {
                    engine.active_orders.remove(&id);
                    engine.order_book.cancel_order(id);
                }
            }
            EngineCommand::ModifyOrder { market, id, new_price, new_quantity } => {
                if market == *market_pair {
                    if let Some(existing_order) = engine.active_orders.get(&id) {
                        let _ = engine.order_book.add_order(existing_order.clone());
                    }
                    if let Ok(Some((updated_order, _, _))) = engine.order_book.modify_order(id, new_price, new_quantity) {
                        engine.active_orders.insert(id, updated_order);
                    }
                }
            }
            _ => {} // Deposit/Withdraw/Settle belong to the ledger worker, ignore here
        }
    }

    // 5. Recalculate sequence numbers for this engine
    engine.recalculate_sequence_number();
    println!("Market {:?} initialized with next sequence number: {}", market_pair, engine.next_sequence_number);

    // 6. Open WAL in append mode for future live writes(causeing double entries)
    // let wal = Wal::new(&wal_path)?;
    
    //using below so read only the existing entries
    // 5. Open/Initialize WAL in append mode using the ALREADY RECOVERED entries 
    // (This prevents Wal::new from scanning the file a second time!)
    let wal = Wal::from_recovered_entries(&wal_path, all_entries)?;
    Ok((engine, wal))
}


/////////BALANCE WAL////////////////////////////////


//BALANCE WAL DATA MODEL

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum BalanceWalCommand {
    Deposit { 
        user_id: u32, 
        asset: String, 
        amount: f64 
    },
    Withdraw { 
        user_id: u32, 
        asset: String, 
        amount: f64 
    },
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BalanceWallEntry {
    pub seq: u64,
    pub command: BalanceWalCommand,
}

pub struct BalanceWal {
    writer: io::BufWriter<File>,
    next_seq: u64,
    path: String,
}

//Struct which we use to save the command in the balance wal

//BALANCE WAL FILE CRETIOn CODE IMPL BLock//


impl BalanceWal {
    /// Initialize or open an existing balance WAL file, resuming `next_seq`.
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path_ref = path.as_ref();
        let path_str = path_ref.to_string_lossy().into_owned();

        let existing_entries = Self::recover(path_ref)?;
        let next_seq = existing_entries
            .last()
            .map(|entry| entry.seq + 1)
            .unwrap_or(1);

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path_ref)?;

        let writer = io::BufWriter::new(file);

        println!("Balance WAL initialized. Resuming from sequence: {}", next_seq);

        Ok(Self {
            writer,
            next_seq,
            path: path_str,
        })
    }

    /// Append a balance command to disk with hardware flush guarantees.
    pub fn append(&mut self, command: &BalanceWalCommand) -> io::Result<()> {
        let seq = self.next_seq;
        let entry = BalanceWallEntry {
            seq,
            command: command.clone(),
        };

        let serialized = serde_json::to_string(&entry)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        self.writer.write_all(serialized.as_bytes())?;
        self.writer.write_all(b"\n")?;
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;

        self.next_seq += 1;
        println!("APPLYING TO BALANCE WAL -> Seq: {}, Command: {:?}", seq, command);

        Ok(())
    }

    /// Replay the balance WAL from disk on startup.
    pub fn recover<P: AsRef<Path>>(path: P) -> io::Result<Vec<BalanceWallEntry>> {
        let path_ref = path.as_ref();
        if !path_ref.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(path_ref)?;
        let reader = BufReader::new(file);
        let mut entries = Vec::new();

        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let entry: BalanceWallEntry = serde_json::from_str(&line)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            
            entries.push(entry);
        }

        Ok(entries)
    }

    /// Truncate and reset the balance WAL back to sequence 1 after a snapshot.
    pub fn reset_wal(&mut self) -> io::Result<()> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;

        self.writer = io::BufWriter::new(file);
        self.next_seq = 1;

        println!("Balance WAL successfully truncated and reset to sequence 1.");
        Ok(())
    }
}

