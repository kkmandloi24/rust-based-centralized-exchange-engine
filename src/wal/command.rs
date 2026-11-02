use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;
use tokio::sync::{mpsc,oneshot};

//DATA MODELS
use serde::{Serialize, Deserialize};
use crate::models::Order;
use crate::engine::MarketPair;


#[derive(Debug, Clone, Serialize,Deserialize)]
pub enum EngineCommand {
    PlaceOrder {
        market: MarketPair, // or MarketPair
        order: Order,   // Passes your entire Order struct!
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
    Deposit {  user_id: u32, asset: String, amount: f64 },       // 👈 ADD THIS
    Withdraw { user_id: u32, asset: String, amount: f64 },      // 👈 ADD THIS
    SettleTrade { 
        trade_id:u64,
        market: MarketPair,
        buyer_order_id:u64,
        seller_order_id:u64,
        buyer_id: u32,
        seller_id: u32,
        base_asset: String,   // e.g., "BTC"
        quote_asset: String,  // e.g., "USD"
        trade_quantity: f64,
        execution_price: f64,
        buyer_limit_price: f64, // Needed to unlock price difference if buyer limit > execution price
    }
}




pub struct Wal {
    writer: io::BufWriter<File>,
    next_seq: u64,
}
/////////////////////////////////////////////////

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
        let mut serialized = serde_json::to_string(&entry)
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
}


//////////////

