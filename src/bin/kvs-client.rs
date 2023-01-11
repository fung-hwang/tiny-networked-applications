use anyhow;
use clap::{Args, Parser, Subcommand};
use kvs::{Error, KvStore, Result};
use std::env::current_dir;

#[derive(Parser, Debug)]
#[command(name = "kvs-client", author, version, about, long_about = None)]
struct Options {
    #[command(subcommand)]
    command: Commands,
    #[arg(short, long, default_value = "127.0.0.1:4000", help = "IP:PORT")]
    addr: String,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Set the value of a string key to a string
    Set(Set),
    /// Get the string value of a given string key
    Get(Get),
    /// Remove a given key
    Rm(Remove),
}

#[derive(Args, Debug)]
struct Set {
    key: String,
    value: String,
}

#[derive(Args, Debug)]
struct Get {
    key: String,
}

#[derive(Args, Debug)]
struct Remove {
    key: String,
}

fn main() -> anyhow::Result<()> {
    let options = Options::parse();
    println!("{:?}", options);

    // let mut store = KvStore::open(current_dir()?)?;

    match options.command {
        Commands::Set(Set { key, value }) => {
            // store.set(key, value).unwrap();
        }
        Commands::Get(Get { key }) => {
            // let cmd = store.get(key)?;
            // if let Some(value) = cmd {
            //     print!("{}", value);
            // } else {
            //     print!("Key not found");
            // }
        }

        Commands::Rm(Remove { key }) => {
            // match store.remove(key) {
            //     Ok(_) => {}
            //     Err(Error::KeyNotFound) => {
            //         print!("Key not found");
            //         std::process::exit(1);
            //     }
            //     Err(err) => {
            //         return Err(err);
            //     }
            // },
        }
    }
    Ok(())
}
