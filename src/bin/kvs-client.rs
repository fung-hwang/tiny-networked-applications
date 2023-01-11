use anyhow;
use clap::{Args, Parser, Subcommand};
use kvs::{Error, KvStore, Result};
use std::env::current_dir;
use std::net::{TcpListener, TcpStream};

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

    if let Ok(mut stream) = TcpStream::connect(&options.addr) {
        match options.command {
            Commands::Set(Set { key, value }) => {
                unimplemented!();
            }
            Commands::Get(Get { key }) => {
                unimplemented!();
            }

            Commands::Rm(Remove { key }) => {
                unimplemented!();
            }
        }
    } else {
        println!("Couldn't connect to server...");
    }

    anyhow::Ok(())
}
