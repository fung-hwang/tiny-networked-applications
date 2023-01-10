use clap::{Args, Parser, Subcommand, ValueEnum};
use kvs::{Error, KvStore, Result};
use std::env::current_dir;
use std::fs::read_to_string;

#[derive(Parser, Debug)]
#[command(name = "kvs-server", author, version, about, long_about = None)]
struct Options {
    #[arg(short, long, default_value = "127.0.0.1:4000", help = "IP:PORT")]
    addr: String,
    #[arg(short, long, help = "ENGINE-NAME")]
    engine: Option<Engine>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum Engine {
    Kvs,
    Sled,
}

fn main() -> Result<()> {
    let mut options = Options::parse();
    println!("{:?}", options);

    // TODO: options.engine, using file to persistent
    // ==================================
    // cur\arg |  None |  kvs  |  sled  |
    // ----------------------------------
    //    None |  kvs  |  kvs  |  sled  |
    // ----------------------------------
    //    kvs  |  kvs  |  kvs  |  Err   |
    // ----------------------------------
    //    sled |  sled |  Err  |  sled  |
    // ==================================
    let cur_engine = current_engine()?;
    if cur_engine.is_none() && options.engine.is_none() {
        options.engine = Some(Engine::Kvs)
        // TODO: if cur_engine.is_none() && options.engine.is_some(), write engine type to file
    }
    if cur_engine.is_some() {
        if options.engine.is_none() {
            options.engine = cur_engine;
        } else if cur_engine != options.engine {
            std::process::exit(1);
        }
    }

    // (loop) reveive cmd and execute

    Ok(())
}

fn current_engine() -> Result<Option<Engine>> {
    let engine_path = current_dir()?.join("engine");
    if !engine_path.exists() {
        Ok(None)
    } else {
        let engine = read_to_string(engine_path)?;
        if engine == "kvs".to_owned() {
            Ok(Some(Engine::Kvs))
        } else if engine == "sled".to_owned() {
            Ok(Some(Engine::Sled))
        } else {
            Err(Error::UnexpectedEngine(engine))
        }
    }
}
