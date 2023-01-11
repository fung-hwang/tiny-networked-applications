use anyhow;
use chrono::Local;
use clap::{Parser, ValueEnum};
use env_logger::Env;
use kvs::{Error, KvStore, Result};
use log::{debug, error, info, warn};
use std::env::current_dir;
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

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

fn main() -> anyhow::Result<()> {
    // log init
    env_logger::Builder::from_env(Env::default().default_filter_or("trace"))
        .format(|buf, record| {
            let style = buf.default_level_style(record.level());
            writeln!(
                buf,
                "[{} {} {}] {}",
                Local::now().format("%Y-%m-%d %H:%M:%S"),
                style.value(record.level()),
                record.module_path().unwrap_or("<unnamed>"),
                &record.args()
            )
        })
        .init();

    let mut options = Options::parse();

    debug!("version = {:?}", env!("CARGO_PKG_VERSION"));
    debug!("{:?}", options);

    set_engine(&mut options)?;

    debug!("After setting engine, {:?}", options);

    // set up the networking
    let listener = TcpListener::bind(&options.addr).unwrap();

    for stream in listener.incoming() {
        let stream = stream.unwrap();

        handle_connection(stream)?;
    }

    // (loop) reveive cmd and execute

    Ok(())
}

fn handle_connection(mut stream: TcpStream) -> anyhow::Result<()> {
    let mut buf = [0; 128];
    stream.read(&mut buf).unwrap();
    let buffer_str = std::str::from_utf8(&buf[..])?.trim_matches(char::from(0));
    debug!("Request: {:?}", buffer_str);

    Ok(())
}

/// If --engine is specified, then ENGINE-NAME must be either "kvs", in which case the built-in engine is used,
/// or "sled", in which case sled is used.
/// If this is the first run (there is no data previously persisted) then the default value is "kvs";
/// if there is previously persisted data then the default is the engine already in use.
/// If data was previously persisted with a different engine than selected, print an error and exit with a non-zero exit code.
/// ==================================
/// cur\arg |  None |  kvs  |  sled  |
/// ----------------------------------
///    None |  kvs  |  kvs  |  sled  |
/// ----------------------------------
///    kvs  |  kvs  |  kvs  |  Err   |
/// ----------------------------------
///    sled |  sled |  Err  |  sled  |
/// ==================================
fn set_engine(options: &mut Options) -> anyhow::Result<()> {
    let cur_engine = current_engine()?;
    if cur_engine.is_none() {
        if options.engine.is_none() {
            options.engine = Some(Engine::Kvs)
        } else {
            // write engine to engine file
            fs::write(
                current_dir()?.join("engine"),
                format!("{:?}", options.engine),
            )?;
        }
    } else {
        if options.engine.is_none() {
            options.engine = cur_engine;
        } else if cur_engine != options.engine {
            error!(
                "cur_engine: {:?} != options.engine: {:?}",
                cur_engine, options.engine
            );
            // TODO: why kvs-server exit?
            std::process::exit(1);
        }
    }
    anyhow::Ok(())
}

/// Get current engine from engine file
///
/// If there is no engine exists, return Ok(None).
fn current_engine() -> anyhow::Result<Option<Engine>> {
    let engine_path = current_dir()?.join("engine");
    if !engine_path.exists() {
        Ok(None)
    } else {
        if let Ok(engine) = Engine::from_str(&fs::read_to_string(engine_path)?, true) {
            Ok(Some(engine))
        } else {
            error!("Unexpected engine type");
            Ok(None)
        }
    }
}
