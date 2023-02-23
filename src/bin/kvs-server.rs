use anyhow;
use bytes::{Bytes, BytesMut};
use chrono::Local;
use clap::{Parser, ValueEnum};
use common::*;
use env_logger::Env;
use kvs::*;
use log::{debug, error, info, warn};
use redis_protocol::resp2::prelude::*;
use serde::{Deserialize, Serialize};
use std::env::current_dir;
use std::fs;
use std::io::{BufReader, Read, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};

mod common;

#[derive(Parser, Debug)]
#[command(name = "kvs-server", author, version, about, long_about = None)]
struct Options {
    #[arg(short, long, default_value = "127.0.0.1:7878", help = "IP:PORT")]
    addr: String,
    #[arg(short, long, help = "ENGINE-TYPE")]
    engine: Option<Engine>,
}

impl Options {
    /// Set the engine in options, according to current engine and args --engine.
    ///
    /// If --engine is specified, then ENGINE-NAME must be either "kvs", in which case the built-in engine is used,
    /// or "sled", in which case sled is used.
    ///
    /// If this is the first run (there is no data previously persisted) then the default value is "kvs".
    ///
    /// if there is previously persisted data then the default is the engine already in use.
    ///
    /// If data was previously persisted with a different engine than selected, print an error and exit with a non-zero exit code.
    ///
    // ==================================
    // cur\arg |  None |  kvs  |  sled  |
    // ----------------------------------
    //    None |  kvs  |  kvs  |  sled  |
    // ----------------------------------
    //    kvs  |  kvs  |  kvs  |  Err   |
    // ----------------------------------
    //    sled |  sled |  Err  |  sled  |
    // ==================================
    fn set_engine(&mut self) -> anyhow::Result<()> {
        let cur_engine = Self::current_engine()?;
        if cur_engine.is_none() {
            if self.engine.is_none() {
                self.engine = Some(Engine::KvStore)
            }
            // write engine type to engine file，e.g. kvs
            fs::write(
                current_dir()?.join("engine"),
                format!("{}", serde_json::to_string(&self.engine)?),
            )?;
        } else {
            if self.engine.is_none() {
                self.engine = cur_engine;
            } else if cur_engine != self.engine {
                error!(
                    "cur_engine: {:?} != options.engine: {:?}",
                    cur_engine, self.engine
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
            anyhow::Ok(None)
        } else {
            let str_from_engine_file = fs::read_to_string(engine_path)?;
            // if let Ok(engine) = Engine::from_str(&str_from_engine_file, true) {
            if let Some(engine) = serde_json::from_str(&str_from_engine_file)? {
                debug!("current engine type: {:?}", engine);
                anyhow::Ok(Some(engine))
            } else {
                error!("Unexpected engine type: {:?}", str_from_engine_file);
                anyhow::Ok(None)
            }
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Serialize, Deserialize)]
enum Engine {
    KvStore,
    Redb,
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
    options.set_engine()?;
    debug!("After setting engine, {:?}", options);

    run(options)?;

    anyhow::Ok(())
}

fn run(options: Options) -> anyhow::Result<()> {
    if let Some(engine) = options.engine {
        match engine {
            Engine::KvStore => {
                let path = current_dir()?.join("kvstore");
                let mut server = KvsServer::new(KvStore::open(path)?);
                debug!("kvsServer - kvStore");
                server.start_server(&options.addr)?;
            }
            Engine::Redb => {
                let path = current_dir()?.join("redb");
                let mut server = KvsServer::new(Redb::open(path)?);
                debug!("kvsServer - redb");
                server.start_server(&options.addr)?;
            }
            Engine::Sled => todo!(),
        }
    }

    anyhow::Ok(())
}

// Trait Object or Generic Type
// A generic type parameter can work with one concrete type at a time,
// whereas trait objects allow for multiple concrete types to fill in for the trait object at run-time.
// We don't need multiple concrete types
pub struct KvsServer<E: KvsEngine> {
    engine: E,
}

impl<E: KvsEngine> KvsServer<E> {
    pub fn new(engine: E) -> Self {
        KvsServer { engine }
    }

    pub fn start_server<A: ToSocketAddrs>(&mut self, addr: &A) -> anyhow::Result<()> {
        debug!("start server");
        // set up the networking, the server is synchronous and single-threaded.
        let listener = TcpListener::bind(addr).unwrap();

        for stream in listener.incoming() {
            let stream = stream.unwrap();

            self.handle_connection(stream)?;
        }

        debug!("end server");
        anyhow::Ok(())
    }

    // TODO: handle single request per connect -> many requests per connect
    fn handle_connection(&mut self, stream: TcpStream) -> anyhow::Result<()> {
        let mut reader = BufReader::new(stream);
        let mut buf = [0; 1024];
        reader.read(&mut buf)?;
        debug!(
            "{:?}",
            std::str::from_utf8(&buf[..])?.trim_matches(char::from(0))
        );

        // parse the contents of buf
        let (frame, frame_size) = match decode(&Bytes::copy_from_slice(&buf)) {
            Ok(Some((f, c))) => (f, c),
            Ok(None) => panic!("Incomplete frame."),
            Err(e) => panic!("Error parsing bytes: {:?}", e),
        };
        debug!("Parsed frame {:?} and consumed {} bytes", frame, frame_size);

        let request = Request::try_from(frame)?;
        info!("Request: {:?}", request);

        // cmd excutor
        match request {
            Request::Set(Set { key, value }) => {
                debug!("set key:{:?} value:{:?}", key, value);
                self.engine.set(key, value)?;
            }
            Request::Get(Get { key }) => {
                debug!("get key:{:?}", key);
                let a = self.engine.get(key)?;
                info!("Get {:?}", a);
            }
            Request::Rm(Remove { key }) => {
                debug!("remove key:{:?}", key);
                self.engine.remove(key)?;
            }
        }

        // TODO: transfer cmd execute result

        anyhow::Ok(())
    }
}
