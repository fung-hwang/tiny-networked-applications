use anyhow;
use bytes::BytesMut;
use clap::Parser;
use common::*;
use redis_protocol::resp2::prelude::*;
use std::io::Write;
use std::net::TcpStream;

mod common;

#[derive(Parser, Debug)]
#[command(name = "kvs-client", author, version, about, long_about = None)]
struct Options {
    #[command(subcommand)]
    command: Command,
    #[arg(short, long, default_value = "127.0.0.1:7878", help = "IP:PORT")]
    addr: String,
}

fn main() -> anyhow::Result<()> {
    // TODO: transfer single request per connect(parse args) -> many requests per connect(parse input to command)
    // Imitate deet
    let options = Options::parse();
    println!("{:?}", options);

    if let Ok(mut stream) = TcpStream::connect(&options.addr) {
        let frame = Frame::from(options.command);

        let mut buf = BytesMut::new();
        let len = match encode_bytes(&mut buf, &frame) {
            Ok(l) => l,
            Err(e) => panic!("Error encoding frame: {:?}", e),
        };
        println!("Encoded {} bytes into buffer with contents {:?}", len, buf);

        stream.write(&buf).unwrap();
    } else {
        println!("Couldn't connect to server...");
    }

    anyhow::Ok(())
}
