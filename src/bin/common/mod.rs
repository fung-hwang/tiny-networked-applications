use bytes::Bytes;
use clap::{Args, Subcommand};
use redis_protocol::resp2::prelude::*;
use std::str::from_utf8;
use thiserror::Error;

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Set the value of a string key to a string
    Set(Set),
    /// Get the string value of a given string key
    Get(Get),
    /// Remove a given key
    Rm(Remove),
}

#[derive(Args, Debug)]
pub struct Set {
    pub key: String,
    pub value: String,
}

#[derive(Args, Debug)]
pub struct Get {
    pub key: String,
}

#[derive(Args, Debug)]
pub struct Remove {
    pub key: String,
}

impl From<Command> for Frame {
    fn from(cmd: Command) -> Self {
        let mut frame_vec = vec![];
        match cmd {
            Command::Set(Set { key, value }) => {
                frame_vec.push(Frame::BulkString("set".into()));
                frame_vec.push(Frame::BulkString(key.into()));
                frame_vec.push(Frame::BulkString(value.into()));
            }
            Command::Get(Get { key }) => {
                frame_vec.push(Frame::BulkString("get".into()));
                frame_vec.push(Frame::BulkString(key.into()));
            }
            Command::Rm(Remove { key }) => {
                frame_vec.push(Frame::BulkString("remove".into()));
                frame_vec.push(Frame::BulkString(key.into()));
            }
        }
        Frame::Array(frame_vec)
    }
}

impl TryFrom<Frame> for Command {
    type Error = CommandError;

    // 一坨答辩
    // need refactor
    fn try_from(frame: Frame) -> std::result::Result<Self, Self::Error> {
        if let Frame::Array(bulk_string_vec) = frame {
            let mut v: Vec<Bytes> = vec![];
            for bulk_string in bulk_string_vec {
                if let Frame::BulkString(s) = bulk_string {
                    v.push(s);
                }
            }
            // println!("{:?}", &v);

            if let Some(a) = v.get(0) {
                if a == &Bytes::from(&b"set"[..]) && v.len() == 3 {
                    Ok(Command::Set(Set {
                        key: from_utf8(&v[1])?.to_string(),
                        value: from_utf8(&v[2])?.to_string(),
                    }))
                } else if a == &Bytes::from(&b"get"[..]) && v.len() == 2 {
                    Ok(Command::Get(Get {
                        key: from_utf8(&v[1])?.to_string(),
                    }))
                } else if a == &Bytes::from(&b"remove"[..]) && v.len() == 2 {
                    Ok(Command::Rm(Remove {
                        key: from_utf8(&v[1])?.to_string(),
                    }))
                } else {
                    Err(CommandError::ParseFrame)
                }
            } else {
                Err(CommandError::ParseFrame)
            }
        } else {
            Err(CommandError::ParseFrame)
        }
    }
}

#[derive(Error, Debug)]
pub enum CommandError {
    #[error("Cannot parse Frame into Command")]
    ParseFrame,
    #[error("Utf8Error")]
    Utf8Error(#[from] std::str::Utf8Error),
}
