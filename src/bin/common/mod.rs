use bytes::Bytes;
use clap::{Args, Subcommand};
use redis_protocol::resp2::prelude::*;
use std::str::from_utf8;
use thiserror::Error;

#[derive(Debug)]
pub enum Response {
    Ok,
    Value(String),
    Err(String),
}

impl From<Response> for Frame {
    fn from(response: Response) -> Self {
        match response {
            Response::Ok => Frame::SimpleString("OK".into()),
            Response::Value(value) => Frame::SimpleString(value.into()),
            // TODO:
            Response::Err(err) => Frame::Error(err.into()),
        }
    }
}

#[derive(Subcommand, Debug)]
pub enum Request {
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

impl From<Request> for Frame {
    fn from(request: Request) -> Self {
        let mut frame_vec = vec![];
        match request {
            Request::Set(Set { key, value }) => {
                frame_vec.push(Frame::BulkString("set".into()));
                frame_vec.push(Frame::BulkString(key.into()));
                frame_vec.push(Frame::BulkString(value.into()));
            }
            Request::Get(Get { key }) => {
                frame_vec.push(Frame::BulkString("get".into()));
                frame_vec.push(Frame::BulkString(key.into()));
            }
            Request::Rm(Remove { key }) => {
                frame_vec.push(Frame::BulkString("remove".into()));
                frame_vec.push(Frame::BulkString(key.into()));
            }
        }
        Frame::Array(frame_vec)
    }
}

impl TryFrom<Frame> for Request {
    type Error = RequestError;

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
                    Ok(Request::Set(Set {
                        key: from_utf8(&v[1])?.to_string(),
                        value: from_utf8(&v[2])?.to_string(),
                    }))
                } else if a == &Bytes::from(&b"get"[..]) && v.len() == 2 {
                    Ok(Request::Get(Get {
                        key: from_utf8(&v[1])?.to_string(),
                    }))
                } else if a == &Bytes::from(&b"remove"[..]) && v.len() == 2 {
                    Ok(Request::Rm(Remove {
                        key: from_utf8(&v[1])?.to_string(),
                    }))
                } else {
                    Err(RequestError::ParseFrameErr)
                }
            } else {
                Err(RequestError::ParseFrameErr)
            }
        } else {
            Err(RequestError::ParseFrameErr)
        }
    }
}

#[derive(Error, Debug)]
pub enum RequestError {
    #[error("Cannot parse Frame into Request")]
    ParseFrameErr,
    #[error("Utf8Error")]
    Utf8Error(#[from] std::str::Utf8Error),
}
