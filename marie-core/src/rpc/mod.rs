pub mod client;
pub mod router;

use std::hash::Hash;

use futures_util::{Sink, Stream};
use crate::id::ID;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct RpcCallId(ID);

pub struct RpcCall {
    pub id: RpcCallId,
    pub name: String,
    pub args: serde_json::Value,
    pub target: Option<serde_json::Value>
}

pub enum RpcError {
    Custom(String),
    SerializerError(serde_json::Error),
    TimeOut,
    Shutdown
}


pub struct RpcReply {
    id: RpcCallId,
    result: RpcResult
}

pub enum RpcResult {
    Ok(serde_json::Value),
    Error(RpcError)
}

pub type BoxedOutcoming<E> = Box<dyn Sink<RpcCall, Error=E> + Send + 'static>;
pub type BoxedIncoming = Box<dyn Stream<Item=RpcReply> + Send + 'static>;

pub type Transport<I, O> = (I, O);