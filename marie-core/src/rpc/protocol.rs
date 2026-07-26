use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{id::ID, post::Message, rpc::RpcError};


#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RpcId(pub(super) ID);

#[derive(Clone, Serialize, Deserialize)]
pub enum RpcMessage {
    Call(RpcCall),
    Ack(RpcAck),
    Reply(RpcReply)
}

impl From<RpcCall> for RpcMessage {
    fn from(value: RpcCall) -> Self {
        RpcMessage::Call(value)
    }
}

impl From<RpcReply> for RpcMessage {
    fn from(value: RpcReply) -> Self {
        RpcMessage::Reply(value)
    }
}

impl Message for RpcMessage {
    const CHANNEL: &str = "/marie/rpc";
}

#[derive(Clone, Serialize, Deserialize)]
pub struct RpcCall {
    pub id: RpcId,
    pub name: String,
    pub args: serde_json::Value,
}


#[derive(Clone, Serialize, Deserialize)]
pub struct RpcReply {
    pub id: RpcId,
    pub result: Result<Value, RpcError>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct RpcAck {
    pub id: RpcId,
}