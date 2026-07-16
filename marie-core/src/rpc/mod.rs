pub mod client;
pub mod server;
pub mod register;
pub mod layers;
pub mod event;

pub use event::{RpcEvent, RpcEventKind};
use libp2p::PeerId;

use std::hash::Hash;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use crate::{id::ID, layer::{IntoService, Layer}, rpc::register::RpcRegistry};

pub use server::{RpcServerActor, RpcServer};
pub use client::{RpcClientActor, RpcClient};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RpcCallId(ID);

#[derive(Clone, Serialize, Deserialize)]
pub struct RpcCall {
    pub id: RpcCallId,
    pub name: String,
    pub args: serde_json::Value,
    pub destination: Option<PeerId>,
    pub source: Option<PeerId>
}

#[derive(Serialize, Deserialize)]
pub enum RpcMessage {
    Call(RpcCall),
    Reply(RpcReply)
}

impl RpcMessage {
    pub fn destination(&self) -> Option<PeerId> {
        use RpcMessage::{Call, Reply};

        match self {
            Call(call) => call.destination.clone(),
            Reply(reply) => reply.destination.clone()
        }
    }

    pub fn source(&self) -> Option<PeerId> {
        use RpcMessage::{Call, Reply};

        match self {
            Call(call) => call.source.clone(),
            Reply(reply) => reply.source.clone()
        }
    }

    pub fn set_destination(&mut self, destination: Option<PeerId>) {
        use RpcMessage::{Call, Reply};
        
        match self {
            Call(call) => call.destination = destination,
            Reply(reply) => reply.destination = destination
        }   
    }

    pub fn set_source(&mut self, source: Option<PeerId>) {
        use RpcMessage::{Call, Reply};

        match self {
            Call(call) => call.source = source,
            Reply(reply) => reply.source = source
        }   
    }
}

#[derive(Debug, Error, Serialize, Deserialize)]
pub enum RpcError {
    #[error("{0}")]
    Custom(String),
    #[error("erreur lors des opérations serde: {0}")]
    SerializerError(String),
    #[error("time-out de l'appel distant")]
    TimeOut,
    #[error("aucun exécuteur n'a été trouvé pour cette procédure")]
    NoExecutorFound,
    #[error("arrêt du serveur d'appel distant")]
    Shutdown
}

#[derive(Serialize, Deserialize)]
pub struct RpcReply {
    id: RpcCallId,
    result: RpcResult,
    destination: Option<PeerId>,
    source: Option<PeerId>
}

#[derive(Serialize, Deserialize)]
pub enum RpcResult {
    Ok(serde_json::Value),
    Error(RpcError)
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Void;

impl Serialize for Void {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_unit()
    }
}

impl<'de> Deserialize<'de> for Void {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        serde::de::IgnoredAny::deserialize(deserializer)?;
        Ok(Void)
    }
}


impl<T> IntoService<RpcClient, ()> for T where T: Layer<Send=RpcMessage, Received=RpcMessage>{
    fn into_service(self, _: ()) -> RpcClient {
        let actor = RpcClientActor::default();
        actor.run(self)
    }
}

impl<T> IntoService<RpcServer, ()> for T where T: Layer<Send=RpcMessage, Received=RpcMessage> {
    fn into_service(self, _: ()) -> RpcServer {
        let actor = RpcServerActor::default();
        actor.run(self)
    }
}