pub mod client;
#[cfg(feature ="rpc-server")]
mod server;
pub mod register;
pub mod layers;
pub mod event;

use async_trait::async_trait;
pub use event::{RpcEvent, RpcEventKind};
use libp2p::PeerId;

use std::hash::Hash;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;
use crate::id::ID;

#[cfg(feature ="rpc-server")]
pub use server::RpcServer;
pub use client::RpcClient;

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
    Ack(RpcAck),
    Reply(RpcReply)
}

impl RpcMessage {
    pub fn destination(&self) -> Option<PeerId> {
        use RpcMessage::{Call, Ack, Reply};

        match self {
            Call(call) => call.destination.clone(),
            Ack(ack) => ack.destination.clone(),
            Reply(reply) => reply.destination.clone()
        }
    }

    pub fn source(&self) -> Option<PeerId> {
        use RpcMessage::{Call, Ack, Reply};

        match self {
            Call(call) => call.source.clone(),
            Ack(ack) => ack.source.clone(),
            Reply(reply) => reply.source.clone()
        }
    }

    pub fn set_destination(&mut self, destination: Option<PeerId>) {
        use RpcMessage::{Call, Ack, Reply};

        match self {
            Call(call) => call.destination = destination,
            Ack(ack) => ack.destination = destination,
            Reply(reply) => reply.destination = destination
        }
    }

    pub fn set_source(&mut self, source: Option<PeerId>) {
        use RpcMessage::{Call, Ack, Reply};

        match self {
            Call(call) => call.source = source,
            Ack(ack) => ack.source = source,
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

/// Accusé de réception envoyé par le serveur dès qu'un appel est pris en
/// charge (exécuteur trouvé, tâche lancée) — avant même que le résultat ne
/// soit disponible. Permet au client de distinguer un appel toujours en
/// cours d'exécution d'un appel perdu, sans attendre la [`RpcReply`] finale
/// — voir [`crate::rpc::client::RpcClient`].
#[derive(Serialize, Deserialize)]
pub struct RpcAck {
    pub id: RpcCallId,
    pub destination: Option<PeerId>,
    pub source: Option<PeerId>
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

#[async_trait]
pub trait RemoteProcedureCall: Sized {
    const NAME: &'static str;
    type Args: Serialize + DeserializeOwned;
    type Return: Serialize + DeserializeOwned;

    #[cfg(feature = "rpc-executor")]
    async fn execute(self, args: Self::Args, caller: PeerId) -> Self::Return;

    #[cfg(feature = "rpc-executor")]
    fn register(self, rpc: &mut RpcServer) where Self: Clone + Send + Sync + 'static {
        let func = move |args, caller| {
            self.clone().execute(args, caller)
        };

        rpc.register(Self::NAME, func);

    }
}