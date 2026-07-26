pub mod client;
#[cfg(feature ="rpc-server")]
mod server;

pub mod protocol;

use async_trait::async_trait;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

#[cfg(feature ="rpc-server")]
pub use server::RpcServer;
pub use client::RpcClient;
pub use marie_macros::core_rpc;

use crate::node::NodeId;
use crate::post::PostError;


#[derive(Clone, Debug, Error, Serialize, Deserialize)]
pub enum RpcError {
    #[error("erreur lors de la désérialization: {0}")]
    DeserializeError(String),
    #[error("erreur lors de l'envoi/réception du message")]
    PostError(#[from] PostError),
    #[error("time-out de l'appel distant")]
    TimeOut,
    #[error("aucun exécuteur n'a été trouvé pour cette procédure {0}")]
    NoExecutorFound(String),
    #[error("arrêt du serveur d'appel distant")]
    Shutdown
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
    async fn execute(self, args: Self::Args, caller: NodeId) -> Self::Return;

    #[cfg(feature = "rpc-executor")]
    fn register(self, rpc: &mut RpcServer) where Self: Clone + Send + Sync + 'static {
        let func = move |args, caller| {
            self.clone().execute(args, caller)
        };

        rpc.register(Self::NAME, func);

    }
}