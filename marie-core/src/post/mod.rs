use futures::{StreamExt, stream::BoxStream};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

use crate::{network::{Network, protocol::NetworkEvent}, node::{LocalNodeId, NodeId}};

pub trait Message: Serialize + DeserializeOwned + Send + Clone + 'static {
    const CHANNEL: &str;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageEnveloppe<T=Vec<u8>> {
    pub channel: String,
    pub destination: NodeId,
    pub source: NodeId,
    pub payload: T
}

#[derive(Debug, Error, Clone, Serialize, Deserialize)]
pub enum PostError {
    #[error("error while serializing message: {0}")]
    SerializerError(String),
    #[error("error while deserializing message: {0}")]
    DeserializeError(String),
}

#[derive(Clone)]
pub struct PostOffice {
    local: LocalNodeId,
    network: Network
}

impl PostOffice {
    pub fn send<M: Message>(&self, msg: impl Into<M>, destination: NodeId) -> Result<(), PostError> {
        let env = MessageEnveloppe {
            channel: M::CHANNEL.to_string(),
            destination: destination.clone(),
            source: NodeId::from(self.local.clone()),
            payload: msg.into()
        };

        let data = serde_json::to_vec(&env).map_err(|err| PostError::SerializerError(err.to_string()))?;

        self.network.send_message(data, destination.clone());
        Ok(())
    } 

    pub fn stream_messages<M: Message>(&self) -> BoxStream<'static, MessageEnveloppe<M>> {
        self.network
            .stream_events()
            .filter_map(|ev| {
            if let NetworkEvent::ReceivedMessage {message, source} = ev 
                && let Ok(env) = serde_json::from_slice::<MessageEnveloppe<M>>(&message)
                && env.channel == M::CHANNEL
            {
                return std::future::ready(Some(env))
            }

            std::future::ready(None)
        }).boxed()
    }
}
