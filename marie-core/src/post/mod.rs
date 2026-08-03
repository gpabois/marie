use std::{collections::HashMap, sync::Arc};

use futures::{StreamExt, stream::BoxStream};
use libp2p::PeerId;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;

use crate::{di::{Constructible, Get}, network::{Network, protocol::NetworkEvent}, node::NodeId};

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

impl<T: Serialize> MessageEnveloppe<T> {
    pub fn serialize(self) -> Result<MessageEnveloppe, PostError> {
        Ok(MessageEnveloppe {
            channel: self.channel,
            destination: self.destination,
            source: self.source,
            payload: serde_json::to_vec(&self.payload).map_err(|err| PostError::SerializerError(err.to_string()))?
        })
    }
}

impl MessageEnveloppe {
    pub fn deserialize<T: DeserializeOwned>(self) -> Result<MessageEnveloppe<T>, PostError> {
        Ok(MessageEnveloppe {
            channel: self.channel,
            destination: self.destination,
            source: self.source,
            payload: serde_json::from_slice(&self.payload).map_err(|err| PostError::DeserializeError(err.to_string()))?
        })
    }
}

#[derive(Debug, Error, Clone, Serialize, Deserialize)]
pub enum PostError {
    #[error("error while serializing message: {0}")]
    SerializerError(String),
    #[error("error while deserializing message: {0}")]
    DeserializeError(String),
}

pub trait RoutePostMessage {
    fn send(&self, message: MessageEnveloppe) -> Result<(), PostError>;
    fn stream_messages(&self, channel: &str) -> BoxStream<'static, MessageEnveloppe>;
}

/// Type opaque enveloppant l'implémentation concrète de [`RoutePostMessage`]
/// utilisée par [`PostOffice`] — même rôle que [`crate::events::EventRouter`]
/// vis-à-vis de [`crate::events::RouteEvents`].
#[derive(Clone)]
pub struct PostMessageRouter(Arc<dyn RoutePostMessage + Send + Sync + 'static>);

impl PostMessageRouter {
    pub fn new(router: impl RoutePostMessage + Send + Sync + 'static) -> Self {
        Self(Arc::new(router))
    }

    pub fn new_local() -> Self {
        Self(Arc::new(LocalPostMessageRouter::new()))
    }
}

impl RoutePostMessage for PostMessageRouter {
    fn send(&self, message: MessageEnveloppe) -> Result<(), PostError> {
        self.0.send(message)
    }

    fn stream_messages(&self, channel: &str) -> BoxStream<'static, MessageEnveloppe> {
        self.0.stream_messages(channel)
    }
}

/// Implémentation de [`RoutePostMessage`] adossée au réseau libp2p — relaie
/// vers `NetworkStrategy::send_message` (`NetworkCommand::Send`, envoi point
/// à point, pas gossipsub) et filtre le flux `NetworkEvent::ReceivedMessage`
/// du nœud sur `stream_messages`, comme [`crate::events::NetworkEventRouter`]
/// le fait pour `NetworkEvent::EventReceived`.
///
/// Embarque aussi un [`LocalPostMessageRouter`] : contrairement à gossipsub
/// (voir [`crate::events::NetworkEventRouter`]), le réseau ne sait pas du
/// tout s'envoyer un message à soi-même (pas de connexion établie vers son
/// propre `PeerId`) — [`Self::send`] redirige donc systématiquement vers lui
/// quand `destination` est le nœud local, plutôt que de fan-out vers les
/// deux comme pour les évènements ; [`Self::stream_messages`] fusionne
/// malgré tout les deux flux, puisqu'un message auto-adressé n'apparaîtra
/// jamais dans le flux réseau.
#[derive(Clone)]
pub struct NetworkPostMessageRouter {
    network: Network,
    local_id: NodeId,
    local: LocalPostMessageRouter,
}

impl NetworkPostMessageRouter {
    pub fn new(network: Network) -> Self {
        let local_id = NodeId::from(PeerId::from(network.local_id()));
        Self { network, local_id: local_id.clone(), local: LocalPostMessageRouter::new() }
    }

    fn is_local(&self, destination: &NodeId) -> bool {
        *destination == self.local_id
    }
}

impl RoutePostMessage for NetworkPostMessageRouter {
    fn send(&self, message: MessageEnveloppe) -> Result<(), PostError> {
        if self.is_local(&message.destination) {
            return self.local.send(message);
        }

        let destination = message.destination.clone();
        let data = serde_json::to_vec(&message).map_err(|err| PostError::SerializerError(err.to_string()))?;
        self.network.send_message(data, destination);

        Ok(())
    }

    fn stream_messages(&self, channel: &str) -> BoxStream<'static, MessageEnveloppe> {
        let owned_channel = channel.to_string();

        let network_stream = self.network
            .stream_events()
            .filter_map(move |event| {
                let matched = match event {
                    NetworkEvent::ReceivedMessage { message, .. } => {
                        serde_json::from_slice::<MessageEnveloppe>(&message)
                            .ok()
                            .filter(|env| env.channel == owned_channel)
                    },
                    _ => None,
                };
                std::future::ready(matched)
            });

        let local_stream = self.local.stream_messages(channel);

        futures::stream::select(local_stream, network_stream).boxed()
    }
}

/// Implémentation de [`RoutePostMessage`] purement locale (un seul processus,
/// pas de réseau) — un [`tokio::sync::broadcast`] par canal (voir
/// [`MessageEnveloppe::channel`]) plutôt qu'un unique flux partagé, même
/// logique que [`crate::events::LocalEventRouter`] (voir sa doc pour la
/// justification du dictionnaire de canaux et du get-or-create défensif
/// dans [`Self::stream_messages`]).
#[derive(Clone)]
pub struct LocalPostMessageRouter {
    channels: Arc<Mutex<HashMap<String, broadcast::Sender<MessageEnveloppe>>>>,
}

impl LocalPostMessageRouter {
    const CHANNEL_CAPACITY: usize = 256;

    pub fn new() -> Self {
        Self { channels: Arc::new(Mutex::new(HashMap::new())) }
    }

    fn channel(&self, name: &str) -> broadcast::Sender<MessageEnveloppe> {
        self.channels
            .lock()
            .entry(name.to_string())
            .or_insert_with(|| broadcast::channel(Self::CHANNEL_CAPACITY).0)
            .clone()
    }
}

impl RoutePostMessage for LocalPostMessageRouter {
    fn send(&self, message: MessageEnveloppe) -> Result<(), PostError> {
        if let Some(tx) = self.channels.lock().get(&message.channel) {
            let _ = tx.send(message);
        }

        Ok(())
    }

    fn stream_messages(&self, channel: &str) -> BoxStream<'static, MessageEnveloppe> {
        let rx = self.channel(channel).subscribe();

        BroadcastStream::new(rx)
            .filter_map(|result| std::future::ready(result.ok()))
            .boxed()
    }
}

#[derive(Clone)]
pub struct PostOffice {
    router: PostMessageRouter
}

impl<C> Constructible<C> for PostOffice where C: Get<PostMessageRouter> {
    fn construct(container: &C, _: ()) -> Self {
        Self::new(container.get())
    }
}

impl PostOffice {
    pub fn new(router: PostMessageRouter) -> Self {
        Self { router }
    }

    pub fn local() -> Self {
        Self::new(PostMessageRouter::new(LocalPostMessageRouter::new()))
    }

    pub fn network(network: Network) -> Self {
        Self::new(PostMessageRouter::new(NetworkPostMessageRouter::new(network)))
    }

    pub fn send<M: Message>(&self, msg: impl Into<M>, destination: NodeId) -> Result<(), PostError> {
        let env = MessageEnveloppe {
            channel: M::CHANNEL.to_string(),
            destination,
            source: NodeId::default(),
            payload: msg.into()
        }.serialize()?;

        self.router.send(env)
    }

    pub fn stream_messages<M: Message>(&self) -> BoxStream<'static, MessageEnveloppe<M>> {
        self.router
            .stream_messages(M::CHANNEL)
            .filter_map(|env| std::future::ready(env.deserialize::<M>().ok()))
            .boxed()
    }
}
