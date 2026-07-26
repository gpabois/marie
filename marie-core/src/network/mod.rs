use std::{ops::Deref, sync::Arc};


use futures::stream::BoxStream;
use libp2p::{PeerId, gossipsub::IdentTopic};

use crate::{events::EventEnvelope, node::NodeId, layer::BoxLayer, network::{protocol::{NetworkCommand, NetworkEvent}}};

mod swarm;
pub mod persistency;
pub mod loopback;
pub mod protocol;

#[derive(Clone, Copy)]
pub struct LocalPeerId(pub(crate) PeerId);

impl From<LocalPeerId> for PeerId {
    fn from(value: LocalPeerId) -> Self {
        value.0
    }
}

impl Deref for LocalPeerId {
    type Target = PeerId;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub trait NetworkStrategy<E=crate::Error> : Send + Sync + 'static {
    fn layer(&self) -> BoxLayer<NetworkCommand, NetworkEvent, E>;
    fn execute(&self, cmd: NetworkCommand);
    fn send_message(&self, data: Vec<u8>, to: NodeId) {
        self.execute(NetworkCommand::Send(data, to))
    }
    fn stream_events(&self) -> BoxStream<'static, NetworkEvent>;

    fn emit_event(&self, event: EventEnvelope) {
        self.execute(NetworkCommand::Publish { topic: IdentTopic::new(event.topic), payload: event.payload });
    }
    fn subscribe(&self, topic: &str) {
        self.execute(NetworkCommand::Subscribe(IdentTopic::new(topic)))
    }
    fn unsubscribe(&self, topic: &str) {
        self.execute(NetworkCommand::Unsubscribe(IdentTopic::new(topic)));
    }
    fn local_id(&self) -> LocalPeerId;
}

#[derive(Clone)]
pub struct Network(Arc<dyn NetworkStrategy>);

impl Network {
    pub fn swarm() -> crate::Result<Self> {
        let net = swarm::SwarmNetwork::new()?;
        Ok(Self(Arc::new(net)))
    }
}

impl Deref for Network {
    type Target = dyn NetworkStrategy;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}



