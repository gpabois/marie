use std::{ops::Deref, sync::Arc};


use libp2p::PeerId;

use crate::{layer::BoxLayer, network::{peer::NodeKind, protocol::{NetworkCommand, NetworkEvent}}};

pub mod peer;
#[cfg(feature = "catalog")]
pub mod catalog;
mod swarm;
pub mod persistency;
pub mod rpc;
pub mod mux;
pub mod bootstrap;
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

pub trait NetworkStrategy<E=anyhow::Error> {
    fn layer(&self) -> BoxLayer<NetworkCommand, NetworkEvent, E>;
    fn local_id(&self) -> LocalPeerId;
}

#[derive(Clone)]
pub struct Network(Arc<dyn NetworkStrategy>);

impl Network {
    pub fn swarm(kind: NodeKind) -> anyhow::Result<Self> {
        let net = swarm::SwarmNetwork::new(kind)?;
        Ok(Self(Arc::new(net)))
    }
}

impl Deref for Network {
    type Target = dyn NetworkStrategy;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}



