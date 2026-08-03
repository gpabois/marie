use libp2p::PeerId;
use serde::{Deserialize, Serialize};

use crate::secret::{KeyEpoch, SecretKey};

pub struct MarieNodeArgs {
    epochs: Vec<(KeyEpoch, SecretKey)>,
    current_epoch: KeyEpoch
}

#[derive(Hash, Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord)]
pub struct NodeId(Vec<u8>);

impl AsRef<[u8]> for NodeId {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl From<PeerId> for NodeId {
    fn from(value: PeerId) -> Self {
        NodeId(value.to_bytes())
    }
}

impl From<NodeId> for PeerId {
    fn from(value: NodeId) -> Self {
        PeerId::from_bytes(&value.0).unwrap()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct OwnNodeId(NodeId);

impl OwnNodeId {
    pub fn new(id: NodeId) -> Self {
        Self(id)
    }

    pub fn local() -> Self {
        Self::new(NodeId::default())
    }
}

impl AsRef<[u8]> for OwnNodeId {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl From<OwnNodeId> for NodeId {
    fn from(value: OwnNodeId) -> Self {
        value.0
    }
}