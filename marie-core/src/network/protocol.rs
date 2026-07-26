use libp2p::{PeerId, gossipsub, rendezvous::{Namespace, Ttl}};
use tokio::sync::oneshot;

use crate::node::NodeId;


pub enum NetworkCommand {
    Listen(oneshot::Sender<()>),
    Send(Vec<u8>, NodeId),
    Subscribe(gossipsub::IdentTopic),
    Unsubscribe(gossipsub::IdentTopic),
    Publish {
        topic: gossipsub::IdentTopic,
        payload: Vec<u8>
    },
    Shutdown,
}


#[derive(Clone)]
pub enum NetworkEvent {
    ReceivedMessage {
        message: Vec<u8>,
        source: NodeId
    },
    BootstrapDiscovered {
        peer_id: PeerId
    },
    NamespacePeerRegistred {
        namespace: Namespace,
        peer_id: PeerId,
        ttl: Ttl
    },
    PeerDisconnected {
        peer_id: PeerId,
    },
    /// Première connexion établie avec `peer_id` (voir
    /// `SwarmEvent::ConnectionEstablished`) — consommé notamment par
    /// [`crate::annuary::Annuary`] pour découvrir les pairs du cluster sans
    /// dépendre du mécanisme de rendez-vous par namespace
    /// (`network::bootstrap::BootstrapClient`).
    PeerConnected {
        peer_id: PeerId,
    },
    EventReceived {
        id: String,
        topic: String,
        data: Vec<u8>,
        source: PeerId,
    }
}