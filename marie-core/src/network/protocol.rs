use libp2p::{PeerId, rendezvous::{Namespace, Ttl}};

use crate::network::mux::Frame;


pub enum NetworkCommand {
    Listen(oneshot::Sender<()>),
    SendFrame(Frame),
    Subscribe(gossipsub::IdentTopic),
    Publish {
        topic: gossipsub::IdentTopic,
        payload: Vec<u8>
    },
    /// Enregistre le pair dans un espace de nom
    /// auprès du serveur bootstrap
    RegisterPeer {
        namespaces: Vec<Namespace>,
        bootstrap_peer_id: PeerId,
        ttl: Option<Ttl>
    },
    Shutdown,
}


#[derive(Clone)]
pub enum NetworkEvent {
    ReceivedFrame(Frame),
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
    PubSubReceived {
        id: String,
        topic: String,
        data: Vec<u8>,
        source: PeerId,
    }
}