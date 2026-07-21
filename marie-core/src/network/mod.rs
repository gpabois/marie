use libp2p::{StreamProtocol, Swarm, gossipsub, identify, mdns, rendezvous, request_response, swarm::{NetworkBehaviour, SwarmEvent::Behaviour}};
use tracing::info;

use crate::network::{peer::NodeKind};

pub mod peer;
#[cfg(feature = "catalog")]
pub mod catalog;
pub mod worker;
pub mod actor;
pub mod persistency;
pub mod rpc;
pub mod mux;
pub mod bootstrap;

#[derive(NetworkBehaviour)]
pub struct MarieBehaviour {
    pub mdns: mdns::tokio::Behaviour,
    pub identify: identify::Behaviour,
    pub pub_sub: gossipsub::Behaviour,
    pub rendezvous: rendezvous::client::Behaviour,
    pub oneway: request_response::json::Behaviour<mux::Frame, ()>
}

pub type MarieSwarm = Swarm<MarieBehaviour>;

pub fn create_swarm(kind: NodeKind) -> Result<Swarm<MarieBehaviour>, anyhow::Error> {
    let swarm = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(libp2p::tcp::Config::default(), libp2p::noise::Config::new, libp2p::yamux::Config::default)?
        .with_behaviour(|key| {
            let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), key.public().to_peer_id()).unwrap();
            let id_config = identify::Config::new("/marie/id/1.0.0".to_string(), key.public())
                .with_agent_version(format!("marie/{}/1.0.0", kind));
            
            let identify = identify::Behaviour::new(id_config);
            
            let pub_sub = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(key.clone()), gossipsub::Config::default()
            ).unwrap();

            let oneway = request_response::json::Behaviour::new([
                (StreamProtocol::new("/marie/rpc/1.0.0"), request_response::ProtocolSupport::Full)
                ], request_response::Config::default()
            );

            let rendezvous = rendezvous::client::Behaviour::new(key.clone());

            MarieBehaviour { mdns, identify, pub_sub, oneway, rendezvous }
        })?
        .build();

    Ok(swarm)
}