use libp2p::{StreamProtocol, Swarm, gossipsub, identify, mdns, request_response, swarm::NetworkBehaviour};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::{job, network::{peer::NodeKind}};

pub mod peer;
pub mod worker;
pub mod cp;
pub mod actor;
pub mod persistency;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Frame {
    channel: String,
    destination: Option<serde_json::Value>,
    source: Option<serde_json::Value>,
    payload: Vec<u8>
}

#[derive(NetworkBehaviour)]
pub struct MarieBehaviour {
    pub worker_gossip: gossipsub::Behaviour,
    pub node_gossip: gossipsub::Behaviour,
    pub mdns: mdns::tokio::Behaviour,
    pub identify: identify::Behaviour,
    pub rpc: cp::rpc::Behaviour,
    pub oneway: request_response::json::Behaviour<Frame, ()>
}

pub type MarieSwarm = Swarm<MarieBehaviour>;

pub async fn start_swarm<Init: Fn(&mut MarieSwarm)>(kind: NodeKind, init: Init) -> Result<Swarm<MarieBehaviour>, anyhow::Error> {
    let mut swarm = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(libp2p::tcp::Config::default(), libp2p::noise::Config::new, libp2p::yamux::Config::default)?
        .with_behaviour(|key| {
            let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), key.public().to_peer_id()).unwrap();
            let id_config = identify::Config::new("/marie/id/1.0.0".to_string(), key.public())
                .with_agent_version(format!("marie/{}/1.0.0", kind));
            let identify = identify::Behaviour::new(id_config);
            
            let worker_gossip = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(key.clone()), gossipsub::Config::default()
            ).unwrap();
            
            let node_gossip = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(key.clone()), gossipsub::Config::default()
            ).unwrap();

            let cp_rpc = request_response::json::Behaviour::new([
                (StreamProtocol::new("/marie/control-plane/1.0.0"), request_response::ProtocolSupport::Full)
                ], request_response::Config::default()
            );

            let oneway = request_response::json::Behaviour::new([
                (StreamProtocol::new("/marie/one-way/1.0.0"), request_response::ProtocolSupport::Full)
                ], request_response::Config::default()
            );

            MarieBehaviour { mdns, identify, worker_gossip, rpc: cp_rpc, node_gossip, oneway }
        })?
        .build();

    init(&mut swarm);

    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    info!("📡 Swarm [{}] initialisé. PeerID: {}", kind, swarm.local_peer_id());
    Ok(swarm)
}