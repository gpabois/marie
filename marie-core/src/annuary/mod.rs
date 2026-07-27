pub mod capabilities;
pub mod protocol;
pub(crate) mod rpc;

use std::{collections::{HashMap, VecDeque}, sync::Arc};

use chrono::{DateTime, Duration, Utc};
use futures::stream::BoxStream;
use libp2p::PeerId;
use parking_lot::Mutex;
use tokio::select;
use tokio_stream::StreamExt;

use crate::{
    annuary::{capabilities::Capabilities, protocol::AnnuaryEvent, rpc::GetCapabilities}, events::EventService, hrw, network::protocol::NetworkEvent, node::NodeId, rpc::{RemoteProcedureCall, RpcClient, RpcServer, Void}
};

#[derive(Clone)]
pub struct Annuary {
    rpc_client: RpcClient,
    events: EventService,
    capabilities: Arc<Capabilities>,
    peers: Arc<Mutex<HashMap<NodeId, PeerTracker>>>,
}

impl Annuary {
    pub fn pick_top_n(&self, id: impl AsRef<[u8]>, capabilities: impl Into<Capabilities>, n: usize) -> VecDeque<NodeId> {
        let capabilities: Capabilities = capabilities.into();
        let peers: Vec<NodeId> = self.peers.lock()
            .iter()
            .filter(|(id, tracker)| tracker.capabilities.includes(capabilities))
            .map(|(id, _)| id.clone())
            .collect();

        hrw::pick_top_n(id.as_ref(), &peers, n).into_iter().cloned().collect()
    }

    /// Tous les pairs actuellement connus portant `capabilities` (là où
    /// [`Self::pick_top_n`] n'en renvoie qu'un sous-ensemble borné classé par
    /// hachage cohérent) — utilisé pour calculer une composition de groupe
    /// complète, ex. les votants d'un groupe Raft (voir
    /// `lease::server::build_lease_authority`), pas juste "un pair pour X".
    pub fn nodes_with(&self, capabilities: impl Into<Capabilities>) -> Vec<NodeId> {
        let capabilities = capabilities.into();
        self.peers.lock()
            .iter()
            .filter(|(_, tracker)| tracker.capabilities.includes(capabilities))
            .map(|(peer_id, _)| peer_id.clone())
            .collect()
    }
}

impl Annuary {
    /// `network_events` : flux brut des évènements réseau locaux (voir
    /// `NetworkStrategy::stream_events`), utilisé pour
    /// `NetworkEvent::PeerConnected`/`PeerDisconnected` — c'est la *seule*
    /// source qui fonctionne dès la première connexion directe à un pair.
    /// Le topic gossip `/marie/annuary` (`AnnuaryEvent`) reste écouté en plus
    /// pour la propagation multi-sauts (un pair appris par un tiers, jamais
    /// connecté directement), mais rien ne l'émet encore aujourd'hui.
    pub fn new(
        capabilities: Capabilities,
        events: EventService,
        rpc_client: RpcClient,
        mut rpc_server: RpcServer,
        network_events: BoxStream<'static, NetworkEvent>,
    ) -> Self {
        let annuary = Annuary {
            rpc_client,
            events,
            capabilities: Arc::new(capabilities),
            peers: Arc::new(Mutex::new(HashMap::new()))
        };

        tokio::spawn(annuary.clone().run(network_events));

        GetCapabilities::new(annuary.clone()).register(&mut rpc_server);

        annuary
    }

    async fn run(self, mut network_events: BoxStream<'static, NetworkEvent>) {
        self.events.subscribe("/marie/annuary");
        let mut rx = self.events.stream_events("/marie/annuary");

        loop {
            select! {
                Some(event) = rx.next() => self.handle_event(event),
                Some(event) = network_events.next() => self.handle_network_event(event),
            }
        }
    }

    fn handle_event(&self, event: AnnuaryEvent) {
        match event {
            AnnuaryEvent::NodeConnected(node_id) => self.on_node_connected(node_id),
            AnnuaryEvent::NodeDisconnected(node_id) => self.on_node_disconnected(node_id),
        }
    }

    fn handle_network_event(&self, event: NetworkEvent) {
        match event {
            NetworkEvent::NodeConnected { node_id } => self.on_node_connected(node_id),
            NetworkEvent::NodeDisconnected { node_id } => self.on_node_disconnected(node_id),
            _ => {}
        }
    }

    fn on_node_connected(&self, node_id: NodeId) {
        let annuary = self.clone();
        tokio::spawn(annuary.handle_peer_connection(node_id));
    }

    fn on_node_disconnected(&self, node_id: NodeId) {
        self.peers.lock().remove(&node_id);
    }

    async fn handle_peer_connection(self, node_id: NodeId) -> crate::Result<()> {
        let capabilities = self.rpc_client.invoke::<GetCapabilities>(
            Void, 
            [node_id.clone()]
        ).await?;
        
        self.peers.lock().entry(node_id).or_insert_with(|| PeerTracker {
            peer_status: PeerStatus::Alive,
            capabilities,
            expires_at: Utc::now() + Duration::hours(2)
        });

        Ok(())
    }
}

enum PeerStatus {
    Unidentified,
    Alive,
    Zombie,
    Dead
}

struct PeerTracker {
    peer_status: PeerStatus,
    capabilities: Capabilities,
    expires_at: DateTime<Utc>,
}