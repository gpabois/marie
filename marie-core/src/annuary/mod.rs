pub mod capabilities;
pub mod protocol;
pub(crate) mod rpc;

use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, Duration, Utc};
use futures::stream::BoxStream;
use libp2p::PeerId;
use parking_lot::Mutex;
use tokio::select;
use tokio_stream::StreamExt;

use crate::{
    annuary::{capabilities::Capabilities, protocol::AnnuaryEvent, rpc::GetCapabilities},
    events::EventService,
    hrw,
    network::protocol::NetworkEvent,
    rpc::{RemoteProcedureCall, RpcClient, RpcServer, Void}
};

#[derive(Clone)]
pub struct Annuary {
    rpc_client: RpcClient,
    events: EventService,
    capabilities: Arc<Capabilities>,
    peers: Arc<Mutex<HashMap<PeerId, PeerTracker>>>,
}

impl Annuary {
    pub fn pick_top_n(&self, id: impl AsRef<[u8]>, capabilities: Capabilities, n: usize) -> Vec<PeerId> {
        let peers: Vec<PeerId> = self.peers.lock()
            .iter()
            .filter(|(id, tracker)| tracker.capabilities.includes(capabilities))
            .map(|(id, _)| *id)
            .collect();

        hrw::pick_top_n(id.as_ref(), &peers, n).into_iter().cloned().collect()
    }

    /// Tous les pairs actuellement connus portant `capabilities` (là où
    /// [`Self::pick_top_n`] n'en renvoie qu'un sous-ensemble borné classé par
    /// hachage cohérent) — utilisé pour calculer une composition de groupe
    /// complète, ex. les votants d'un groupe Raft (voir
    /// `lease::server::build_lease_authority`), pas juste "un pair pour X".
    pub fn peers_with(&self, capabilities: Capabilities) -> Vec<PeerId> {
        self.peers.lock()
            .iter()
            .filter(|(_, tracker)| tracker.capabilities.includes(capabilities))
            .map(|(peer_id, _)| *peer_id)
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
            AnnuaryEvent::NodeConnected(peer_id) => self.on_peer_connected(peer_id),
            AnnuaryEvent::NodeDisconnected(peer_id) => self.on_peer_disconnected(peer_id),
        }
    }

    fn handle_network_event(&self, event: NetworkEvent) {
        match event {
            NetworkEvent::PeerConnected { peer_id } => self.on_peer_connected(peer_id),
            NetworkEvent::PeerDisconnected { peer_id } => self.on_peer_disconnected(peer_id),
            _ => {}
        }
    }

    fn on_peer_connected(&self, peer_id: PeerId) {
        let annuary = self.clone();
        tokio::spawn(annuary.handle_peer_connection(peer_id));
    }

    fn on_peer_disconnected(&self, peer_id: PeerId) {
        self.peers.lock().remove(&peer_id);
    }

    async fn handle_peer_connection(self, peer_id: PeerId) -> crate::Result<()> {
        let capabilities = self.rpc_client.invoke::<GetCapabilities>(
            Void, 
            [peer_id]
        ).await?;
        
        self.peers.lock().entry(peer_id).or_insert_with(|| PeerTracker {
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