pub mod capabilities;
pub mod protocol;
pub(crate) mod rpc;

use std::{collections::{HashMap, VecDeque}, sync::Arc};

use chrono::{DateTime, Duration, Utc};
use futures::stream::{BoxStream, StreamExt};
use libp2p::PeerId;
use parking_lot::Mutex;

use crate::{
    annuary::{capabilities::Capabilities, protocol::AnnuaryEvent, rpc::GetCapabilities}, di::{Constructible, Get, Resolve}, events::Event, hrw, network::{Network, protocol::NetworkEvent}, node::{OwnNodeId, NodeId}, rpc::{RpcClient, Void}
};
#[cfg(feature = "rpc-executor")]
use crate::rpc::{RemoteProcedureCall, RpcServer};

pub trait RouteAnnuaryEvents {
    /// Identité du nœud local — portée par le router plutôt que passée à
    /// côté à [`Annuary::new`] : c'est [`NetworkAnnuaryEventRouter`] qui la
    /// connaît nativement (dérivée du `PeerId` libp2p, voir
    /// [`crate::network::NetworkStrategy::local_id`]), donc autant en faire
    /// la source unique plutôt que de risquer une désynchronisation avec un
    /// paramètre fourni séparément.
    fn local_node_id(&self) -> OwnNodeId;
    fn stream_events(&self) -> BoxStream<'static, AnnuaryEvent>;
}

/// Type opaque enveloppant l'implémentation concrète de [`RouteAnnuaryEvents`]
/// utilisée par [`Annuary`] — même rôle que [`crate::events::EventRouter`]
/// vis-à-vis de [`crate::events::RouteEvents`].
#[derive(Clone)]
pub struct AnnuaryEventRouter(Arc<dyn RouteAnnuaryEvents + Send + Sync + 'static>);

impl AnnuaryEventRouter {
    pub fn new(router: impl RouteAnnuaryEvents + Send + Sync + 'static) -> Self {
        Self(Arc::new(router))
    }
}

impl RouteAnnuaryEvents for AnnuaryEventRouter {
    fn local_node_id(&self) -> OwnNodeId {
        self.0.local_node_id()
    }

    fn stream_events(&self) -> BoxStream<'static, AnnuaryEvent> {
        self.0.stream_events()
    }
}

/// Implémentation de [`RouteAnnuaryEvents`] adossée au réseau libp2p — fusionne
/// deux sources de [`NetworkEvent`] (voir [`crate::network::NetworkStrategy::stream_events`]) :
/// les connexions/déconnexions de première main (`NodeConnected`/`NodeDisconnected`,
/// détectées dès l'établissement/la perte de connexion libp2p, sans dépendre
/// du gossip) et les [`AnnuaryEvent`] gossipés sur [`AnnuaryEvent::TOPIC`]
/// (`/marie/annuary`) pour la propagation multi-sauts (un pair appris par un
/// tiers, jamais connecté directement). Contrairement à
/// [`crate::events::NetworkEventRouter`]/[`crate::post::NetworkPostMessageRouter`],
/// pas de [`crate::events::LocalEventRouter`] embarqué ici pour le
/// self-delivery : `RouteAnnuaryEvents` ne permet pas d'émettre (un nœud ne
/// "se connecte" jamais à lui-même), donc rien à se relivrer.
#[derive(Clone)]
pub struct NetworkAnnuaryEventRouter {
    network: Network,
    local_node_id: OwnNodeId,
}

impl NetworkAnnuaryEventRouter {
    pub fn new(network: Network) -> Self {
        network.subscribe(AnnuaryEvent::TOPIC);
        let local_node_id = OwnNodeId::new(NodeId::from(PeerId::from(network.local_id())));
        Self { network, local_node_id }
    }
}

impl RouteAnnuaryEvents for NetworkAnnuaryEventRouter {
    fn local_node_id(&self) -> OwnNodeId {
        self.local_node_id.clone()
    }

    fn stream_events(&self) -> BoxStream<'static, AnnuaryEvent> {
        self.network
            .stream_events()
            .filter_map(|event| {
                let matched = match event {
                    NetworkEvent::NodeConnected { node_id } => Some(AnnuaryEvent::NodeConnected(node_id)),
                    NetworkEvent::NodeDisconnected { node_id } => Some(AnnuaryEvent::NodeDisconnected(node_id)),
                    NetworkEvent::EventReceived { topic, data, .. } if topic == AnnuaryEvent::TOPIC => {
                        serde_json::from_slice(&data).ok()
                    }
                    _ => None,
                };
                std::future::ready(matched)
            })
            .boxed()
    }
}

/// Implémentation de [`RouteAnnuaryEvents`] purement locale (un seul
/// processus, pas de réseau) — contrairement à [`crate::events::LocalEventRouter`]
/// ou [`crate::post::LocalPostMessageRouter`], il n'y a ici rien à relayer :
/// [`AnnuaryEvent`] ne décrit que des connexions/déconnexions *réseau* vers
/// d'autres pairs (voir [`NetworkAnnuaryEventRouter`]), un concept qui n'existe
/// pas hors réseau — un nœud local sans [`Network`] n'observera donc jamais
/// aucun pair. Sert de contrepartie triviale pour les tests/nœuds qui
/// n'exposent pas de [`Network`] ; sans réseau pour la dériver, l'identité du
/// nœud local doit être fournie explicitement à la construction.
#[derive(Clone)]
pub struct LocalAnnuaryEventRouter {
    local_node_id: OwnNodeId,
}

impl LocalAnnuaryEventRouter {
    pub fn new(local_node_id: OwnNodeId) -> Self {
        Self { local_node_id }
    }
}

impl RouteAnnuaryEvents for LocalAnnuaryEventRouter {
    fn local_node_id(&self) -> OwnNodeId {
        self.local_node_id.clone()
    }

    fn stream_events(&self) -> BoxStream<'static, AnnuaryEvent> {
        futures::stream::pending().boxed()
    }
}

#[derive(Clone)]
pub struct Annuary {
    rpc_client: RpcClient,
    capabilities: Arc<Capabilities>,
    local_node_id: OwnNodeId,
    peers: Arc<Mutex<HashMap<NodeId, PeerTracker>>>,
}


impl Annuary {
    /// Les `n` pairs portant `capabilities`, classés par hachage cohérent
    /// (Rendezvous/HRW, voir [`crate::hrw`]) par rapport à `id` — le nœud
    /// local est inclus dans le pool candidat s'il porte lui-même
    /// `capabilities`, au même titre que n'importe quel pair connu.
    pub fn pick_top_n(&self, id: impl AsRef<[u8]>, capabilities: impl Into<Capabilities>, n: usize) -> VecDeque<NodeId> {
        let capabilities: Capabilities = capabilities.into();
        let mut candidates: Vec<NodeId> = self.peers.lock()
            .iter()
            .filter(|(_, tracker)| tracker.capabilities.includes(capabilities))
            .map(|(id, _)| id.clone())
            .collect();

        if self.capabilities.includes(capabilities) {
            candidates.push(self.local_node_id.clone().into());
        }

        hrw::pick_top_n(id.as_ref(), &candidates, n).into_iter().cloned().collect()
    }

    /// Tous les pairs actuellement connus portant `capabilities` (là où
    /// [`Self::pick_top_n`] n'en renvoie qu'un sous-ensemble borné classé par
    /// hachage cohérent) — utilisé pour calculer une composition de groupe
    /// complète, ex. les votants d'un groupe Raft (voir
    /// `lease::server::build_lease_authority`), pas juste "un pair pour X".
    /// Comme [`Self::pick_top_n`], inclut le nœud local s'il porte lui-même
    /// `capabilities`.
    pub fn nodes_with(&self, capabilities: impl Into<Capabilities>) -> Vec<NodeId> {
        let capabilities = capabilities.into();
        let mut nodes: Vec<NodeId> = self.peers.lock()
            .iter()
            .filter(|(_, tracker)| tracker.capabilities.includes(capabilities))
            .map(|(peer_id, _)| peer_id.clone())
            .collect();

        if self.capabilities.includes(capabilities) {
            nodes.push(self.local_node_id.clone().into());
        }

        nodes
    }
}

#[cfg(feature = "rpc-executor")]
impl<C> Constructible<C> for Annuary
     where C: Get<Capabilities>
                + Get<AnnuaryEventRouter>
                + Resolve<RpcClient>
                + Resolve<RpcServer>

{
    fn construct(container: &C, _: ()) -> Self {
        Self::new(
            container.get(),
            container.get(),
            container.resolve(()),
            container.resolve(())
        )
    }
}

#[cfg(not(feature = "rpc-executor"))]
impl<C> Constructible<C> for Annuary
     where C: Get<Capabilities>
                + Get<AnnuaryEventRouter>
                + Resolve<RpcClient>

{
    fn construct(container: &C, _: ()) -> Self {
        Self::new(
            container.get(),
            container.get(),
            container.resolve(())
        )
    }
}


impl Annuary {
    #[cfg(feature = "rpc-executor")]
    pub fn new(
        capabilities: Capabilities,
        router: AnnuaryEventRouter,
        rpc_client: RpcClient,
        mut rpc_server: RpcServer,
    ) -> Self {
        let annuary = Annuary {
            rpc_client,
            capabilities: Arc::new(capabilities),
            local_node_id: router.local_node_id(),
            peers: Arc::new(Mutex::new(HashMap::new()))
        };

        tokio::spawn(annuary.clone().run(router.stream_events()));

        GetCapabilities::new(annuary.clone()).register(&mut rpc_server);

        annuary
    }

    #[cfg(not(feature = "rpc-executor"))]
    pub fn new(
        capabilities: Capabilities,
        router: AnnuaryEventRouter,
        rpc_client: RpcClient,
    ) -> Self {
        let annuary = Annuary {
            rpc_client,
            capabilities: Arc::new(capabilities),
            local_node_id: router.local_node_id(),
            peers: Arc::new(Mutex::new(HashMap::new()))
        };

        tokio::spawn(annuary.clone().run(router.stream_events()));

        annuary
    }

    async fn run(self, mut events: BoxStream<'static, AnnuaryEvent>) {
        while let Some(event) = events.next().await {
            self.handle_event(event);
        }
    }

    fn handle_event(&self, event: AnnuaryEvent) {
        match event {
            AnnuaryEvent::NodeConnected(node_id) => self.on_node_connected(node_id),
            AnnuaryEvent::NodeDisconnected(node_id) => self.on_node_disconnected(node_id),
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
