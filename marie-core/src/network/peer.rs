use libp2p::PeerId;

pub struct PeerNode {
    pub peer_id: Option<PeerId>,
    pub node_kind: NodeKind
}

#[derive(strum_macros::Display, strum_macros::EnumString)]
pub enum NodeKind {
    /// Noeud dédié à traiter des jobs
    Worker,
    /// Noeud dédié à surveiller que les jobs arrivent à terme
    WorkerWatchdog,
    /// Nœud dédié à la persistance durable de structures de données du
    /// cluster (aujourd'hui : le contenu CRDT des sessions, voir
    /// `network::persistency`) — ne participe ni au cluster Raft du control
    /// plane, ni à l'exécution de jobs.
    Persistency,
    /// Nœud tiers, développé par l'utilisateur, qui se contente de rejoindre
    /// le réseau (voir `Marie::join`) sans endosser de rôle de cluster — ex.
    /// une passerelle HTTP/WebSocket pour du HITL, ou l'affichage des
    /// logs/statuts d'une session. N'est jamais authentifié comme
    /// `ControlPlane`/`Worker`/`Persistency` (voir `NetworkActor::run`).
    Client
}
