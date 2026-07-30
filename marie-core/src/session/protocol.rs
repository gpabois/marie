use serde::{Deserialize, Serialize};

use crate::{
    expert::ExpertId, graph::{GraphId, NodeId}, hitl::{Hitl, protocol::HitlRequest}, session::{channel::ChannelUpdate, frames::FrameId}, tools::RequestToolCall 
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FrameResponse {
    pub frame_id: FrameId,
    pub updates: Vec<ChannelUpdate>,
    pub result: FrameResult
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum FrameResult {
    // --- Commandes HITL ------ //
    RequestHitl(Hitl),
    /// Demande l'avis d'experts
    AskExperts(Vec<ExpertId>),
    /// Demande l'exécution d'outils
    RequestToolsCalls(Vec<RequestToolCall>),
    // --- Commandes de graphe ------ //
    ExecuteGraph(GraphId),
    /// Va au prochain noeud
    /// Cela ajoute un frame 
    Continue,
    /// Va à un noeud nommément désigné
    GoTo(NodeId),
    /// Fourche sur plusieurs noeuds en simultané
    Fork {
        /// les branches à crééer
        branches: Vec<NodeId>,
        /// le noeud rendez-vous des branches
        join: NodeId
    },
    // --- Commandes générales ------ //
    /// Le noeud a terminé, rien besoin de faire de plus
    Completed,
    /// Le noeud a échoué
    Failed(String)
}

pub enum SessionEvent {
    HitlRequested(HitlRequest)
}