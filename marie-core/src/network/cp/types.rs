use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

use crate::{
    expert::{catalog::ExpertId, declaration::ExpertDeclaration},
    job::{Job, JobId, JobState},
    mode::state_graph::{catalog::StateGraphId, declaration::StateGraphDeclaration},
    model::declaration::{Model, ModelId},
    network::worker::info::WorkerInfo,
    session::SessionId,
    tools::{catalog::ToolId, declaration::ToolDeclaration},
    workspace::WorkspaceId,
};

/// Métadonnées réseau attachées à un membre du cluster Raft — c'est ce
/// qu'openraft appelle "Node" (à ne pas confondre avec un worker du système).
#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftNode {
    pub peer_id: Option<PeerId>,
    /// Multiaddr libp2p, ex: "/ip4/10.0.0.2/tcp/4001"
    pub addr: String,
}

pub type RaftNodeId = u64;

// ---------------------------------------------------------------------------
// Commandes répliquées via le log Raft
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ControlPlaneRequest {
    SubmitJob(Job),
    AssignJob { job_id: JobId, worker: PeerId },
    CommitState { job_id: JobId, new_state: JobState },
    RegisterWorker { worker: WorkerInfo },
    /// Retire un worker du registre (voir `network::cp::reconcile`) — un
    /// `PeerId` libp2p ne revit jamais (identité régénérée à chaque
    /// démarrage, voir `network::start_swarm`), donc un worker détecté mort
    /// ne reviendra jamais sous ce même `PeerId` : contrairement à
    /// `RemoveModel`/`RemoveTool`/`RemoveExpert` (retraits explicites,
    /// voulus par un appelant), celui-ci est purement un nettoyage — sans
    /// lui, `ControlPlaneState::workers` grossirait indéfiniment au fil des
    /// redémarrages du cluster, chacun laissant derrière lui les entrées de
    /// l'incarnation précédente.
    UnregisterWorker { peer_id: PeerId },
    /// Un pair `Persistency` (voir `network::persistency`) s'est fait
    /// connaître — ajouté aux détenteurs de secours pour toute session (voir
    /// `ControlPlaneState::session_holders` et `network::cp::reconcile`).
    RegisterPersistency { peer_id: PeerId },
    /// Crée ou remplace la déclaration d'un modèle du catalogue (voir
    /// `RpcCall::SET_MODEL`). Persisté localement au repos par chaque nœud
    /// control plane qui applique cette entrée (voir
    /// `ControlPlaneStateMachineStore::persist_model_mutation`), pas
    /// seulement par le leader qui l'a proposée.
    SetModel { id: ModelId, declaration: Model },
    /// Retire un modèle du catalogue (voir `RpcCall::REMOVE_MODEL`).
    RemoveModel { id: ModelId },
    /// Crée ou remplace la déclaration d'un tool du catalogue (voir
    /// `RpcCall::SET_TOOL`). Persisté localement au repos par chaque nœud
    /// control plane qui applique cette entrée (voir
    /// `ControlPlaneStateMachineStore::persist_tool_mutation`), pas
    /// seulement par le leader qui l'a proposée. Ne dit rien de qui exécute
    /// ce tool (voir `RpcCall::REGISTER_RPC` et
    /// `tools::client::ToolClient::register_executor`, non répliqués).
    SetTool { id: ToolId, declaration: ToolDeclaration },
    /// Retire un tool du catalogue (voir `RpcCall::REMOVE_TOOL`).
    RemoveTool { id: ToolId },
    /// Crée ou remplace la déclaration d'un expert du catalogue (voir
    /// `RpcCall::SET_EXPERT`). Persisté localement au repos par chaque nœud
    /// control plane qui applique cette entrée (voir
    /// `ControlPlaneStateMachineStore::persist_expert_mutation`), pas
    /// seulement par le leader qui l'a proposée. Ne référence les modèles et
    /// tools que par identifiant (voir [`ExpertDeclaration`]) — leur
    /// existence n'est pas vérifiée à la déclaration.
    SetExpert { id: ExpertId, declaration: ExpertDeclaration },
    /// Retire un expert du catalogue (voir `RpcCall::REMOVE_EXPERT`).
    RemoveExpert { id: ExpertId },
    /// Crée ou remplace la déclaration d'un graphe d'états du catalogue (voir
    /// `RpcCall::SET_STATE_GRAPH`). Persisté localement au repos par chaque
    /// nœud control plane qui applique cette entrée (voir
    /// `ControlPlaneStateMachineStore::persist_state_graph_mutation`), pas
    /// seulement par le leader qui l'a proposée. Cohérence des `nodes`/`edges`
    /// (voir [`StateGraphDeclaration`]) déjà validée côté appelant (voir
    /// `mode::state_graph::client::StateGraphClient::set`) avant même cette
    /// proposition — pas revérifiée ici.
    SetStateGraph { id: StateGraphId, declaration: StateGraphDeclaration },
    /// Retire un graphe d'états du catalogue (voir
    /// `RpcCall::REMOVE_STATE_GRAPH`).
    RemoveStateGraph { id: StateGraphId },
    /// Déclare (ou efface, si `workspace_id` est `None`) le workspace
    /// auquel appartient `session_id` (voir `RpcCall::SET_SESSION_WORKSPACE`
    /// et `ControlPlaneState::session_workspaces`) — sert à dériver les
    /// détenteurs d'un workspace (voir `network::cp::workspace_holders_for`)
    /// à partir de ceux, déjà connus, de ses sessions membres.
    SetSessionWorkspace { session_id: SessionId, workspace_id: Option<WorkspaceId> },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ControlPlaneResponse {
    Ok,
    Rejected { reason: String },
}

// Déclaration du TypeConfig openraft
// ---------------------------------------------------------------------------
//
// Ce macro génère un type `TypeConfig` (unit struct) qui implémente
// `openraft::RaftTypeConfig` avec les associated types suivants. C'est ce
// type qui est passé en paramètre générique partout ailleurs (Raft<TypeConfig>,
// RaftLogStorage<TypeConfig>, etc.)

openraft::declare_raft_types!(
    pub TypeConfig:
        D = ControlPlaneRequest,
        R = ControlPlaneResponse,
        NodeId = RaftNodeId,
        Node = RaftNode,
);
