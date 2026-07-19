use std::collections::HashMap;

use bytemuck::{Pod, Zeroable};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    agent::{AgentId, status::AgentStatus},
    id::ID,
    session::{SessionId, state::executable::OrchestrationStrategy, state::frame::GraphFrameId},
};

/// Identifiant d'une [`OrchestrationFrame`], scopé à sa session — même forme
/// qu'[`AgentId`]/[`GraphFrameId`].
#[derive(Debug, Hash, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct OrchestrationFrameId(SessionId, ID);

impl OrchestrationFrameId {
    pub fn new(session_id: SessionId, id: ID) -> Self {
        Self(session_id, id)
    }

    pub fn session_id(&self) -> SessionId {
        self.0
    }

    pub fn local_id(&self) -> ID {
        self.1
    }
}

impl AsRef<[u8]> for OrchestrationFrameId {
    fn as_ref(&self) -> &[u8] {
        bytemuck::bytes_of(self)
    }
}

impl std::fmt::Display for OrchestrationFrameId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.0, self.1)
    }
}

impl std::str::FromStr for OrchestrationFrameId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (session_part, local_part) = s.split_once('/').ok_or_else(|| anyhow::anyhow!("format de OrchestrationFrameId invalide : {s}"))?;
        Ok(Self(session_part.parse()?, local_part.parse()?))
    }
}

/// Sérialisé/désérialisé comme une chaîne — même raison que
/// [`crate::agent::AgentId`] (sert de clé de `HashMap` dans
/// [`crate::session::model::Session::orchestrations`]).
impl Serialize for OrchestrationFrameId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for OrchestrationFrameId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let repr = String::deserialize(deserializer)?;
        repr.parse().map_err(serde::de::Error::custom)
    }
}

/// Référence à un enfant d'une [`OrchestrationFrame`] — un agent nu ou un
/// sous-graphe indépendant (voir
/// [`crate::session::state::executable::ChildTask`]), chacun sa propre unité
/// d'exécution adressable (son propre [`AgentId`]/[`GraphFrameId`], resoumis
/// comme Job séparé) — contrairement à un [`crate::session::state::Cursor`]
/// issu d'un [`crate::session::state::NodeKind::Fork`], qui vit dans le même
/// `StateGraph`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChildRef {
    Agent(AgentId),
    Graph(GraphFrameId),
}

impl std::fmt::Display for ChildRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChildRef::Agent(id) => write!(f, "agent:{id}"),
            ChildRef::Graph(id) => write!(f, "graph:{id}"),
        }
    }
}

impl std::str::FromStr for ChildRef {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (kind, id) = s.split_once(':').ok_or_else(|| anyhow::anyhow!("format de ChildRef invalide : {s}"))?;
        match kind {
            "agent" => Ok(ChildRef::Agent(id.parse()?)),
            "graph" => Ok(ChildRef::Graph(id.parse()?)),
            _ => Err(anyhow::anyhow!("préfixe de ChildRef inconnu : {kind}")),
        }
    }
}

/// Sérialisé/désérialisé comme une chaîne plutôt que via le `derive` par
/// défaut (qui produirait un objet `{"Agent": ...}`/`{"Graph": ...}`,
/// incompatible avec une clé de `HashMap` en JSON) — même raison que
/// [`crate::agent::AgentId`] : sert de clé de [`OrchestrationFrame::results`].
impl Serialize for ChildRef {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for ChildRef {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let repr = String::deserialize(deserializer)?;
        repr.parse().map_err(serde::de::Error::custom)
    }
}

/// Frame qui a déclenché une orchestration — soit un
/// [`AgentFrame`](crate::agent::frame::AgentFrame) (via `system/push-mode`),
/// soit un curseur d'un [`GraphFrame`](crate::session::state::frame::GraphFrame)
/// (nœud [`Executable::Orchestration`](crate::session::state::executable::Executable::Orchestration)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Waiter {
    Agent(AgentId),
    Graph(GraphFrameId),
}

/// Fan-out de sous-tâches avec jointure AND, satellite de
/// [`AgentFrame`](crate::agent::frame::AgentFrame) au même titre que
/// [`GraphFrame`](crate::session::state::frame::GraphFrame) (voir la doc de
/// [`crate::session::state`]) — contrairement à ce dernier, sans Job dédié :
/// purement réactif, mis à jour depuis `session::server` à chaque fois qu'un
/// enfant rapporte son résultat (voir `session::server::report_agent_run`/
/// `report_graph_run`, généralisés pour scanner `pending`).
///
/// Distinct du parallélisme topologique (`NodeKind::Fork`/`NodeKind::Join`
/// d'un [`GraphFrame`]) : cardinalité décidée à l'exécution (`children` peut
/// dépendre d'une donnée connue seulement au runtime), pas déclarée dans la
/// topologie d'un graphe — voir la doc du module pour la distinction
/// complète.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrchestrationFrame {
    pub id: OrchestrationFrameId,
    pub owner: Waiter,
    pub strategy: OrchestrationStrategy,
    /// Tous les enfants prévus, dans l'ordre de création.
    pub children: Vec<ChildRef>,
    /// Sous-ensemble ordonné de `children` déjà inséré dans `Session` et
    /// soumis comme Job — pour [`OrchestrationStrategy::Parallel`], égal à
    /// `children` dès la création ; pour [`OrchestrationStrategy::Sequential`],
    /// grandit d'un élément à la fois (voir `session::server::push_orchestration`/
    /// le réveil en cascade dans `report_agent_run`/`report_graph_run`), le
    /// suivant n'étant soumis qu'une fois le précédent retiré de `pending`.
    pub spawned: Vec<ChildRef>,
    /// Enfants encore attendus — AND-join, se vide au fil des rapports (voir
    /// la doc de [`crate::agent::status::YieldStatus::WaitingAgents`] pour la
    /// même sémantique côté `AgentFrame`).
    pub pending: Vec<ChildRef>,
    pub results: HashMap<ChildRef, Value>,
    pub status: AgentStatus,
}
