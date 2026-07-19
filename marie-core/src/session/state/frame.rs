use bytemuck::{Pod, Zeroable};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    agent::{AgentId, status::AgentStatus},
    id::ID,
    session::{SessionId, state::StateGraph, state::orchestration::OrchestrationFrameId},
};

/// Identifiant d'un [`GraphFrame`], scopé à sa session — même forme que
/// [`AgentId`] (une session peut porter plusieurs graphes en cours, comme
/// elle porte plusieurs [`AgentFrame`](crate::agent::frame::AgentFrame)).
#[derive(Debug, Hash, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct GraphFrameId(SessionId, ID);

impl GraphFrameId {
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

impl AsRef<[u8]> for GraphFrameId {
    fn as_ref(&self) -> &[u8] {
        bytemuck::bytes_of(self)
    }
}

impl std::fmt::Display for GraphFrameId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.0, self.1)
    }
}

impl std::str::FromStr for GraphFrameId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (session_part, local_part) = s.split_once('/').ok_or_else(|| anyhow::anyhow!("format de GraphFrameId invalide : {s}"))?;
        Ok(Self(session_part.parse()?, local_part.parse()?))
    }
}

/// Sérialisé/désérialisé comme une chaîne plutôt que via le `derive` par
/// défaut — même raison que [`crate::agent::AgentId`] : sert de clé de
/// `HashMap` dans [`crate::session::model::Session::graphs`], sérialisé en
/// JSON par [`crate::session::catalog::SessionCatalog`].
impl Serialize for GraphFrameId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for GraphFrameId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let repr = String::deserialize(deserializer)?;
        repr.parse().map_err(serde::de::Error::custom)
    }
}

/// Ce qui a poussé un [`GraphFrame`] — un [`AgentFrame`](crate::agent::frame::AgentFrame)
/// directement (via `system/push-mode`), ou une
/// [`OrchestrationFrame`](crate::session::state::orchestration::OrchestrationFrame)
/// qui l'a créé comme enfant (voir [`crate::session::state::executable::ChildTask::Graph`]) —
/// dans ce second cas, il n'y a pas d'[`AgentId`] direct à porter tant que la
/// chaîne de propriété n'a pas été remontée jusqu'à un agent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GraphOwner {
    Agent(AgentId),
    Orchestration(OrchestrationFrameId),
}

/// Issue d'un `GraphFrame`, rapportée par le driver
/// (`session::state::worker::RunGraphStep`) à `SessionServer` en toute fin
/// de son dernier pas — même modèle qu'[`crate::agent::status::AgentResponse`]
/// côté `AgentFrame`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GraphResponse {
    Finished { output: Value },
    Failed { error: String },
}

/// Un niveau de la pile de composition hiérarchique d'un [`GraphFrame`] — le
/// `StateGraph` en cours d'exécution à ce niveau, et le nœud du niveau
/// parent où reprendre une fois ce niveau conclu (`None` pour le niveau
/// racine).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphStackFrame {
    pub graph: StateGraph,
    pub return_node: Option<String>,
}

/// État d'exécution d'un State Graph, satellite de [`AgentFrame`](crate::agent::frame::AgentFrame)
/// au même titre qu'[`OrchestrationFrame`](crate::session::state::orchestration::OrchestrationFrame)
/// (voir la doc de [`crate::session::state`]) — poussé par un `AgentFrame`
/// (`owner`, via `system/push-mode`) ou par un nœud d'un autre `GraphFrame`
/// (composition en profondeur, voir [`GraphStackFrame`]).
///
/// `stack` porte toujours au moins un niveau : un graphe "fixe" (topologie
/// figée, instancié une fois depuis le catalogue ou construit inline) n'en a
/// jamais qu'un seul ; un nœud [`crate::session::state::executable::Executable::Subgraph`]
/// en empile un second le temps de son exécution (voir
/// `crate::session::state::worker::RunGraphStep`, le driver qui pilote
/// toujours le *sommet* de la pile).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphFrame {
    pub id: GraphFrameId,
    pub owner: GraphOwner,
    pub stack: Vec<GraphStackFrame>,
    pub error: String,
}

impl GraphFrame {
    #[must_use]
    pub fn top(&self) -> &GraphStackFrame {
        self.stack.last().expect("GraphFrame::stack ne doit jamais être vide")
    }

    #[must_use]
    pub fn top_mut(&mut self) -> &mut GraphStackFrame {
        self.stack.last_mut().expect("GraphFrame::stack ne doit jamais être vide")
    }

    /// Statut dérivé de l'ensemble des curseurs actifs du sommet de la pile
    /// (voir la doc de [`crate::session::state::Cursor`]) : `Running` si au
    /// moins un curseur est prêt à avancer, `Yielding` si tous les curseurs
    /// actifs sont bloqués (v1 : celui du premier curseur bloqué rencontré —
    /// agréger plusieurs raisons de blocage simultanées est un raffinement
    /// possible, pas bloquant), `Finished` quand tous les curseurs du niveau
    /// racine ont conclu. Un niveau non-racine entièrement conclu reste
    /// `Running` : c'est au driver de dépiler (voir
    /// `crate::session::state::worker::RunGraphStep`) avant que ce statut ne
    /// redevienne pertinent.
    #[must_use]
    pub fn status(&self) -> AgentStatus {
        let cursors = &self.top().graph.cursors;

        if cursors.iter().any(|cursor| cursor.status == AgentStatus::Running) {
            return AgentStatus::Running;
        }

        if let Some(status) = cursors.iter().find(|cursor| matches!(cursor.status, AgentStatus::Yielding(_))).map(|cursor| cursor.status.clone()) {
            return status;
        }

        if cursors.iter().any(|cursor| cursor.status == AgentStatus::Failed) {
            return AgentStatus::Failed;
        }

        if !cursors.is_empty() && cursors.iter().all(|cursor| cursor.status == AgentStatus::Finished) && self.stack.len() == 1 {
            return AgentStatus::Finished;
        }

        // Curseurs tous parqués dans `graph.joins` (en attente d'un
        // rendez-vous), ou sommet de pile conclu mais niveaux parents encore
        // à dépiler : dans les deux cas, le `GraphFrame` doit continuer à
        // être piloté.
        AgentStatus::Running
    }
}
