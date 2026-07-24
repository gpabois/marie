use std::{collections::HashMap, fmt, str::FromStr};

use bytemuck::{Pod, Zeroable};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    agent::{AgentId, frame::AgentFrame, status::{AgentStatus, YieldStatus}}, 
    id::ID, 
    state::State, 
    graph::{GraphFrame, GraphFrameId}, 
};



#[derive(Debug, Hash, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Pod, Zeroable, JsonSchema)]
#[repr(C)]
pub struct SessionId(ID);

impl SessionId {
    #[must_use]
    pub fn new(id: ID) -> Self {
        Self(id)
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl FromStr for SessionId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

impl From<ID> for SessionId {
    fn from(id: ID) -> Self {
        Self(id)
    }
}

impl AsRef<[u8]> for SessionId {
    fn as_ref(&self) -> &[u8] {
        bytemuck::bytes_of(self)
    }
}



#[derive(Debug, Hash, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct SessionLogId(ID);

impl SessionLogId {
    #[must_use]
    pub fn new(id: ID) -> Self {
        Self(id)
    }
}

impl fmt::Display for SessionLogId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl FromStr for SessionLogId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

impl From<ID> for SessionLogId {
    fn from(id: ID) -> Self {
        Self(id)
    }
}

impl AsRef<[u8]> for SessionLogId {
    fn as_ref(&self) -> &[u8] {
        bytemuck::bytes_of(self)
    }
}

/// Une entrée du journal d'une session, identifiée par [`SessionLogId`] pour
/// permettre d'y ajouter du texte au fil de l'eau (voir
/// [`crate::session::rpc::InsertInLog`]) plutôt que de ne pouvoir qu'ajouter
/// des lignes complètes et immuables.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionLog {
    pub id: SessionLogId,
    pub content: String,
}


/// État d'une session — un ou plusieurs [`AgentFrame`], zéro ou plusieurs
/// [`GraphFrame`]/[`OrchestrationFrame`]/[`HitlFrame`] satellites (voir la
/// doc de [`crate::state_graph`] pour la symétrie de ces trois), un
/// journal d'évènements (`logs`) et un store clé-valeur libre (`vars`, voir
/// `persistency::var::SessionVarStore`).
///
/// Contrairement à un catalogue de déclarations (voir
/// [`crate::expert::Expert`]/[`crate::model::Model`]), une session est
/// amenée à être écrite en continu tant qu'un agent l'exécute — mais, sur le
/// même modèle que ces catalogues, [`Self::insert`]/[`Self::update`]
/// remplacent l'enregistrement entier plutôt que de fusionner un delta :
/// c'est à l'appelant (voir [`client::SessionClient`]) de renvoyer l'état
/// complet à jour à chaque mutation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub id: SessionId,
    pub frames: AgentFrameMap,
    pub graphs: HashMap<GraphFrameId, GraphFrame>,
    pub logs: Vec<SessionLog>,
    pub state: State,
    /// Horodatage géré par le store (voir
    /// `session::store::SessionStore::insert`), pas par l'appelant : toute
    /// valeur posée ici avant un `insert` est ignorée, écrasée par l'heure
    /// serveur au moment de l'écriture.
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Comme [`Self::created_at`], géré par le store — mis à jour à chaque
    /// `insert`/`replace` (voir `session::store::SessionStore::replace`),
    /// contrairement à `created_at` qu'un `replace` laisse intact.
    pub last_updated_at: chrono::DateTime<chrono::Utc>,
}

impl Session {
    pub fn state(&self) -> &State {
        &self.state
    }

    pub fn graph_frame(&self, id: &GraphFrameId) -> Option<&GraphFrame> {
        self.graphs.get(id)
    }
}


#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentFrameMap(HashMap<AgentId, AgentFrame>);

impl From<HashMap<AgentId, AgentFrame>> for AgentFrameMap {
    fn from(value: HashMap<AgentId, AgentFrame>) -> Self {
        Self(value)
    }
}

impl std::ops::Deref for AgentFrameMap {
    type Target = HashMap<AgentId, AgentFrame>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for AgentFrameMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl AgentFrameMap {
    pub fn iter_waiting_hitl(&self) -> impl Iterator<Item=&AgentFrame> {
        self.values().filter(|frame| matches!(&frame.status, AgentStatus::Yielding(YieldStatus::WaitingHitl { .. })))
    }
}