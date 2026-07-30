use std::{fmt, str::FromStr};

use bytemuck::{Pod, Zeroable};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{id::ID, session::frames::FrameId};

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
    type Err = crate::Error;

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
    pub root_frame: Option<FrameId>,
    /// Statut de haut niveau de la session — indépendant de
    /// [`crate::session::frames::FrameStatus`], qui suit l'état d'un frame
    /// individuel de son arbre, pas la session dans son ensemble.
    pub status: SessionStatus,
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

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub enum SessionStatus {
    #[default]
    Pending,
    Archived,
    Ongoing,
    Failed(String)
}