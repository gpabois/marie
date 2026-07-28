use std::{fmt, str::FromStr};

use bytemuck::{Pod, Zeroable};
use serde::{Deserialize, Serialize};

use crate::{id::ID, session::model::SessionId};


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
    type Err = crate::Error;

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
    pub session_id: SessionId,
    pub content: SessionLogContent,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub last_updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SessionLogContent {
    Plain(String),
    AgentLog {
        label: String,
        content: String
    }
}
