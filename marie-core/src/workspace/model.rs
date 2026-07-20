use std::{collections::HashMap, fmt, str::FromStr};

use bytemuck::{Pod, Zeroable};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{id::ID, session::SessionId};

#[derive(Debug, Hash, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct WorkspaceId(ID);

impl WorkspaceId {
    #[must_use]
    pub fn new(id: ID) -> Self {
        Self(id)
    }
}

impl fmt::Display for WorkspaceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl FromStr for WorkspaceId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

impl From<ID> for WorkspaceId {
    fn from(id: ID) -> Self {
        Self(id)
    }
}

impl AsRef<[u8]> for WorkspaceId {
    fn as_ref(&self) -> &[u8] {
        bytemuck::bytes_of(self)
    }
}

/// État d'un workspace : le regroupement logique de sessions (voir
/// [`crate::session::Session`]) et les variables qu'elles partagent.
///
/// Struct concrète et typée plutôt qu'un document CRDT (l'ancien
/// `YrsWorkspace`, disparu avec le control plane) : depuis que chaque
/// workspace est servi par un unique pair propriétaire (sélection
/// déterministe via `bootstrap.select_peer`, voir
/// [`crate::workspace::client::WorkspaceClient`]), il n'y a plus d'écriture
/// concurrente entre pairs à fusionner — même choix que
/// [`crate::session::Session`]/[`crate::model::Model`], et mêmes bénéfices :
/// colonnes lisibles côté store (voir `workspace::store`) au lieu d'un blob
/// opaque, et types serde réguliers sur le réseau.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Workspace {
    pub id: WorkspaceId,
    pub sessions: Vec<SessionId>,
    /// Variables partagées du workspace — même sémantique que
    /// [`Session::vars`](crate::session::Session) : traitées comme un unique
    /// document JSON requêté/patché par expression JSONPath (voir
    /// [`crate::workspace::rpc::QueryVars`]/[`crate::workspace::rpc::PatchVars`]).
    pub vars: HashMap<String, Value>,
    /// Horodatage géré par le store (voir
    /// `workspace::store::WorkspaceStore::insert`), pas par l'appelant :
    /// toute valeur posée ici avant un `insert` est ignorée, écrasée par
    /// l'heure serveur au moment de l'écriture.
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Comme [`Self::created_at`], géré par le store — mis à jour à chaque
    /// `insert`/`replace` (voir `workspace::store::WorkspaceStore::replace`),
    /// contrairement à `created_at` qu'un `replace` laisse intact.
    pub last_updated_at: chrono::DateTime<chrono::Utc>,
}

impl Workspace {
    #[must_use]
    pub fn new(id: WorkspaceId) -> Self {
        Self {
            id,
            sessions: Vec::new(),
            vars: HashMap::new(),
            created_at: chrono::Utc::now(),
            last_updated_at: chrono::Utc::now(),
        }
    }

    /// Rattache `session_id` au workspace — idempotent, un rattachement
    /// rejoué (RPC retenté) ne crée pas de doublon.
    pub fn add_session(&mut self, session_id: SessionId) {
        if !self.sessions.contains(&session_id) {
            self.sessions.push(session_id);
        }
    }

    /// Détache `session_id` du workspace — sans effet s'il n'y était pas
    /// (même idempotence que [`Self::add_session`]).
    pub fn remove_session(&mut self, session_id: &SessionId) {
        self.sessions.retain(|id| id != session_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::generate_id;

    #[test]
    fn add_session_is_idempotent() {
        let mut workspace = Workspace::new(WorkspaceId::new(generate_id()));
        let session_id = SessionId::new(generate_id());

        workspace.add_session(session_id);
        workspace.add_session(session_id);

        assert_eq!(workspace.sessions, vec![session_id]);
    }

    #[test]
    fn remove_session_is_idempotent() {
        let mut workspace = Workspace::new(WorkspaceId::new(generate_id()));
        let kept = SessionId::new(generate_id());
        let removed = SessionId::new(generate_id());

        workspace.add_session(kept);
        workspace.add_session(removed);

        workspace.remove_session(&removed);
        workspace.remove_session(&removed);

        assert_eq!(workspace.sessions, vec![kept]);
    }
}
