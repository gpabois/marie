pub mod client;
pub mod crdt;
pub mod sync;

use std::collections::HashMap;

use serde_json::Value;

use crate::{agent::context::ContextEntry, id::ID, session::SessionId};

pub type WorkspaceId = ID;

/// API métier d'un workspace : un espace de travail qui regroupe plusieurs
/// [`SessionId`] (voir `ControlPlaneState::session_workspaces` côté control
/// plane, qui répertorie l'appartenance — une session donnée n'appartient
/// jamais qu'à un seul workspace à la fois) et porte un état partagé entre
/// elles, sous deux formes complémentaires : un fil de [`ContextEntry`]
/// (mémoire/discussion commune, même type que le contexte d'un frame) et un
/// store clé-valeur libre (`String` -> [`Value`], sans structure imposée,
/// à charge des agents de s'accorder sur ce qu'ils y rangent).
///
/// Comme [`crate::session::SessionApi`], délibérément indépendante de son
/// mécanisme de synchronisation — voir [`crdt::YrsWorkspace`], seule
/// implémentation à ce jour, sur exactement le même principe qu'un
/// `session::crdt::YrsSession` (CRDT `yrs`, diffs échangés entre pairs via
/// gossip plutôt que Raft : cet état est amené à être écrit en continu par
/// plusieurs agents concurrents).
pub trait WorkspaceApi {
    /// Identifiant du workspace.
    fn id(&self) -> WorkspaceId;

    /// Rattache `session_id` au workspace — sans effet si elle en fait déjà
    /// partie. Ne modifie pas l'appartenance côté control plane (voir
    /// `RpcCall::SET_SESSION_WORKSPACE`) : c'est la responsabilité de
    /// l'appelant (voir `workspace::client::WorkspaceClient::add_session`).
    fn add_session(&mut self, session_id: SessionId) -> anyhow::Result<()>;

    /// Détache `session_id` du workspace — sans effet si elle n'en fait pas
    /// partie.
    fn remove_session(&mut self, session_id: SessionId) -> anyhow::Result<()>;

    /// Sessions actuellement membres du workspace.
    fn sessions(&self) -> Vec<SessionId>;

    /// Ajoute une entrée au fil de contexte partagé du workspace.
    fn push_context_entry(&mut self, entry: &ContextEntry) -> anyhow::Result<()>;

    /// Fil de contexte partagé complet, dans l'ordre d'ajout.
    fn context(&self) -> Vec<ContextEntry>;

    /// Définit (crée ou remplace) une valeur du store clé-valeur partagé.
    fn set_value(&mut self, key: &str, value: &Value) -> anyhow::Result<()>;

    /// Retire une clé du store clé-valeur partagé — sans effet si elle
    /// n'existe pas.
    fn remove_value(&mut self, key: &str) -> anyhow::Result<()>;

    /// Valeur associée à `key` dans le store clé-valeur partagé, si connue.
    fn value(&self, key: &str) -> Option<Value>;

    /// Snapshot complet du store clé-valeur partagé.
    fn values(&self) -> HashMap<String, Value>;
}
