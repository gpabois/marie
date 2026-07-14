pub mod client;
pub mod crdt;
pub mod sync;

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    agent::{context::ContextEntry, frame::AgentFrame, status::AgentStatus},
    id::ID,
    mode::SessionMode,
    tools::ToolCall,
};

pub type SessionId = ID;

/// API métier d'une session : création/lecture de frames, journal, pile de
/// modes — délibérément indépendante de son mécanisme de synchronisation.
/// L'unique implémentation aujourd'hui ([`crdt::YrsSession`]) est adossée à
/// un CRDT `yrs` (voir sa doc pour le pourquoi), mais rien dans cette API ne
/// l'exige : la synchronisation elle-même (vecteur d'état, calcul/application
/// d'un diff, construction depuis un diff reçu d'un pair) reste un détail
/// d'implémentation propre à `YrsSession`, volontairement hors de ce trait —
/// figer ce choix ici lierait toute future implémentation alternative au
/// même mécanisme de sync que la première.
pub trait SessionApi {
    /// Identifiant de la session.
    fn id(&self) -> SessionId;

    /// Enregistre l'état intégral d'un frame — utilisé à la prise en charge
    /// initiale d'un frame que ce worker n'a encore jamais vu.
    fn put_frame(&mut self, local_id: ID, frame: &AgentFrame) -> anyhow::Result<()>;

    /// Reconstruit un frame à partir de son état synchronisé, s'il est connu
    /// localement.
    fn frame(&self, local_id: ID) -> Option<AgentFrame>;

    /// Remplace le statut d'un frame connu (transition de cycle de vie, voir
    /// [`AgentStatus`]).
    fn set_status(&mut self, local_id: ID, status: &AgentStatus) -> anyhow::Result<()>;

    /// Ajoute une entrée au contexte d'un frame connu (nouveau message
    /// échangé avec le modèle).
    fn push_context_entry(&mut self, local_id: ID, entry: &ContextEntry) -> anyhow::Result<()>;

    /// Ajoute `chunk` à la sortie standard streamée d'un frame connu.
    fn append_stdio(&mut self, local_id: ID, chunk: &str) -> anyhow::Result<()>;

    /// Ajoute `chunk` à la sortie d'erreur streamée d'un frame connu.
    fn append_stderr(&mut self, local_id: ID, chunk: &str) -> anyhow::Result<()>;

    /// Ajoute une entrée au journal de la session (voir [`SessionLog`]).
    fn push_log(&mut self, log: &SessionLog) -> anyhow::Result<()>;

    /// Journal complet de la session, dans l'ordre d'ajout.
    fn logs(&self) -> Vec<SessionLog>;

    /// Empile `mode` au sommet de la pile de modes de la session. Rejette
    /// [`SessionMode::Simple`] : c'est le mode implicite d'une pile vide, il
    /// n'y a rien à empiler pour y revenir — [`Self::pop_mode`] suffit.
    fn push_mode(&mut self, mode: &SessionMode) -> anyhow::Result<()>;

    /// Remplace le mode au sommet de la pile par `mode`, sans changer la
    /// profondeur de la pile (contrairement à [`Self::push_mode`]/
    /// [`Self::pop_mode`]) — pour persister la *progression* d'un mode déjà
    /// empilé, pas pour en empiler un nouveau. Échoue s'il n'y a rien à
    /// remplacer (pile vide). Comme [`Self::push_mode`], rejette
    /// [`SessionMode::Simple`].
    fn update_current_mode(&mut self, mode: &SessionMode) -> anyhow::Result<()>;

    /// Dépile et retourne le mode au sommet de la pile, ou `None` si elle
    /// est déjà vide.
    fn pop_mode(&mut self) -> anyhow::Result<Option<SessionMode>>;

    /// Mode au sommet de la pile — [`SessionMode::Simple`] si elle est vide.
    fn current_mode(&self) -> SessionMode;

    /// Pile complète des modes, du fond (index 0) au sommet — pour
    /// inspection/debug ; [`Self::current_mode`] suffit au pilotage normal.
    fn mode_stack(&self) -> Vec<SessionMode>;

    /// Définit (crée ou remplace) une valeur du store clé-valeur libre de la
    /// session — backend de `/session/var` dans le VFS (voir
    /// `persistency::var::VarFileSystem`), sur le même principe que
    /// `workspace::WorkspaceApi::set_value`.
    fn set_value(&mut self, key: &str, value: &Value) -> anyhow::Result<()>;

    /// Retire une clé du store clé-valeur libre — sans effet si elle
    /// n'existe pas.
    fn remove_value(&mut self, key: &str) -> anyhow::Result<()>;

    /// Valeur associée à `key` dans le store clé-valeur libre, si connue.
    fn value(&self, key: &str) -> Option<Value>;

    /// Snapshot complet du store clé-valeur libre.
    fn values(&self) -> HashMap<String, Value>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionLog {
    id: ID,
    data: SessionLogSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SessionLogSpec {
    AgentMessage { label: String, message: String },
    ToolCall(ToolCall),
}
