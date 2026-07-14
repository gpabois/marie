use serde::{Deserialize, Serialize};

use crate::workspace::WorkspaceId;

/// Topic gossipsub sur lequel circule le contenu CRDT des workspaces (diffs
/// yrs, voir [`WorkspaceSyncMessage`]) — même principe que
/// `session::sync::SESSION_SYNC_TOPIC` : un seul topic fixe pour tous les
/// workspaces (filtré par `workspace_id` côté abonné) plutôt qu'un topic par
/// workspace.
///
/// Partagé entre tout composant qui détient un workspace —
/// `workspace::client::WorkspaceClient`, sur ce nœud comme sur tout autre
/// worker exécutant une session membre du même workspace — pour qu'ils se
/// synchronisent mutuellement sans se connaître autrement que par ce topic.
pub const WORKSPACE_SYNC_TOPIC: &str = "marie/worker/workspace-sync/1.0.0";

/// Message gossipé sur [`WORKSPACE_SYNC_TOPIC`] : un diff yrs incrémental
/// pour `workspace_id`, à appliquer via
/// `workspace::crdt::YrsWorkspace::apply_diff` par quiconque détient déjà ce
/// workspace localement (les autres l'ignorent).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceSyncMessage {
    pub workspace_id: WorkspaceId,
    pub diff: Vec<u8>,
}
