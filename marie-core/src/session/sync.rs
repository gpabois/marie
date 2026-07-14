use serde::{Deserialize, Serialize};

use crate::session::SessionId;

/// Topic gossipsub sur lequel circule le contenu CRDT des sessions (diffs
/// yrs, voir [`SessionSyncMessage`]) — un seul topic fixe pour toutes les
/// sessions (filtré par `session_id` côté abonné) plutôt qu'un topic par
/// session : plus simple, et cohérent avec le reste du cluster qui est de
/// toute façon de petite taille (mDNS/LAN, voir `network::start_swarm`).
///
/// Partagé entre tout composant qui détient ou archive des sessions —
/// `session::client::SessionClient` (détention transitoire,
/// tant qu'un job tourne) et `network::persistency` (détention durable, voir
/// `crate::persistency`) — pour qu'ils se synchronisent mutuellement sans se
/// connaître autrement que par ce topic.
pub const SESSION_SYNC_TOPIC: &str = "marie/worker/session-sync/1.0.0";

/// Message gossipé sur [`SESSION_SYNC_TOPIC`] : un diff yrs incrémental pour
/// `session_id`, à appliquer via `session::crdt::YrsSession::apply_diff` par
/// quiconque détient déjà cette session localement (les autres l'ignorent,
/// sauf `network::persistency` qui doit d'abord rattraper l'état complet
/// auprès de l'émetteur — voir la note sur les racines concurrentes dans
/// `session::crdt::YrsSession::from_diff`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionSyncMessage {
    pub session_id: SessionId,
    pub diff: Vec<u8>,
}
