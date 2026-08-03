pub mod in_memory;
#[cfg(feature = "catalog")]
pub mod postgres;

use std::ops::Deref;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

pub use in_memory::InMemorySessionLogsStore;

use crate::session::SessionId;
use crate::session::logs::SessionLog;

/// Bornes optionnelles pour [`StoreSessionLogs::list_log_after`] — `after`/
/// `before` filtrent sur `SessionLog::created_at`, chacun ignoré (aucune
/// borne côté correspondant) quand `None`.
#[derive(Debug, Default, Clone, Copy)]
pub struct SearchLogQuery {
    pub after: Option<DateTime<Utc>>,
    pub before: Option<DateTime<Utc>>
}

/// Sous-ensemble de [`crate::session::store::StoreSession`] dédié au journal
/// d'une session (`marie_session_logs`) — séparé du reste
/// (session/frames/snapshots/hitls) sur le même principe que
/// [`crate::session::frames::store::StoreSessionFrame`]/
/// [`crate::hitl::store::StoreHitl`].
#[async_trait]
pub trait StoreSessionLogs {
    /// Insère `log`, ou remplace son contenu s'il existe déjà (voir la doc de
    /// [`SessionLog`] : un même `SessionLogId` peut recevoir plusieurs
    /// écritures successives, ce n'est pas un ajout immuable en fin de
    /// journal).
    async fn insert_log(&self, log: SessionLog) -> crate::Result<()>;
    /// Tout le journal de `session_id`, du plus ancien au plus récent.
    async fn list_log(&self, session_id: SessionId) -> crate::Result<Vec<SessionLog>>;
    /// Comme [`Self::list_log`], restreint aux bornes de `query`.
    async fn list_log_after(&self, session_id: SessionId, query: SearchLogQuery) -> crate::Result<Vec<SessionLog>>;
}

/// Type opaque enveloppant l'implémentation concrète de [`StoreSessionLogs`]
/// — même rôle que [`crate::session::frames::store::SessionFrameStore`]
/// vis-à-vis de
/// [`StoreSessionFrame`](crate::session::frames::store::StoreSessionFrame) :
/// `Arc<dyn StoreSessionLogs + Send + Sync + 'static>` plutôt qu'un paramètre
/// générique, pour qu'un appelant qui n'a besoin que de lire/écrire le
/// journal (voir [`crate::session::logs::SessionLogs`]) puisse dépendre de ce
/// type seul, sans tirer l'intégralité de
/// [`crate::session::store::SessionStore`].
#[derive(Clone)]
pub struct SessionLogStore(Arc<dyn StoreSessionLogs + Send + Sync + 'static>);

impl SessionLogStore {
    pub fn new(store: Arc<dyn StoreSessionLogs + Send + Sync + 'static>) -> Self {
        Self(store)
    }

    pub fn in_memory() -> Self {
        Self::new(Arc::new(InMemorySessionLogsStore::new()))
    }
}

impl Deref for SessionLogStore {
    type Target = dyn StoreSessionLogs + Send + Sync + 'static;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}
