pub mod in_memory;
#[cfg(feature = "catalog")]
pub mod postgres;

use std::ops::Deref;
use std::sync::Arc;

use async_trait::async_trait;

pub use in_memory::InMemorySessionSnapshotStore;

use crate::session::SessionId;
use crate::session::frames::FrameId;

use super::Snapshot;

/// Sous-ensemble de [`crate::session::store::StoreSession`] dédié aux
/// clichés de session (`marie_session_snapshots`) — séparé du reste
/// (session/frames/hitls/logs) sur le même principe que
/// [`crate::session::frames::store::StoreSessionFrame`]/
/// [`crate::hitl::store::StoreHitl`]/
/// [`crate::session::logs::store::StoreSessionLogs`].
#[async_trait]
pub trait StoreSessionSnapshot {
    async fn latest_snapshot(&self, id: &SessionId, frame_id: &FrameId) -> crate::Result<Snapshot>;
    async fn snapshot_at(&self, id: &SessionId, frame_id: &FrameId, superstep: u32) -> crate::Result<Snapshot>;
    async fn upsert_snapshot(&self, snapshot: Snapshot) -> crate::Result<()>;
}

/// Type opaque enveloppant l'implémentation concrète de
/// [`StoreSessionSnapshot`] — même rôle que
/// [`crate::session::store::SessionStore`] vis-à-vis de
/// [`crate::session::store::StoreSession`] : `Arc<dyn StoreSessionSnapshot +
/// Send + Sync + 'static>` plutôt qu'un paramètre générique, pour qu'un
/// appelant qui n'a besoin que de lire/écrire des clichés (voir
/// [`crate::session::snapshot::SnapshotContainer`]) puisse dépendre de ce
/// type seul, sans tirer l'intégralité de
/// [`crate::session::store::SessionStore`].
#[derive(Clone)]
pub struct SessionSnapshotStore(Arc<dyn StoreSessionSnapshot + Send + Sync + 'static>);

impl SessionSnapshotStore {
    pub fn new(store: Arc<dyn StoreSessionSnapshot + Send + Sync + 'static>) -> Self {
        Self(store)
    }

    pub fn in_memory() -> Self {
        Self::new(Arc::new(InMemorySessionSnapshotStore::new()))
    }
}

impl Deref for SessionSnapshotStore {
    type Target = dyn StoreSessionSnapshot + Send + Sync + 'static;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}
