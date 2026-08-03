use std::collections::HashMap;

use async_trait::async_trait;
use parking_lot::Mutex;

use crate::session::SessionId;
use crate::session::frames::FrameId;
use crate::session::snapshot::Snapshot;
use crate::session::store::InMemorySessionStore;

use super::StoreSessionSnapshot;

/// Erreur renvoyée par [`InMemorySessionSnapshotStore`] — seul cas où
/// [`StoreSessionSnapshot`] peut échouer sans backend externe : une absence,
/// jamais une panne de connexion/désérialisation comme côté
/// [`crate::store::PgStore`].
#[derive(Debug, thiserror::Error)]
pub enum InMemorySessionSnapshotStoreError {
    #[error("cliché ({0}, {1}, superstep {2}) introuvable")]
    SnapshotNotFound(SessionId, FrameId, u32),
}

/// Implémentation en mémoire de [`StoreSessionSnapshot`], autonome —
/// contrairement à [`InMemorySessionStore`] (qui délègue à ce type, voir sa
/// doc), elle ne connaît que les clichés, pas le reste d'une session (pas de
/// `Session`/`FrameNode`/`HitlFrame` à consulter).
#[derive(Default)]
pub struct InMemorySessionSnapshotStore {
    pub(crate) snapshots: Mutex<HashMap<(SessionId, FrameId, u32), Snapshot>>,
}

impl InMemorySessionSnapshotStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl StoreSessionSnapshot for InMemorySessionSnapshotStore {
    async fn latest_snapshot(&self, id: &SessionId, frame_id: &FrameId) -> crate::Result<Snapshot> {
        Ok(self
            .snapshots
            .lock()
            .values()
            .filter(|snapshot| snapshot.session_id == *id && snapshot.frame_id == *frame_id)
            .max_by_key(|snapshot| snapshot.superstep)
            .cloned()
            .ok_or(InMemorySessionSnapshotStoreError::SnapshotNotFound(*id, *frame_id, 0))?)
    }

    async fn snapshot_at(&self, id: &SessionId, frame_id: &FrameId, superstep: u32) -> crate::Result<Snapshot> {
        Ok(self
            .snapshots
            .lock()
            .get(&(*id, *frame_id, superstep))
            .cloned()
            .ok_or(InMemorySessionSnapshotStoreError::SnapshotNotFound(*id, *frame_id, superstep))?)
    }

    async fn upsert_snapshot(&self, snapshot: Snapshot) -> crate::Result<()> {
        self.snapshots.lock().insert((snapshot.session_id, snapshot.frame_id, snapshot.superstep), snapshot);
        Ok(())
    }
}

/// Implémentation en mémoire de [`StoreSessionSnapshot`] pour
/// [`InMemorySessionStore`] — pure délégation à
/// [`InMemorySessionSnapshotStore`] (voir sa doc pour la logique réelle) : le
/// champ `snapshots` d'[`InMemorySessionStore`] en est une instance, pas une
/// `HashMap` brute, pour ne pas dupliquer la logique entre les deux.
#[async_trait]
impl StoreSessionSnapshot for InMemorySessionStore {
    async fn latest_snapshot(&self, id: &SessionId, frame_id: &FrameId) -> crate::Result<Snapshot> {
        self.snapshots.latest_snapshot(id, frame_id).await
    }

    async fn snapshot_at(&self, id: &SessionId, frame_id: &FrameId, superstep: u32) -> crate::Result<Snapshot> {
        self.snapshots.snapshot_at(id, frame_id, superstep).await
    }

    async fn upsert_snapshot(&self, snapshot: Snapshot) -> crate::Result<()> {
        self.snapshots.upsert_snapshot(snapshot).await
    }
}
