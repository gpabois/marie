use std::{collections::HashMap, ops::{Deref, DerefMut}, sync::Arc};

use chrono::{DateTime, Utc};
use moka::future::Cache;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{di::{Constructible, Get}, session::{SessionId, channel::ChannelName, controller::SessionError, frames::FrameId, snapshot::store::SessionSnapshotStore}};

pub mod store;

#[derive(Debug, Hash, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotRef {
    pub session_id: SessionId,
    pub frame_id: FrameId,
    pub superstep: u32
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Snapshot {
    pub session_id: SessionId,
    pub frame_id: FrameId,
    pub superstep: u32,
    pub channels: HashMap<ChannelName, Value>,
    pub join_sources: Vec<SnapshotRef>,
    pub created_at: DateTime<Utc>,
    pub pin_count: u32
}

impl Snapshot {
    pub fn new(session_id: SessionId, frame_id: FrameId, superstep: u32, channels: HashMap<ChannelName, Value>, join_sources: Vec<SnapshotRef>) -> Self {
        Self {
            session_id,
            frame_id,
            superstep,
            channels,
            join_sources,
            created_at: Utc::now(),
            pin_count: 0
        }
    }
}

impl Snapshot {
    pub fn r#ref(&self) -> SnapshotRef {
        SnapshotRef { session_id: self.session_id, frame_id: self.frame_id, superstep: self.superstep }
    }
}

pub struct SnapshotContainer {
    pub store: SessionSnapshotStore,
    pub snapshot: Snapshot,
    pub dirty: bool
}

impl SnapshotContainer {
    /// Charge le cliché `(session_id, frame_id, superstep)` depuis `store`
    /// — même rôle pour [`Snapshots::arena`] que
    /// [`crate::session::frames::FrameNodeContainer::new`] pour
    /// `FrameTree::arena` : point d'entrée unique vers [`StoreSession`], un
    /// cliché n'existe dans le cache qu'enveloppé ici, jamais comme
    /// `Snapshot` nu.
    pub async fn new(store: SessionSnapshotStore, session_id: SessionId, frame_id: FrameId, superstep: u32) -> crate::Result<Self> {
        let snapshot = store.snapshot_at(&session_id, &frame_id, superstep).await?;
        Ok(Self::from_loaded(store, snapshot))
    }

    /// Enveloppe un [`Snapshot`] déjà persisté (chargé par [`Self::new`] ou
    /// [`Snapshots::latest`]) — `dirty` à `false`, contrairement à
    /// [`Self::from_snapshot`].
    fn from_loaded(store: SessionSnapshotStore, snapshot: Snapshot) -> Self {
        Self { store, snapshot, dirty: false }
    }

    /// Enveloppe un [`Snapshot`] fraîchement créé en mémoire, pas encore
    /// persisté (voir [`Snapshots::insert`]) — `dirty` à `true` d'emblée
    /// pour qu'il soit écrit au premier [`Self::flush`]/à son éviction du
    /// cache.
    fn from_snapshot(store: SessionSnapshotStore, snapshot: Snapshot) -> Self {
        Self { store, snapshot, dirty: true }
    }

    /// Persiste `snapshot` si `dirty` est levé, puis rabaisse le drapeau —
    /// même logique que
    /// [`crate::session::frames::FrameNodeContainer::flush`].
    pub async fn flush(&mut self) -> crate::Result<()> {
        if !self.dirty {
            return Ok(());
        }

        self.store.upsert_snapshot(self.snapshot.clone()).await?;
        self.dirty = false;

        Ok(())
    }
}

impl Deref for SnapshotContainer {
    type Target = Snapshot;

    fn deref(&self) -> &Self::Target {
        &self.snapshot
    }
}

impl DerefMut for SnapshotContainer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.dirty = true;
        &mut self.snapshot
    }
}

impl Drop for SnapshotContainer {
    /// Comme [`crate::session::frames::FrameNodeContainer::drop`] : `Drop`
    /// ne peut pas être `async`, donc on détache un `tokio::spawn`
    /// fire-and-forget plutôt que de bloquer sur [`Self::flush`].
    fn drop(&mut self) {
        if !self.dirty {
            return;
        }

        let store = self.store.clone();
        let snapshot = self.snapshot.clone();

        tokio::spawn(async move {
            if let Err(err) = store.upsert_snapshot(snapshot).await {
                tracing::warn!("échec de la persistance d'un cliché évincé du cache: {err}");
            }
        });
    }
}

#[derive(Clone)]
pub struct SessionSnapshotsFactory(Arc<dyn Fn(SessionId) -> SessionSnapshots + Send + Sync + 'static>);

impl SessionSnapshotsFactory {
    pub fn new<F>(factory: F) -> Self where F: Fn(SessionId) -> SessionSnapshots + Send + Sync + 'static {
        Self(Arc::new(factory))
    }

    pub fn create(&self, session_id: SessionId) -> SessionSnapshots {
        (self.0)(session_id)
    }
}

impl<C> Constructible<C> for SessionSnapshotsFactory where 
    C: Get<SessionSnapshotStore> + Clone + Send + Sync + 'static
{
    fn construct(container: &C, _: ()) -> Self {
        let container = container.clone();
        Self::new(move |session_id| {
            SessionSnapshots::new(container.get(), session_id)
        })
    }
}

/// Clichés d'une session — même rôle vis-à-vis de [`Snapshot`] que
/// `session::frames::FrameTree` vis-à-vis de `FrameNode` : cache de
/// conteneurs adossé à Postgres (voir `marie_session_snapshots`,
/// `migrations/0016_session_snapshot.sql`), indexé par [`SnapshotRef`]
/// (`(session_id, frame_id, superstep)`, sa clé primaire — voir la doc de
/// [`StoreSession::upsert_snapshot`]) plutôt que par `frame_id` seul,
/// puisque plusieurs clichés coexistent pour un même frame.
pub struct SessionSnapshots {
    store: SessionSnapshotStore,
    session_id: SessionId,
    snapshots: Cache<SnapshotRef, Arc<Mutex<SnapshotContainer>>>
}

impl SessionSnapshots {
    pub fn new(store: SessionSnapshotStore, session_id: SessionId)-> Self {
        Self {
            store,
            session_id,
            snapshots: Cache::builder().build()
        }
    }

    /// Enregistre un nouveau cliché en mémoire et le met en cache — pas
    /// encore persisté (voir [`SnapshotContainer::from_snapshot`]) : un
    /// premier [`SnapshotContainer::flush`] (ou son éviction du cache)
    /// l'écrira dans `marie_session_snapshots`.
    pub async fn push(&self, snapshot: Snapshot) -> SnapshotRef {
        let r#ref = snapshot.r#ref();
        let container = SnapshotContainer::from_snapshot(self.store.clone(), snapshot);
        self.snapshots.insert(r#ref.clone(), Arc::new(Mutex::new(container))).await;
        r#ref
    }

    /// Cliché `(frame_id, superstep)`, depuis le cache si déjà chargé,
    /// sinon chargé depuis `store` (voir [`SnapshotContainer::new`]) — même
    /// idiome que `FrameTree::get`.
    ///
    /// Panique si le cliché n'existe pas ou si le chargement échoue — voir
    /// [`Self::try_get`] pour le chemin qui laisse l'appelant décider.
    pub async fn get(&self, frame_id: &FrameId, superstep: u32) -> Arc<Mutex<SnapshotContainer>> {
        self.try_get(frame_id, superstep).await.expect("cliché introuvable ou erreur de stockage")
    }

    /// Comme [`Self::get`], sans paniquer.
    pub async fn try_get(&self, frame_id: &FrameId, superstep: u32) -> Result<Arc<Mutex<SnapshotContainer>>, SessionError> {
        let store = self.store.clone();
        let session_id = self.session_id;
        let frame_id = *frame_id;
        let r#ref = SnapshotRef { session_id, frame_id, superstep };

        self.snapshots
            .try_get_with(r#ref, async move {
                SnapshotContainer::new(store, session_id, frame_id, superstep)
                    .await
                    .map(|container| Arc::new(Mutex::new(container)))
            })
            .await
            .map_err(SessionError::StorageError)
    }

    /// Dernier cliché connu de `frame_id` (plus grand `superstep`, voir
    /// [`StoreSession::latest_snapshot`]). Contrairement à
    /// [`Self::get`]/[`Self::try_get`], ne peut pas passer par
    /// `Cache::try_get_with` : l'arène est indexée par `(frame_id,
    /// superstep)` (voir [`SnapshotRef`]), et le superstep du dernier
    /// cliché n'est justement pas connu avant d'avoir interrogé `store`. Le
    /// résultat est mis en cache après coup, pour que les accès suivants à
    /// ce superstep précis (via [`Self::get`]) le trouvent déjà chargé.
    pub async fn latest(&self, frame_id: &FrameId) -> Result<Arc<Mutex<SnapshotContainer>>, SessionError> {
        let snapshot = self
            .store
            .latest_snapshot(&self.session_id, frame_id)
            .await
            .map_err(|err| SessionError::StorageError(Arc::new(err)))?;

        let r#ref = snapshot.r#ref();
        let container = Arc::new(Mutex::new(SnapshotContainer::from_loaded(self.store.clone(), snapshot)));
        self.snapshots.insert(r#ref, container.clone()).await;

        Ok(container)
    }
}