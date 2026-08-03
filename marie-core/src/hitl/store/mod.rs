pub mod in_memory;
#[cfg(feature = "catalog")]
pub mod postgres;

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use async_trait::async_trait;

pub use in_memory::InMemoryHitlStore;

use crate::hitl::dto::PendingHitl;
use crate::hitl::{Answer, Hitl, HitlFrame, HitlId};
use crate::session::SessionId;

/// Sous-ensemble de [`crate::session::store::StoreSession`] dédié aux
/// requêtes human-in-the-loop d'une session (`marie_session_hitls`) — séparé
/// du reste (session/frames/snapshots/logs) sur le même principe que
/// [`crate::session::frames::store::StoreSessionFrame`].
#[async_trait]
pub trait StoreHitl {
    async fn get_hitl_frame(&self, id: &SessionId, hitl_id: &HitlId) -> crate::Result<HitlFrame>;
    /// Comme [`Self::get_hitl_frame`], mais dépouillé de tout le reste
    /// (`answer`) — pour un appelant qui n'a besoin que de la question posée
    /// (ex. la représer à un humain), pas de l'état de réponse. La position
    /// dans l'arbre des frames ne vit plus sur [`HitlFrame`] (voir sa doc) :
    /// un appelant qui en a besoin passe par
    /// [`crate::session::frames::store::StoreSessionFrame::get_frame_id_by_hitl_id`].
    async fn get_hitl(&self, id: &SessionId, hitl_id: &HitlId) -> crate::Result<Hitl>;
    async fn list_unanswered_hitls_frames(&self, id: SessionId) -> crate::Result<Vec<HitlFrame>>;
    async fn upsert_hitl_frame(&self, hitl: HitlFrame) -> crate::Result<()>;
    async fn write_hitl_response(&self, id: &SessionId, hitl_id: &HitlId, answers: HashMap<String, Answer>) -> crate::Result<()>;

    /// Requêtes hitl en attente de `id`, sous forme de [`PendingHitl`] — même
    /// portée que [`Self::list_unanswered_hitls_frames`], mais dépouillée de
    /// `frame_id`/`answer` : un appelant qui veut seulement savoir "qu'est-ce
    /// qui attend une réponse" (voir
    /// [`crate::session::hitls::Hitls::get_pending_hitls`],
    /// [`crate::session::client::SessionClient::get_session`]) n'a pas besoin
    /// du [`HitlFrame`] complet.
    async fn list_pending_hitls(&self, id: SessionId) -> crate::Result<Vec<PendingHitl>>;
}

/// Type opaque enveloppant l'implémentation concrète de [`StoreHitl`] — même
/// rôle que [`crate::session::frames::store::SessionFrameStore`] vis-à-vis de
/// [`StoreSessionFrame`](crate::session::frames::store::StoreSessionFrame) :
/// `Arc<dyn StoreHitl + Send + Sync + 'static>` plutôt qu'un paramètre
/// générique, pour qu'un appelant qui n'a besoin que des requêtes hitl (voir
/// [`crate::session::hitls::Hitls`]) puisse dépendre de ce type seul, sans
/// tirer l'intégralité de [`crate::session::store::SessionStore`].
#[derive(Clone)]
pub struct HitlStore(Arc<dyn StoreHitl + Send + Sync + 'static>);

impl HitlStore {
    pub fn new(store: Arc<dyn StoreHitl + Send + Sync + 'static>) -> Self {
        Self(store)
    }

    pub fn in_memory() -> Self {
        Self::new(Arc::new(InMemoryHitlStore::new()))
    }
}

impl Deref for HitlStore {
    type Target = dyn StoreHitl + Send + Sync + 'static;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}
