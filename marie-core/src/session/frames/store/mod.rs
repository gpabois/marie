pub mod in_memory;
#[cfg(feature = "catalog")]
pub mod postgres;

use std::ops::Deref;
use std::sync::Arc;

use async_trait::async_trait;

pub use in_memory::InMemorySessionFrameStore;

use crate::{hitl::HitlId, session::SessionId};

use super::{FrameId, FrameNode};

/// Sous-ensemble de [`crate::session::store::StoreSession`] dédié à l'arbre
/// des frames d'une session (`marie_session_frames`) — séparé du reste
/// (session/snapshots/hitls/logs) pour que des types qui n'ont besoin que de
/// lire/écrire des frames (ex. [`crate::session::frames::FrameTree`])
/// puissent dépendre de ce trait seul plutôt que de l'intégralité de
/// [`crate::session::store::StoreSession`].
#[async_trait]
pub trait StoreSessionFrame {
    async fn get_root_frame_id(&self, id: SessionId) -> crate::Result<FrameId>;
    /// Désigne `frame_id` comme racine de `session_id`, en rabaissant
    /// `is_root` (voir [`FrameNode::is_root`]) sur toute autre frame de la
    /// même session au passage — au plus une racine par session à la fois.
    /// Échoue si `frame_id` n'existe pas encore dans le store : contrairement
    /// à [`Self::upsert_frame`], cette méthode ne fait que basculer un
    /// drapeau sur une ligne déjà persistée.
    async fn set_root_frame_id(&self, session_id: SessionId, frame_id: FrameId) -> crate::Result<FrameId>;
    async fn get_frame(&self, id: &SessionId, frame_id: &FrameId) -> crate::Result<FrameNode>;
    /// Pose `data = FrameData::Hitl(hitl_id)` sur `frame_id`, déjà persisté
    /// — remplace la colonne `frame_id` autrefois portée par
    /// `marie_session_hitls` (voir `migrations/0007_session_hitl.sql`) : un
    /// seul sens de lookup à maintenir plutôt que de dupliquer la relation
    /// frame/hitl dans les deux tables. Échoue si `frame_id` n'existe pas
    /// encore, comme [`Self::set_root_frame_id`].
    async fn bind_hitl_to_frame(&self, id: &SessionId, frame_id: &FrameId, hitl_id: &HitlId) -> crate::Result<()>;
    /// L'id du frame lié à `hitl_id` via [`Self::bind_hitl_to_frame`] —
    /// `None`, pas une erreur, si aucun frame n'est (encore) lié.
    async fn get_frame_id_by_hitl_id(&self, id: &SessionId, hitl_id: HitlId) -> crate::Result<Option<FrameId>>;

    async fn upsert_frame(&self, node: FrameNode) -> crate::Result<FrameNode>;
    async fn delete_frame(&self, id: &SessionId, frame_id: &FrameId) -> crate::Result<FrameNode>;
}

/// Type opaque enveloppant l'implémentation concrète de [`StoreSessionFrame`]
/// — même rôle que [`crate::session::store::SessionStore`] vis-à-vis de
/// [`StoreSession`] : `Arc<dyn StoreSessionFrame + Send + Sync + 'static>`
/// plutôt qu'un paramètre générique, pour qu'un appelant qui n'a besoin que
/// de lire/écrire des frames (voir [`crate::session::frames::FrameTree`])
/// puisse dépendre de ce type seul, sans tirer l'intégralité de
/// [`crate::session::store::SessionStore`].
#[derive(Clone)]
pub struct SessionFrameStore(Arc<dyn StoreSessionFrame + Send + Sync + 'static>);

impl SessionFrameStore {
    pub fn new(store: Arc<dyn StoreSessionFrame + Send + Sync + 'static>) -> Self {
        Self(store)
    }

    pub fn in_memory() -> Self {
        Self::new(Arc::new(InMemorySessionFrameStore::new()))
    }
}

impl Deref for SessionFrameStore {
    type Target = dyn StoreSessionFrame + Send + Sync + 'static;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}