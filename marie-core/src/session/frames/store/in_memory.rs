use std::collections::HashMap;

use async_trait::async_trait;
use parking_lot::Mutex;

use crate::hitl::HitlId;
use crate::session::SessionId;
use crate::session::store::InMemorySessionStore;

use super::super::{FrameData, FrameId, FrameNode};
use super::StoreSessionFrame;

/// Erreur renvoyée par [`InMemorySessionFrameStore`] — seul cas où
/// [`StoreSessionFrame`] peut échouer sans backend externe : une absence,
/// jamais une panne de connexion/désérialisation comme côté [`crate::store::PgStore`].
#[derive(Debug, thiserror::Error)]
pub enum InMemorySessionFrameStoreError {
    #[error("frame {1} introuvable pour la session {0}")]
    FrameNotFound(SessionId, FrameId),
    #[error("la session {0} n'a pas encore de frame racine")]
    NoRootFrame(SessionId),
}

/// Implémentation en mémoire de [`StoreSessionFrame`], autonome —
/// contrairement à [`crate::session::store::InMemorySessionStore`] (qui
/// délègue à ce type, voir sa doc), elle ne connaît que l'arbre des frames,
/// pas le reste d'une session (pas de `Session::root_frame` à consulter).
/// C'est justement pour ça que [`Self::get_root_frame_id`] ne peut pas
/// s'appuyer sur `Session::root_frame` comme le fait [`crate::store::PgStore`]
/// avant sa bascule vers `is_root` : ce champ vit sur [`FrameNode`] lui-même
/// (voir sa doc), pas sur `Session`.
#[derive(Default)]
pub struct InMemorySessionFrameStore {
    pub(crate) frames: Mutex<HashMap<(SessionId, FrameId), FrameNode>>,
}

impl InMemorySessionFrameStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl StoreSessionFrame for InMemorySessionFrameStore {
    async fn get_root_frame_id(&self, id: SessionId) -> crate::Result<FrameId> {
        Ok(self
            .frames
            .lock()
            .values()
            .find(|frame| frame.session_id == id && frame.is_root)
            .map(|frame| frame.id)
            .ok_or(InMemorySessionFrameStoreError::NoRootFrame(id))?)
    }

    /// Comme [`crate::store::PgStore`]'s `impl StoreSessionFrame` : rabaisse
    /// `is_root` sur toutes les autres frames de la session avant de le lever
    /// sur `frame_id` — au plus une racine par session à la fois. Échoue si
    /// `frame_id` n'existe pas encore (voir la doc du trait).
    async fn set_root_frame_id(&self, session_id: SessionId, frame_id: FrameId) -> crate::Result<FrameId> {
        let mut frames = self.frames.lock();

        if !frames.contains_key(&(session_id, frame_id)) {
            return Err(InMemorySessionFrameStoreError::FrameNotFound(session_id, frame_id).into());
        }

        for (key, existing) in frames.iter_mut() {
            if key.0 == session_id {
                existing.is_root = key.1 == frame_id;
            }
        }

        Ok(frame_id)
    }

    async fn get_frame(&self, id: &SessionId, frame_id: &FrameId) -> crate::Result<FrameNode> {
        Ok(self.frames.lock().get(&(*id, *frame_id)).cloned().ok_or(InMemorySessionFrameStoreError::FrameNotFound(*id, *frame_id))?)
    }

    /// Pose `data = FrameData::Hitl(hitl_id)` sur un frame déjà persisté —
    /// même idiome que [`Self::set_root_frame_id`] : échoue si `frame_id`
    /// n'existe pas encore, contrairement à [`Self::upsert_frame`].
    async fn bind_hitl_to_frame(&self, id: &SessionId, frame_id: &FrameId, hitl_id: &HitlId) -> crate::Result<()> {
        let mut frames = self.frames.lock();

        let frame = frames
            .get_mut(&(*id, *frame_id))
            .ok_or(InMemorySessionFrameStoreError::FrameNotFound(*id, *frame_id))?;

        frame.data = FrameData::Hitl(*hitl_id);

        Ok(())
    }

    /// Voir la doc de [`crate::store::PgStore`]'s `impl StoreSessionFrame` :
    /// remplace la colonne `frame_id` autrefois portée par
    /// `marie_session_hitls`, en lisant plutôt le `FrameData::Hitl` du frame
    /// propriétaire — `None` si aucun frame n'est (encore) lié, ce n'est pas
    /// une erreur (voir la doc du trait).
    async fn get_frame_id_by_hitl_id(&self, id: &SessionId, hitl_id: HitlId) -> crate::Result<Option<FrameId>> {
        Ok(self
            .frames
            .lock()
            .values()
            .find(|frame| frame.session_id == *id && frame.data == FrameData::Hitl(hitl_id))
            .map(|frame| frame.id))
    }

    async fn upsert_frame(&self, node: FrameNode) -> crate::Result<FrameNode> {
        self.frames.lock().insert((node.session_id, node.id), node.clone());
        Ok(node)
    }

    async fn delete_frame(&self, id: &SessionId, frame_id: &FrameId) -> crate::Result<FrameNode> {
        Ok(self.frames.lock().remove(&(*id, *frame_id)).ok_or(InMemorySessionFrameStoreError::FrameNotFound(*id, *frame_id))?)
    }
}

/// Implémentation en mémoire de [`StoreSessionFrame`] pour
/// [`InMemorySessionStore`] — pure délégation à
/// [`InMemorySessionFrameStore`] (voir sa doc pour la logique réelle) : le
/// champ `frames` d'[`InMemorySessionStore`] en est une instance, pas une
/// `HashMap` brute, pour ne pas dupliquer la logique `is_root` entre les
/// deux.
#[async_trait]
impl StoreSessionFrame for InMemorySessionStore {
    async fn get_root_frame_id(&self, id: SessionId) -> crate::Result<FrameId> {
        self.frames.get_root_frame_id(id).await
    }

    async fn set_root_frame_id(&self, session_id: SessionId, frame_id: FrameId) -> crate::Result<FrameId> {
        self.frames.set_root_frame_id(session_id, frame_id).await
    }

    async fn get_frame(&self, id: &SessionId, frame_id: &FrameId) -> crate::Result<FrameNode> {
        self.frames.get_frame(id, frame_id).await
    }

    async fn bind_hitl_to_frame(&self, id: &SessionId, frame_id: &FrameId, hitl_id: &HitlId) -> crate::Result<()> {
        self.frames.bind_hitl_to_frame(id, frame_id, hitl_id).await
    }

    async fn get_frame_id_by_hitl_id(&self, id: &SessionId, hitl_id: HitlId) -> crate::Result<Option<FrameId>> {
        self.frames.get_frame_id_by_hitl_id(id, hitl_id).await
    }

    async fn upsert_frame(&self, node: FrameNode) -> crate::Result<FrameNode> {
        self.frames.upsert_frame(node).await
    }

    async fn delete_frame(&self, id: &SessionId, frame_id: &FrameId) -> crate::Result<FrameNode> {
        self.frames.delete_frame(id, frame_id).await
    }
}
