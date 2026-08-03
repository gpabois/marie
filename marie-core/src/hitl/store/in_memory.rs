use std::collections::HashMap;

use async_trait::async_trait;
use parking_lot::Mutex;

use crate::hitl::dto::PendingHitl;
use crate::hitl::{Answer, Hitl, HitlFrame, HitlId};
use crate::session::SessionId;
use crate::session::store::InMemorySessionStore;

use super::StoreHitl;

/// Erreur renvoyée par [`InMemoryHitlStore`] — seul cas où [`StoreHitl`] peut
/// échouer sans backend externe : une absence, jamais une panne de
/// connexion/désérialisation comme côté [`crate::store::PgStore`].
#[derive(Debug, thiserror::Error)]
pub enum InMemoryHitlStoreError {
    #[error("hitl {1} introuvable pour la session {0}")]
    HitlNotFound(SessionId, HitlId),
}

/// Implémentation en mémoire de [`StoreHitl`], autonome — contrairement à
/// [`InMemorySessionStore`] (qui délègue à ce type, voir sa doc), elle ne
/// connaît que les requêtes hitl, pas le reste d'une session (pas de
/// `Session`/`FrameNode`/`SessionLog` à consulter).
#[derive(Default)]
pub struct InMemoryHitlStore {
    pub(crate) hitls: Mutex<HashMap<(SessionId, HitlId), HitlFrame>>,
}

impl InMemoryHitlStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl StoreHitl for InMemoryHitlStore {
    async fn get_hitl_frame(&self, id: &SessionId, hitl_id: &HitlId) -> crate::Result<HitlFrame> {
        Ok(self.hitls.lock().get(&(*id, *hitl_id)).cloned().ok_or(InMemoryHitlStoreError::HitlNotFound(*id, *hitl_id))?)
    }

    async fn get_hitl(&self, id: &SessionId, hitl_id: &HitlId) -> crate::Result<Hitl> {
        Ok(self.get_hitl_frame(id, hitl_id).await?.hitl)
    }

    async fn list_unanswered_hitls_frames(&self, id: SessionId) -> crate::Result<Vec<HitlFrame>> {
        Ok(self.hitls.lock().values().filter(|hitl| hitl.session_id == id && hitl.answer.is_none()).cloned().collect())
    }

    async fn upsert_hitl_frame(&self, hitl: HitlFrame) -> crate::Result<()> {
        self.hitls.lock().insert((hitl.session_id, hitl.id), hitl);
        Ok(())
    }

    async fn write_hitl_response(&self, id: &SessionId, hitl_id: &HitlId, answers: HashMap<String, Answer>) -> crate::Result<()> {
        let mut hitl = self.get_hitl_frame(id, hitl_id).await?;
        hitl.answer = Some(answers);
        self.upsert_hitl_frame(hitl).await
    }

    async fn list_pending_hitls(&self, id: SessionId) -> crate::Result<Vec<PendingHitl>> {
        Ok(self
            .list_unanswered_hitls_frames(id)
            .await?
            .into_iter()
            .map(|hitl| PendingHitl { session_id: hitl.session_id, id: hitl.id, hitl: hitl.hitl })
            .collect())
    }
}

/// Implémentation en mémoire de [`StoreHitl`] pour [`InMemorySessionStore`] —
/// pure délégation à [`InMemoryHitlStore`] (voir sa doc pour la logique
/// réelle) : le champ `hitls` d'[`InMemorySessionStore`] en est une instance,
/// pas une `HashMap` brute, pour ne pas dupliquer la logique entre les deux.
#[async_trait]
impl StoreHitl for InMemorySessionStore {
    async fn get_hitl_frame(&self, id: &SessionId, hitl_id: &HitlId) -> crate::Result<HitlFrame> {
        self.hitls.get_hitl_frame(id, hitl_id).await
    }

    async fn get_hitl(&self, id: &SessionId, hitl_id: &HitlId) -> crate::Result<Hitl> {
        self.hitls.get_hitl(id, hitl_id).await
    }

    async fn list_unanswered_hitls_frames(&self, id: SessionId) -> crate::Result<Vec<HitlFrame>> {
        self.hitls.list_unanswered_hitls_frames(id).await
    }

    async fn upsert_hitl_frame(&self, hitl: HitlFrame) -> crate::Result<()> {
        self.hitls.upsert_hitl_frame(hitl).await
    }

    async fn write_hitl_response(&self, id: &SessionId, hitl_id: &HitlId, answers: HashMap<String, Answer>) -> crate::Result<()> {
        self.hitls.write_hitl_response(id, hitl_id, answers).await
    }

    async fn list_pending_hitls(&self, id: SessionId) -> crate::Result<Vec<PendingHitl>> {
        self.hitls.list_pending_hitls(id).await
    }
}
