use std::collections::HashMap;

use async_trait::async_trait;
use sqlx::Row as _;
use sqlx::types::Json;

use crate::hitl::dto::PendingHitl;
use crate::hitl::{Answer, Hitl, HitlFrame, HitlId};
use crate::session::SessionId;
use crate::store::PgStore;

use super::StoreHitl;

/// Implémentation PostgreSQL de [`StoreHitl`], contre `marie_session_hitls`
/// (voir `migrations/0019_session_hitl.sql`) par sa clé primaire composite
/// `(session_id, id)` — même logique de portée que `marie_session_frames.
/// (session_id, frame_id)` : un [`HitlId`] n'a de sens qu'au sein de sa
/// session. Contrairement aux autres tables, `ON CONFLICT` n'a ici qu'un
/// seul usage : créer le `HitlFrame` puis, plus tard, y écrire la réponse
/// une fois qu'elle arrive (`HitlFrame::answer`) — pas de variante
/// "remplacement complet" comme `upsert_session`/`upsert_frame`.
///
/// `list_unanswered_hitls_frames` filtre sur `frame -> 'answer' = '{}'::jsonb`
/// : `HitlFrame::answer` est une `HashMap<String, Answer>` (une entrée par
/// question du formulaire, voir [`crate::hitl::model::Question::key`]), pas
/// un `Option` — une requête sans réponse sérialise `answer` en objet JSON
/// vide plutôt qu'en `null`, d'où ce test d'égalité contre `'{}'::jsonb`
/// plutôt qu'un `IS NULL`/`= 'null'::jsonb`. `write_hitl_response` est la
/// seule à remplacer entièrement `answer` une fois la réponse arrivée —
/// cohérent avec le commentaire ci-dessus sur `ON CONFLICT`.
#[async_trait]
impl StoreHitl for PgStore {
    async fn get_hitl_frame(&self, id: &SessionId, hitl_id: &HitlId) -> crate::Result<HitlFrame> {
        let row = sqlx::query(
            "SELECT frame FROM marie_session_hitls WHERE session_id = $1 AND id = $2",
        )
        .bind(id.to_string())
        .bind(hitl_id.to_string())
        .fetch_one(self.pool())
        .await?;

        Ok(row.try_get::<Json<HitlFrame>, _>("frame")?.0)
    }

    async fn get_hitl(&self, id: &SessionId, hitl_id: &HitlId) -> crate::Result<Hitl> {
        Ok(self.get_hitl_frame(id, hitl_id).await?.hitl)
    }

    async fn list_unanswered_hitls_frames(&self, id: SessionId) -> crate::Result<Vec<HitlFrame>> {
        let rows = sqlx::query(
            "SELECT frame FROM marie_session_hitls \
             WHERE session_id = $1 AND frame -> 'answer' IS NULL",
        )
        .bind(id.to_string())
        .fetch_all(self.pool())
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| Ok::<_, sqlx::Error>(row.try_get::<Json<HitlFrame>, _>("frame")?.0))
            .collect::<Result<Vec<_>, _>>()?)
    }

    async fn upsert_hitl_frame(&self, hitl: HitlFrame) -> crate::Result<()> {
        sqlx::query(
            "INSERT INTO marie_session_hitls (session_id, id, frame, created_at, last_updated_at) \
             VALUES ($1, $2, $3, NOW(), NOW()) \
             ON CONFLICT (session_id, id) DO UPDATE SET frame = EXCLUDED.frame, last_updated_at = NOW()",
        )
        .bind(hitl.session_id.to_string())
        .bind(hitl.id.to_string())
        .bind(Json(&hitl))
        .execute(self.pool())
        .await?;

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
