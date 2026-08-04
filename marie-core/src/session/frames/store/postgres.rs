use async_trait::async_trait;
use sqlx::Row as _;
use sqlx::types::Json;

use crate::hitl::HitlId;
use crate::session::SessionId;
use crate::session::frames::{FrameKind, FrameId, FrameNode};
use crate::store::PgStore;

use super::StoreSessionFrame;

/// Implémentation PostgreSQL de [`StoreSessionFrame`], contre
/// `marie_session_frames` (voir `migrations/0015_session_frame.sql`) —
/// adressée par sa clé primaire composite `(session_id, frame_id)` : un
/// `FrameId` n'est unique qu'au sein de sa session, donc c'est la paire — pas
/// `frame_id` seul — que `ON CONFLICT` doit cibler pour que deux sessions
/// distinctes puissent réutiliser des `FrameId` sans collision (même logique
/// que `fs_alias.(scope, from_path)`, voir `vfs::alias::PostgresAliasCatalog`).
///
/// `get_root_frame_id` cherche la ligne dont `node->>'is_root'` vaut `true`
/// plutôt que `marie_sessions.root_frame` (voir [`FrameNode::is_root`]) :
/// ainsi ce trait ne dépend que de `marie_session_frames`, jamais de
/// `marie_sessions` — cohérent avec [`InMemorySessionFrameStore`], qui n'a
/// justement aucun accès à une donnée `Session`. `set_root_frame_id` est la
/// seule à toucher `is_root` : elle rabaisse toutes les autres lignes de la
/// session dans la même transaction avant de lever le drapeau sur
/// `frame_id` — `upsert_frame` se contente d'écrire `node` tel quel.
#[async_trait]
impl StoreSessionFrame for PgStore {
    async fn get_root_frame_id(&self, id: SessionId) -> crate::Result<FrameId> {
        let row = sqlx::query(
            "SELECT frame_id FROM marie_session_frames \
             WHERE session_id = $1 AND (node->>'is_root')::boolean IS TRUE",
        )
        .bind(id.to_string())
        .fetch_one(self.pool())
        .await?;

        Ok(row
            .try_get::<String, _>("frame_id")?
            .parse()
            .expect("le frame_id stocké dans marie_session_frames est toujours un FrameId valide"))
    }

    async fn set_root_frame_id(&self, session_id: SessionId, frame_id: FrameId) -> crate::Result<FrameId> {
        let mut tx = self.pool().begin().await?;

        sqlx::query(
            "UPDATE marie_session_frames \
             SET node = jsonb_set(node, '{is_root}', 'false'), last_updated_at = NOW() \
             WHERE session_id = $1 AND frame_id <> $2 AND (node->>'is_root')::boolean IS TRUE",
        )
        .bind(session_id.to_string())
        .bind(frame_id.to_string())
        .execute(&mut *tx)
        .await?;

        let result = sqlx::query(
            "UPDATE marie_session_frames \
             SET node = jsonb_set(node, '{is_root}', 'true'), last_updated_at = NOW() \
             WHERE session_id = $1 AND frame_id = $2",
        )
        .bind(session_id.to_string())
        .bind(frame_id.to_string())
        .execute(&mut *tx)
        .await?;

        if result.rows_affected() == 0 {
            crate::bail!("le frame {frame_id} n'existe pas pour la session {session_id}");
        }

        tx.commit().await?;

        Ok(frame_id)
    }

    /// Pose `data = FrameData::Hitl(hitl_id)` sur un frame déjà persisté —
    /// même idiome que [`Self::set_root_frame_id`] : `jsonb_set` sur le
    /// champ `data` de `node` plutôt qu'un `upsert_frame` complet, pour ne
    /// pas avoir à recharger/resérialiser le reste du [`FrameNode`].
    async fn bind_hitl_to_frame(&self, id: &SessionId, frame_id: &FrameId, hitl_id: &HitlId) -> crate::Result<()> {
        let result = sqlx::query(
            "UPDATE marie_session_frames \
             SET node = jsonb_set(node, '{data}', $3::jsonb), last_updated_at = NOW() \
             WHERE session_id = $1 AND frame_id = $2",
        )
        .bind(id.to_string())
        .bind(frame_id.to_string())
        .bind(Json(&FrameKind::Hitl(*hitl_id)))
        .execute(self.pool())
        .await?;

        if result.rows_affected() == 0 {
            crate::bail!("le frame {frame_id} n'existe pas pour la session {id}");
        }

        Ok(())
    }

    /// Cherche le frame dont `data` vaut `FrameData::Hitl(hitl_id)` — sens
    /// inverse de [`Self::bind_hitl_to_frame`], remplace la colonne
    /// `frame_id` autrefois portée par `marie_session_hitls` (voir
    /// `migrations/0007_session_hitl.sql`) : un seul sens de lookup à
    /// maintenir plutôt que de dupliquer la relation dans les deux tables.
    /// Égalité `jsonb` plutôt que `->>'kind'`/`->>'0'` : `node -> 'data'` et
    /// la valeur sérialisée de `FrameData::Hitl(hitl_id)` passent par le
    /// même `Serialize`, donc la comparaison reste correcte quelle que soit
    /// la représentation JSON exacte de la variante. `None` — via
    /// `fetch_optional` plutôt que `fetch_one` — si aucun frame n'est
    /// (encore) lié : ce n'est pas une erreur (voir la doc du trait).
    async fn get_frame_id_by_hitl_id(&self, id: &SessionId, hitl_id: HitlId) -> crate::Result<Option<FrameId>> {
        let row = sqlx::query(
            "SELECT frame_id FROM marie_session_frames \
             WHERE session_id = $1 AND node -> 'data' = $2::jsonb",
        )
        .bind(id.to_string())
        .bind(Json(&FrameKind::Hitl(hitl_id)))
        .fetch_optional(self.pool())
        .await?;

        Ok(row
            .map(|row| row.try_get::<String, _>("frame_id"))
            .transpose()?
            .map(|frame_id| frame_id.parse().expect("le frame_id stocké dans marie_session_frames est toujours un FrameId valide")))
    }

    async fn get_frame(&self, id: &SessionId, frame_id: &FrameId) -> crate::Result<FrameNode> {
        let row = sqlx::query(
            "SELECT node FROM marie_session_frames WHERE session_id = $1 AND frame_id = $2",
        )
        .bind(id.to_string())
        .bind(frame_id.to_string())
        .fetch_one(self.pool())
        .await?;

        Ok(row.try_get::<Json<FrameNode>, _>("node")?.0)
    }

    async fn upsert_frame(&self, node: FrameNode) -> crate::Result<FrameNode> {
        sqlx::query(
            "INSERT INTO marie_session_frames (session_id, frame_id, node, created_at, last_updated_at) \
             VALUES ($1, $2, $3, NOW(), NOW()) \
             ON CONFLICT (session_id, frame_id) DO UPDATE SET node = EXCLUDED.node, last_updated_at = NOW()",
        )
        .bind(node.session_id.to_string())
        .bind(node.id.to_string())
        .bind(Json(&node))
        .execute(self.pool())
        .await?;

        Ok(node)
    }

    async fn delete_frame(&self, id: &SessionId, frame_id: &FrameId) -> crate::Result<FrameNode> {
        let row = sqlx::query(
            "DELETE FROM marie_session_frames WHERE session_id = $1 AND frame_id = $2 \
             RETURNING node",
        )
        .bind(id.to_string())
        .bind(frame_id.to_string())
        .fetch_one(self.pool())
        .await?;

        Ok(row.try_get::<Json<FrameNode>, _>("node")?.0)
    }
}
