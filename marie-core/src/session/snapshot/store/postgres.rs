use async_trait::async_trait;
use sqlx::Row as _;
use sqlx::types::Json;

use crate::session::SessionId;
use crate::session::frames::FrameId;
use crate::session::snapshot::Snapshot;
use crate::store::PgStore;

use super::StoreSessionSnapshot;

/// Implémentation PostgreSQL de [`StoreSessionSnapshot`], contre
/// `marie_session_snapshots` (voir `migrations/0016_session_snapshot.sql`)
/// par sa clé primaire composite `(session_id, frame_id, superstep)` :
/// plusieurs clichés coexistent pour un même frame (un par `superstep`), donc
/// c'est ce triplet — pas `(session_id, frame_id)` — qui identifie un
/// cliché. `superstep` (`u32` côté domaine) est lié en `i64` : Postgres n'a
/// pas d'entier non signé (voir `sqlx::postgres::types`).
#[async_trait]
impl StoreSessionSnapshot for PgStore {
    async fn latest_snapshot(&self, id: &SessionId, frame_id: &FrameId) -> crate::Result<Snapshot> {
        let row = sqlx::query(
            "SELECT data FROM marie_session_snapshots \
             WHERE session_id = $1 AND frame_id = $2 \
             ORDER BY superstep DESC LIMIT 1",
        )
        .bind(id.to_string())
        .bind(frame_id.to_string())
        .fetch_one(self.pool())
        .await?;

        Ok(row.try_get::<Json<Snapshot>, _>("data")?.0)
    }

    async fn snapshot_at(&self, id: &SessionId, frame_id: &FrameId, superstep: u32) -> crate::Result<Snapshot> {
        let row = sqlx::query(
            "SELECT data FROM marie_session_snapshots \
             WHERE session_id = $1 AND frame_id = $2 AND superstep = $3",
        )
        .bind(id.to_string())
        .bind(frame_id.to_string())
        .bind(superstep as i64)
        .fetch_one(self.pool())
        .await?;

        Ok(row.try_get::<Json<Snapshot>, _>("data")?.0)
    }

    async fn upsert_snapshot(&self, snapshot: Snapshot) -> crate::Result<()> {
        sqlx::query(
            "INSERT INTO marie_session_snapshots (session_id, frame_id, superstep, data, created_at) \
             VALUES ($1, $2, $3, $4, NOW()) \
             ON CONFLICT (session_id, frame_id, superstep) DO UPDATE SET data = EXCLUDED.data",
        )
        .bind(snapshot.session_id.to_string())
        .bind(snapshot.frame_id.to_string())
        .bind(snapshot.superstep as i64)
        .bind(Json(&snapshot))
        .execute(self.pool())
        .await?;

        Ok(())
    }
}
