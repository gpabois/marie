use async_trait::async_trait;
use sqlx::Row as _;
use sqlx::postgres::PgRow;
use sqlx::types::Json;

use crate::session::SessionId;
use crate::session::logs::{SessionLog, SessionLogContent};
use crate::store::PgStore;

use super::{SearchLogQuery, StoreSessionLogs};

/// Implémentation PostgreSQL de [`StoreSessionLogs`], contre
/// `marie_session_logs` — `insert_log` fait l'upsert (`ON CONFLICT`) plutôt
/// qu'un simple `INSERT` puisqu'un même `SessionLogId` peut recevoir
/// plusieurs écritures successives (voir la doc du trait).
#[async_trait]
impl StoreSessionLogs for PgStore {
    async fn insert_log(&self, log: SessionLog) -> crate::Result<()> {
        sqlx::query(
            "INSERT INTO marie_session_logs (session_id, id, content, created_at, last_updated_at) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (session_id, id) DO UPDATE SET content = EXCLUDED.content, last_updated_at = EXCLUDED.last_updated_at",
        )
        .bind(log.session_id.to_string())
        .bind(log.id.to_string())
        .bind(Json(&log.content))
        .bind(log.created_at)
        .bind(log.last_updated_at)
        .execute(self.pool())
        .await?;

        Ok(())
    }

    async fn list_log(&self, session_id: SessionId) -> crate::Result<Vec<SessionLog>> {
        let rows = sqlx::query(
            "SELECT id, content, created_at, last_updated_at FROM marie_session_logs \
             WHERE session_id = $1 ORDER BY created_at",
        )
        .bind(session_id.to_string())
        .fetch_all(self.pool())
        .await?;

        Ok(rows.into_iter().map(|row| decode_log_row(session_id, row)).collect::<Result<Vec<_>, sqlx::Error>>()?)
    }

    async fn list_log_after(&self, session_id: SessionId, query: SearchLogQuery) -> crate::Result<Vec<SessionLog>> {
        let rows = sqlx::query(
            "SELECT id, content, created_at, last_updated_at FROM marie_session_logs \
             WHERE session_id = $1 \
             AND ($2::timestamptz IS NULL OR created_at > $2) \
             AND ($3::timestamptz IS NULL OR created_at < $3) \
             ORDER BY created_at",
        )
        .bind(session_id.to_string())
        .bind(query.after)
        .bind(query.before)
        .fetch_all(self.pool())
        .await?;

        Ok(rows.into_iter().map(|row| decode_log_row(session_id, row)).collect::<Result<Vec<_>, sqlx::Error>>()?)
    }
}

/// Reconstitue un [`SessionLog`] depuis une ligne de `marie_session_logs` —
/// `session_id` n'est pas stocké de nouveau côté colonnes lues (il est déjà
/// connu de l'appelant, voir [`StoreSessionLogs::list_log`]/
/// [`StoreSessionLogs::list_log_after`]).
fn decode_log_row(session_id: SessionId, row: PgRow) -> Result<SessionLog, sqlx::Error> {
    Ok(SessionLog {
        id: row
            .try_get::<String, _>("id")?
            .parse()
            .expect("l'id stocké dans marie_session_logs est toujours un SessionLogId valide"),
        session_id,
        content: row.try_get::<Json<SessionLogContent>, _>("content")?.0,
        created_at: row.try_get("created_at")?,
        last_updated_at: row.try_get("last_updated_at")?,
    })
}
