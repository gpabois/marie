use crate::session::store::SessionStorable;
use crate::session::{Session, SessionId, SessionLog};
use crate::state::State;
use crate::store::PgStore;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::Row as _;
use sqlx::postgres::PgRow;
use sqlx::types::Json;


#[async_trait]
impl SessionStorable for PgStore {
    async fn list(&self) -> crate::Result<Vec<Session>> {
        let rows = sqlx::query(
            "SELECT id, frames, graphs, orchestrations, hitls, logs, vars, created_at, last_updated_at \
             FROM session",
        )
        .fetch_all(self.pool())
        .await?;
    
        rows.into_iter().map(decode_row).collect()  
    }

    async fn get(&self, id: SessionId) -> crate::Result<Option<Session>> {
        let id = id.to_string();
        let row = sqlx::query(
            "SELECT id, frames, graphs, orchestrations, hitls, logs, vars, created_at, last_updated_at \
             FROM session WHERE id = $1",
        )
        .bind(&id)
        .fetch_optional(self.pool())
        .await?;

        row.map(decode_row).transpose()
    }

    async fn insert(&self, session: Session) -> crate::Result<()> {
        let id = session.id.to_string();

        sqlx::query(
            "INSERT INTO marie_sessions (id, frames, graphs, orchestrations, hitls, logs, vars, created_at, last_updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())",
        )
        .bind(&id)
        .bind(Json(&session.logs))
        .execute(self.pool())
        .await?;
        Ok(())
    }

    async fn replace(&self, session: Session) -> crate::Result<()> {
        let id = session.id.to_string();

        sqlx::query(
            "UPDATE session SET \
                frames = $2, graphs = $3, orchestrations = $4, hitls = $5, logs = $6, vars = $7, last_updated_at = NOW() \
             WHERE id = $1",
        )
        .bind(&id)
        .bind(Json(&session.logs))
        .bind(Json(&session.state))
        .execute(self.pool())
        .await?;
        Ok(())
    }

    async fn delete(&self, id: SessionId) -> crate::Result<()> {
        let id = id.to_string();
        sqlx::query("DELETE FROM session WHERE id = $1").bind(&id).execute(self.pool()).await?;
        Ok(())
    }
}


/// Reconstitue une [`Session`] depuis une ligne de la table `session` (voir
/// `migrations/0001_session.sql`) — symétrique de l'insertion dans
/// [`PgStore::insert`]/[`PgStore::replace`]. Chaque collection de [`Session`]
/// a sa propre colonne JSONB plutôt qu'un blob unique : contrairement à
/// l'ancienne table `session` (contenu CRDT `yrs`, voir
/// `persistency::session`), cette `Session`-ci est un enregistrement
/// classique remplacé en bloc à chaque mutation, donc décomposable colonne à
/// colonne comme `expert`/`model`/`tool`.
fn decode_row(row: PgRow) -> crate::Result<Session> {
    Ok(Session {
        id: row.try_get::<String, _>("id")?.parse()?,
        logs: row.try_get::<Json<Vec<SessionLog>>, _>("logs")?.0,
        state: row.try_get::<Json<State>, _>("vars")?.0,
        created_at: row.try_get::<DateTime<Utc>, _>("created_at")?,
        last_updated_at: row.try_get::<DateTime<Utc>, _>("last_updated_at")?,
    })
}