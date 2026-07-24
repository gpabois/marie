use crate::agent::frame::AgentFrame;
use crate::graph::GraphFrame;
use crate::session::{Session, SessionId};
use crate::store::PgStore;

use super::SessionStore;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::Row as _;
use sqlx::postgres::PgRow;
use sqlx::types::Json;


#[async_trait]
impl SessionStore for PgStore {
    async fn list(self) -> anyhow::Result<Vec<Session>> {
        let rows = sqlx::query(
            "SELECT id, frames, graphs, orchestrations, hitls, logs, vars, created_at, last_updated_at \
             FROM session",
        )
        .fetch_all(self.pool())
        .await?;
    
        rows.into_iter().map(decode_row).collect()  
    }

    async fn get(self, id: SessionId) -> anyhow::Result<Option<Session>> {
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

    async fn insert(mut self, session: Session) -> anyhow::Result<()> {
        let id = session.id.to_string();

        sqlx::query(
            "INSERT INTO session (id, frames, graphs, orchestrations, hitls, logs, vars, created_at, last_updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())",
        )
        .bind(&id)
        .bind(Json(&session.frames))
        .bind(Json(&session.graphs))
        .bind(Json(&session.orchestrations))
        .bind(Json(&session.hitls))
        .bind(Json(&session.logs))
        .bind(Json(&session.vars))
        .execute(self.pool())
        .await?;
        Ok(())
    }

    async fn replace(mut self, session: Session) -> anyhow::Result<()> {
        let id = session.id.to_string();

        sqlx::query(
            "UPDATE session SET \
                frames = $2, graphs = $3, orchestrations = $4, hitls = $5, logs = $6, vars = $7, last_updated_at = NOW() \
             WHERE id = $1",
        )
        .bind(&id)
        .bind(Json(&session.frames))
        .bind(Json(&session.graphs))
        .bind(Json(&session.orchestrations))
        .bind(Json(&session.hitls))
        .bind(Json(&session.logs))
        .bind(Json(&session.vars))
        .execute(self.pool())
        .await?;
        Ok(())
    }

    async fn delete(mut self, id: SessionId) -> anyhow::Result<()> {
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
fn decode_row(row: PgRow) -> anyhow::Result<Session> {
    Ok(Session {
        id: row.try_get::<String, _>("id")?.parse()?,
        frames: row.try_get::<Json<std::collections::HashMap<AgentId, AgentFrame>>, _>("frames")?.0.into(),
        graphs: row.try_get::<Json<std::collections::HashMap<_, GraphFrame>>, _>("graphs")?.0,
        orchestrations: row.try_get::<Json<std::collections::HashMap<_, OrchestrationFrame>>, _>("orchestrations")?.0,
        hitls: row.try_get::<Json<std::collections::HashMap<_, HitlFrame>>, _>("hitls")?.0,
        logs: row.try_get::<Json<Vec<SessionLog>>, _>("logs")?.0,
        vars: row.try_get::<Json<std::collections::HashMap<String, serde_json::Value>>, _>("vars")?.0,
        created_at: row.try_get::<DateTime<Utc>, _>("created_at")?,
        last_updated_at: row.try_get::<DateTime<Utc>, _>("last_updated_at")?,
    })
}