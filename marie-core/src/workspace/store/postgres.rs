#[cfg(feature = "catalog")]
use crate::store::PgStore;
use crate::workspace::{Workspace, WorkspaceId};
use crate::workspace::store::WorkspaceStorable;
use async_trait::async_trait;
#[cfg(feature = "catalog")]
use chrono::{DateTime, Utc};
#[cfg(feature = "catalog")]
use sqlx::Row as _;
#[cfg(feature = "catalog")]
use sqlx::postgres::PgRow;
#[cfg(feature = "catalog")]
use sqlx::types::Json;

#[async_trait]
impl WorkspaceStorable for PgStore {
    async fn list(&self) -> crate::Result<Vec<Workspace>> {
        let rows = sqlx::query(
            "SELECT id, sessions, vars, created_at, last_updated_at \
             FROM workspace",
        )
        .fetch_all(self.pool())
        .await?;

        rows.into_iter().map(decode_row).collect()
    }

    async fn get(&self, id: WorkspaceId) -> crate::Result<Option<Workspace>> {
        let id = id.to_string();
        let row = sqlx::query(
            "SELECT id, sessions, vars, created_at, last_updated_at \
             FROM workspace WHERE id = $1",
        )
        .bind(&id)
        .fetch_optional(self.pool())
        .await?;

        row.map(decode_row).transpose()
    }

    async fn insert(&self, workspace: Workspace) -> crate::Result<()> {
        let id = workspace.id.to_string();

        sqlx::query(
            "INSERT INTO workspace (id, sessions, vars, created_at, last_updated_at) \
             VALUES ($1, $2, $3, NOW(), NOW())",
        )
        .bind(&id)
        .bind(Json(&workspace.sessions))
        .bind(Json(&workspace.vars))
        .execute(self.pool())
        .await?;
        Ok(())
    }

    async fn replace(&self, workspace: Workspace) -> crate::Result<()> {
        let id = workspace.id.to_string();

        sqlx::query(
            "UPDATE workspace SET \
                sessions = $2, vars = $3, last_updated_at = NOW() \
             WHERE id = $1",
        )
        .bind(&id)
        .bind(Json(&workspace.sessions))
        .bind(Json(&workspace.vars))
        .execute(self.pool())
        .await?;
        Ok(())
    }

    async fn delete(&self, id: WorkspaceId) -> crate::Result<()> {
        let id = id.to_string();
        sqlx::query("DELETE FROM workspace WHERE id = $1").bind(&id).execute(self.pool()).await?;
        Ok(())
    }
}

/// Reconstitue un [`Workspace`] depuis une ligne de la table `workspace`
/// (voir `migrations/0007_workspace.sql`) — symétrique de l'insertion dans
/// [`PgStore::insert`]/[`PgStore::replace`]. Chaque collection a sa propre
/// colonne JSONB plutôt qu'un blob unique : contrairement à l'ancien contenu
/// de workspace (document CRDT `yrs`), ce `Workspace`-ci est un
/// enregistrement classique remplacé en bloc à chaque mutation, donc
/// décomposable colonne à colonne comme `session`/`model`/`tool`.
fn decode_row(row: PgRow) -> crate::Result<Workspace> {
    Ok(Workspace {
        id: row.try_get::<String, _>("id")?.parse()?,
        sessions: row.try_get::<Json<Vec<crate::session::SessionId>>, _>("sessions")?.0,
        vars: row.try_get::<Json<std::collections::HashMap<String, serde_json::Value>>, _>("vars")?.0,
        created_at: row.try_get::<DateTime<Utc>, _>("created_at")?,
        last_updated_at: row.try_get::<DateTime<Utc>, _>("last_updated_at")?,
    })
}