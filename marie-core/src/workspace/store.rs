use std::sync::Arc;

use async_trait::async_trait;
use tokio::select;
use tokio::sync::{mpsc, oneshot};

use crate::workspace::{Workspace, WorkspaceId};

#[cfg(feature = "catalog")]
use crate::store::PgStore;
#[cfg(feature = "catalog")]
use chrono::{DateTime, Utc};
#[cfg(feature = "catalog")]
use sqlx::Row as _;
#[cfg(feature = "catalog")]
use sqlx::postgres::PgRow;
#[cfg(feature = "catalog")]
use sqlx::types::Json;

enum Command {
    GetWorkspace(WorkspaceId, oneshot::Sender<Result<Option<Workspace>, anyhow::Error>>),
    ListWorkspaces(oneshot::Sender<Result<Vec<Workspace>, anyhow::Error>>),
    InsertWorkspace(Workspace, oneshot::Sender<Result<(), anyhow::Error>>),
    ReplaceWorkspace(Workspace, oneshot::Sender<Result<(), anyhow::Error>>),
    DeleteWorkspace(WorkspaceId, oneshot::Sender<Result<(), anyhow::Error>>),
    Shutdown,
}

pub struct WorkspaceStoreActor;

impl WorkspaceStoreActor {
    pub fn create<Store>(store: Store) -> WorkspaceStoreClient where Store: WorkspaceStore + 'static {
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<Command>();

        let stor = store.clone();

        tokio::spawn(async move {
            let store = stor;
            use Command::*;
            loop {
                select! {
                    Some(cmd) = cmd_rx.recv() => {
                        match cmd {
                            GetWorkspace(id, to) => {
                                to.send(store.clone().get(id).await);
                            },
                            ListWorkspaces(to) => {
                                to.send(store.clone().list().await);
                            }
                            InsertWorkspace(workspace, to) => {
                                to.send(store.clone().insert(workspace).await);
                            },
                            ReplaceWorkspace(workspace, to) => {
                                to.send(store.clone().replace(workspace).await);
                            },
                            DeleteWorkspace(workspace_id, to) => {
                                to.send(store.clone().delete(workspace_id).await);
                            },
                            Shutdown => break
                        }
                    }
                }
            }
        });

        WorkspaceStoreClient(cmd_tx.clone(), Arc::new(Handler(cmd_tx)))
    }
}

struct Handler(mpsc::UnboundedSender<Command>);

impl Drop for Handler {
    fn drop(&mut self) {
        self.0.send(Command::Shutdown);
    }
}

/// Client du stockage de workspace
#[derive(Clone)]
pub struct WorkspaceStoreClient(mpsc::UnboundedSender<Command>, Arc<Handler>);

#[async_trait]
impl WorkspaceStore for WorkspaceStoreClient {
    async fn get(self, id: WorkspaceId) -> anyhow::Result<Option<Workspace>> {
        let (tx, rx) = oneshot::channel();
        self.0.send(Command::GetWorkspace(id, tx))?;
        rx.await?
    }

    async fn insert(mut self, workspace: Workspace) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.0.send(Command::InsertWorkspace(workspace, tx));
        rx.await?
    }

    async fn replace(mut self, workspace: Workspace) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.0.send(Command::ReplaceWorkspace(workspace, tx));
        rx.await?
    }

    async fn delete(mut self, id: WorkspaceId) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.0.send(Command::DeleteWorkspace(id, tx));
        rx.await?
    }

    async fn list(self) -> anyhow::Result<Vec<Workspace>> {
        let (tx, rx) = oneshot::channel();
        self.0.send(Command::ListWorkspaces(tx));
        rx.await?
    }
}

/// Stockage CRUD d'un [`Workspace`] complet — même modèle que
/// `session::store::SessionStore`, y compris le split volontaire
/// [`Self::insert`]/[`Self::replace`] (pas un simple upsert) : il porte la
/// sémantique de `workspace::rpc::InsertWorkspace`/mutations ("crée" contre
/// "remplace l'état complet d'un workspace *existant*"), et c'est ce qui
/// permet à l'implémentation Postgres de ne poser [`Workspace::created_at`]
/// qu'à la création et de ne jamais y retoucher lors d'un remplacement —
/// voir la doc de ces deux champs.
#[async_trait]
pub trait WorkspaceStore: Send + Sync + Clone {
    async fn get(self, id: WorkspaceId) -> anyhow::Result<Option<Workspace>>;
    async fn insert(mut self, workspace: Workspace) -> anyhow::Result<()>;
    async fn replace(mut self, workspace: Workspace) -> anyhow::Result<()>;
    async fn delete(mut self, id: WorkspaceId) -> anyhow::Result<()>;
    async fn list(self) -> anyhow::Result<Vec<Workspace>>;
}

/// Reconstitue un [`Workspace`] depuis une ligne de la table `workspace`
/// (voir `migrations/0007_workspace.sql`) — symétrique de l'insertion dans
/// [`PgStore::insert`]/[`PgStore::replace`]. Chaque collection a sa propre
/// colonne JSONB plutôt qu'un blob unique : contrairement à l'ancien contenu
/// de workspace (document CRDT `yrs`), ce `Workspace`-ci est un
/// enregistrement classique remplacé en bloc à chaque mutation, donc
/// décomposable colonne à colonne comme `session`/`model`/`tool`.
#[cfg(feature = "catalog")]
fn decode_row(row: PgRow) -> anyhow::Result<Workspace> {
    Ok(Workspace {
        id: row.try_get::<String, _>("id")?.parse()?,
        sessions: row.try_get::<Json<Vec<crate::session::SessionId>>, _>("sessions")?.0,
        vars: row.try_get::<Json<std::collections::HashMap<String, serde_json::Value>>, _>("vars")?.0,
        created_at: row.try_get::<DateTime<Utc>, _>("created_at")?,
        last_updated_at: row.try_get::<DateTime<Utc>, _>("last_updated_at")?,
    })
}

#[cfg(feature = "catalog")]
#[async_trait]
impl WorkspaceStore for PgStore {
    async fn list(self) -> anyhow::Result<Vec<Workspace>> {
        let rows = sqlx::query(
            "SELECT id, sessions, vars, created_at, last_updated_at \
             FROM workspace",
        )
        .fetch_all(self.pool())
        .await?;

        rows.into_iter().map(decode_row).collect()
    }

    async fn get(self, id: WorkspaceId) -> anyhow::Result<Option<Workspace>> {
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

    async fn insert(mut self, workspace: Workspace) -> anyhow::Result<()> {
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

    async fn replace(mut self, workspace: Workspace) -> anyhow::Result<()> {
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

    async fn delete(mut self, id: WorkspaceId) -> anyhow::Result<()> {
        let id = id.to_string();
        sqlx::query("DELETE FROM workspace WHERE id = $1").bind(&id).execute(self.pool()).await?;
        Ok(())
    }
}
