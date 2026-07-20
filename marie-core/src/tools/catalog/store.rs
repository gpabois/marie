use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use sqlx::Row as _;
use sqlx::postgres::PgRow;
use sqlx::types::Json;
use tokio::select;
use tokio::sync::{mpsc, oneshot};

use crate::{
    store::PgStore,
    tools::{Tool, ToolId},
};

/// Reconstitue un [`Tool`] depuis une ligne de la table `tool` (voir
/// `migrations/0005_tool.sql`) — symétrique de l'insertion dans
/// [`PgStore::insert`]/[`PgStore::replace`].
fn decode_row(row: &PgRow) -> anyhow::Result<Tool> {
    Ok(Tool {
        name: ToolId::from(row.try_get::<String, _>("name")?),
        description: row.try_get("description")?,
        parameters_schema: row.try_get::<Json<Value>, _>("parameters_schema")?.0,
    })
}

/// Stockage CRUD local du catalogue de tools (voir `tools::catalog::store`),
/// sur le même principe que [`crate::session::store::SessionStore`] (voir sa
/// doc pour la justification du `self` par valeur + `Clone` plutôt que
/// `&self`, et du découpage `insert`/`replace`). `name` (voir [`ToolId`]) sert
/// à la fois de clé primaire et d'identifiant : contrairement à
/// [`crate::expert::Expert`], un [`Tool`] ne porte pas de champ `id` distinct.
#[async_trait]
pub trait ToolStore: Send + Sync + Clone {
    async fn get(self, id: ToolId) -> anyhow::Result<Option<Tool>>;
    async fn insert(self, value: Tool) -> anyhow::Result<()>;
    async fn replace(self, value: Tool) -> anyhow::Result<()>;
    async fn delete(self, id: ToolId) -> anyhow::Result<()>;
    /// Toutes les entrées actuellement stockées.
    async fn list(self) -> anyhow::Result<Vec<Tool>>;
}

#[async_trait]
impl ToolStore for PgStore {
    async fn get(self, id: ToolId) -> anyhow::Result<Option<Tool>> {
        let id = id.to_string();
        let row = sqlx::query("SELECT name, description, parameters_schema FROM tool WHERE name = $1")
            .bind(&id)
            .fetch_optional(self.pool())
            .await?;
        row.as_ref().map(decode_row).transpose()
    }

    async fn insert(self, value: Tool) -> anyhow::Result<()> {
        let name = value.name.to_string();

        sqlx::query("INSERT INTO tool (name, description, parameters_schema) VALUES ($1, $2, $3)")
            .bind(&name)
            .bind(&value.description)
            .bind(Json(&value.parameters_schema))
            .execute(self.pool())
            .await?;
        Ok(())
    }

    async fn replace(self, value: Tool) -> anyhow::Result<()> {
        let name = value.name.to_string();

        sqlx::query("UPDATE tool SET description = $2, parameters_schema = $3 WHERE name = $1")
            .bind(&name)
            .bind(&value.description)
            .bind(Json(&value.parameters_schema))
            .execute(self.pool())
            .await?;
        Ok(())
    }

    async fn delete(self, id: ToolId) -> anyhow::Result<()> {
        let id = id.to_string();
        sqlx::query("DELETE FROM tool WHERE name = $1").bind(&id).execute(self.pool()).await?;
        Ok(())
    }

    async fn list(self) -> anyhow::Result<Vec<Tool>> {
        let rows = sqlx::query("SELECT name, description, parameters_schema FROM tool").fetch_all(self.pool()).await?;
        rows.iter().map(decode_row).collect()
    }
}

/// Commandes traitées en série par [`ToolStoreActor`] — voir la doc de
/// [`crate::session::store`] (`Command`) pour la raison de cette indirection
/// par acteur plutôt qu'un accès direct au store depuis chaque appelant.
enum Command {
    Get(ToolId, oneshot::Sender<anyhow::Result<Option<Tool>>>),
    List(oneshot::Sender<anyhow::Result<Vec<Tool>>>),
    Insert(Tool, oneshot::Sender<anyhow::Result<()>>),
    Replace(Tool, oneshot::Sender<anyhow::Result<()>>),
    Delete(ToolId, oneshot::Sender<anyhow::Result<()>>),
    Shutdown,
}

pub struct ToolStoreActor;

impl ToolStoreActor {
    pub fn create<Store>(store: Store) -> ToolStoreClient
    where
        Store: ToolStore + 'static,
    {
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<Command>();

        tokio::spawn(async move {
            use Command::*;
            loop {
                select! {
                    Some(cmd) = cmd_rx.recv() => {
                        match cmd {
                            Get(id, to) => {
                                let _ = to.send(store.clone().get(id).await);
                            }
                            List(to) => {
                                let _ = to.send(store.clone().list().await);
                            }
                            Insert(value, to) => {
                                let _ = to.send(store.clone().insert(value).await);
                            }
                            Replace(value, to) => {
                                let _ = to.send(store.clone().replace(value).await);
                            }
                            Delete(id, to) => {
                                let _ = to.send(store.clone().delete(id).await);
                            }
                            Shutdown => break,
                        }
                    }
                }
            }
        });

        ToolStoreClient(cmd_tx.clone(), Arc::new(Handler(cmd_tx)))
    }
}

struct Handler(mpsc::UnboundedSender<Command>);

impl Drop for Handler {
    fn drop(&mut self) {
        let _ = self.0.send(Command::Shutdown);
    }
}

/// Client du stockage de tools — cheap à cloner (canal + `Arc`), ferme
/// l'acteur ([`Command::Shutdown`]) quand le dernier exemplaire est droppé.
#[derive(Clone)]
pub struct ToolStoreClient(mpsc::UnboundedSender<Command>, Arc<Handler>);

#[async_trait]
impl ToolStore for ToolStoreClient {
    async fn get(self, id: ToolId) -> anyhow::Result<Option<Tool>> {
        let (tx, rx) = oneshot::channel();
        self.0.send(Command::Get(id, tx))?;
        rx.await?
    }

    async fn insert(self, value: Tool) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.0.send(Command::Insert(value, tx))?;
        rx.await?
    }

    async fn replace(self, value: Tool) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.0.send(Command::Replace(value, tx))?;
        rx.await?
    }

    async fn delete(self, id: ToolId) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.0.send(Command::Delete(id, tx))?;
        rx.await?
    }

    async fn list(self) -> anyhow::Result<Vec<Tool>> {
        let (tx, rx) = oneshot::channel();
        self.0.send(Command::List(tx))?;
        rx.await?
    }
}
