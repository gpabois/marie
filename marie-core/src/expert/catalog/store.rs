use std::sync::Arc;

use async_trait::async_trait;
use sqlx::Row as _;
use sqlx::postgres::PgRow;
use sqlx::types::Json;
use tokio::select;
use tokio::sync::{mpsc, oneshot};

use crate::{
    expert::{Expert, ExpertId},
    model::ModelId,
    store::PgStore,
    tools::ToolId,
};

/// Reconstitue un [`Expert`] depuis une ligne de la table `expert` (voir
/// `migrations/0003_expert.sql`) — symétrique de l'insertion dans
/// [`PgStore::insert`]/[`PgStore::replace`].
fn decode_row(row: &PgRow) -> anyhow::Result<Expert> {
    Ok(Expert {
        id: ExpertId::new(row.try_get::<String, _>("id")?),
        prompt: row.try_get("prompt")?,
        model_id: ModelId::new(row.try_get::<String, _>("model_id")?),
        allowed_tools: row.try_get::<Json<Vec<ToolId>>, _>("allowed_tools")?.0,
    })
}

/// Stockage CRUD local du catalogue d'experts (voir `expert::catalog::store`),
/// sur le même principe que [`crate::session::store::SessionStore`] (voir sa
/// doc pour la justification du `self` par valeur + `Clone` plutôt que
/// `&self`, et du découpage `insert`/`replace`).
#[async_trait]
pub trait ExpertStore: Send + Sync + Clone {
    async fn get(self, id: ExpertId) -> anyhow::Result<Option<Expert>>;
    async fn insert(self, value: Expert) -> anyhow::Result<()>;
    async fn replace(self, value: Expert) -> anyhow::Result<()>;
    async fn delete(self, id: ExpertId) -> anyhow::Result<()>;
    /// Toutes les entrées actuellement stockées.
    async fn list(self) -> anyhow::Result<Vec<Expert>>;
}

#[async_trait]
impl ExpertStore for PgStore {
    async fn get(self, id: ExpertId) -> anyhow::Result<Option<Expert>> {
        let id = id.to_string();
        let row = sqlx::query("SELECT id, prompt, model_id, allowed_tools FROM expert WHERE id = $1")
            .bind(&id)
            .fetch_optional(self.pool())
            .await?;
        row.as_ref().map(decode_row).transpose()
    }

    async fn insert(self, value: Expert) -> anyhow::Result<()> {
        let id = value.id.to_string();
        let model_id = value.model_id.to_string();

        sqlx::query("INSERT INTO expert (id, prompt, model_id, allowed_tools) VALUES ($1, $2, $3, $4)")
            .bind(&id)
            .bind(&value.prompt)
            .bind(&model_id)
            .bind(Json(&value.allowed_tools))
            .execute(self.pool())
            .await?;
        Ok(())
    }

    async fn replace(self, value: Expert) -> anyhow::Result<()> {
        let id = value.id.to_string();
        let model_id = value.model_id.to_string();

        sqlx::query("UPDATE expert SET prompt = $2, model_id = $3, allowed_tools = $4 WHERE id = $1")
            .bind(&id)
            .bind(&value.prompt)
            .bind(&model_id)
            .bind(Json(&value.allowed_tools))
            .execute(self.pool())
            .await?;
        Ok(())
    }

    async fn delete(self, id: ExpertId) -> anyhow::Result<()> {
        let id = id.to_string();
        sqlx::query("DELETE FROM expert WHERE id = $1").bind(&id).execute(self.pool()).await?;
        Ok(())
    }

    async fn list(self) -> anyhow::Result<Vec<Expert>> {
        let rows = sqlx::query("SELECT id, prompt, model_id, allowed_tools FROM expert").fetch_all(self.pool()).await?;
        rows.iter().map(decode_row).collect()
    }
}

/// Commandes traitées en série par [`ExpertStoreActor`] — voir la doc de
/// [`crate::session::store`] (`Command`) pour la raison de cette indirection
/// par acteur plutôt qu'un accès direct au store depuis chaque appelant.
enum Command {
    Get(ExpertId, oneshot::Sender<anyhow::Result<Option<Expert>>>),
    List(oneshot::Sender<anyhow::Result<Vec<Expert>>>),
    Insert(Expert, oneshot::Sender<anyhow::Result<()>>),
    Replace(Expert, oneshot::Sender<anyhow::Result<()>>),
    Delete(ExpertId, oneshot::Sender<anyhow::Result<()>>),
    Shutdown,
}

pub struct ExpertStoreActor;

impl ExpertStoreActor {
    pub fn create<Store>(store: Store) -> ExpertStoreClient
    where
        Store: ExpertStore + 'static,
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

        ExpertStoreClient(cmd_tx.clone(), Arc::new(Handler(cmd_tx)))
    }
}

struct Handler(mpsc::UnboundedSender<Command>);

impl Drop for Handler {
    fn drop(&mut self) {
        let _ = self.0.send(Command::Shutdown);
    }
}

/// Client du stockage d'experts — cheap à cloner (canal + `Arc`), ferme
/// l'acteur ([`Command::Shutdown`]) quand le dernier exemplaire est droppé.
#[derive(Clone)]
pub struct ExpertStoreClient(mpsc::UnboundedSender<Command>, Arc<Handler>);

#[async_trait]
impl ExpertStore for ExpertStoreClient {
    async fn get(self, id: ExpertId) -> anyhow::Result<Option<Expert>> {
        let (tx, rx) = oneshot::channel();
        self.0.send(Command::Get(id, tx))?;
        rx.await?
    }

    async fn insert(self, value: Expert) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.0.send(Command::Insert(value, tx))?;
        rx.await?
    }

    async fn replace(self, value: Expert) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.0.send(Command::Replace(value, tx))?;
        rx.await?
    }

    async fn delete(self, id: ExpertId) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.0.send(Command::Delete(id, tx))?;
        rx.await?
    }

    async fn list(self) -> anyhow::Result<Vec<Expert>> {
        let (tx, rx) = oneshot::channel();
        self.0.send(Command::List(tx))?;
        rx.await?
    }
}
