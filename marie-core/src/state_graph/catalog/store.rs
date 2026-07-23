use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::Row as _;
use sqlx::postgres::PgRow;
use sqlx::types::Json;
use tokio::select;
use tokio::sync::{mpsc, oneshot};

use crate::{
    state_graph::declaration::{StateGraphDeclaration, StateGraphId},
    store::PgStore,
};

/// Représentation persistée d'une entrée du catalogue : contrairement à
/// [`StateGraphDeclaration`], qui ne porte pas d'identifiant propre (voir sa
/// doc — l'id n'existe que comme clé du catalogue), cette enveloppe porte
/// `id` à côté de la déclaration pour que [`StateGraphStore::list`] puisse
/// reconstituer le catalogue complet à froid sans dépendre d'une clé de
/// stockage externe.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredStateGraph {
    pub id: StateGraphId,
    pub declaration: StateGraphDeclaration,
}

/// Reconstitue un [`StoredStateGraph`] depuis une ligne de la table
/// `state_graph` (voir `migrations/0004_state_graph.sql`) — symétrique de
/// l'insertion dans [`PgStore::insert`]/[`PgStore::replace`].
fn decode_row(row: &PgRow) -> anyhow::Result<StoredStateGraph> {
    Ok(StoredStateGraph {
        id: StateGraphId::new(row.try_get::<String, _>("id")?),
        declaration: StateGraphDeclaration {
            nodes: row.try_get::<Json<_>, _>("nodes")?.0,
            edges: row.try_get::<Json<_>, _>("edges")?.0,
            entry: row.try_get("entry")?,
        },
    })
}

/// Stockage CRUD local du catalogue de graphes d'états (voir
/// `state_graph::catalog::store`), sur le même principe que
/// [`crate::session::store::SessionStore`] (voir sa doc pour la justification
/// du `self` par valeur + `Clone` plutôt que `&self`, et du découpage
/// `insert`/`replace`).
#[async_trait]
pub trait StateGraphStore: Send + Sync + Clone {
    async fn get(self, id: StateGraphId) -> anyhow::Result<Option<StoredStateGraph>>;
    async fn insert(self, value: StoredStateGraph) -> anyhow::Result<()>;
    async fn replace(self, value: StoredStateGraph) -> anyhow::Result<()>;
    async fn delete(self, id: StateGraphId) -> anyhow::Result<()>;
    /// Toutes les entrées actuellement stockées.
    async fn list(self) -> anyhow::Result<Vec<StoredStateGraph>>;
}

#[async_trait]
impl StateGraphStore for PgStore {
    async fn get(self, id: StateGraphId) -> anyhow::Result<Option<StoredStateGraph>> {
        let id = id.to_string();
        let row = sqlx::query("SELECT id, entry, nodes, edges FROM state_graph WHERE id = $1")
            .bind(&id)
            .fetch_optional(self.pool())
            .await?;
        row.as_ref().map(decode_row).transpose()
    }

    async fn insert(self, value: StoredStateGraph) -> anyhow::Result<()> {
        let id = value.id.to_string();

        sqlx::query("INSERT INTO state_graph (id, entry, nodes, edges) VALUES ($1, $2, $3, $4)")
            .bind(&id)
            .bind(&value.declaration.entry)
            .bind(Json(&value.declaration.nodes))
            .bind(Json(&value.declaration.edges))
            .execute(self.pool())
            .await?;
        Ok(())
    }

    async fn replace(self, value: StoredStateGraph) -> anyhow::Result<()> {
        let id = value.id.to_string();

        sqlx::query("UPDATE state_graph SET entry = $2, nodes = $3, edges = $4 WHERE id = $1")
            .bind(&id)
            .bind(&value.declaration.entry)
            .bind(Json(&value.declaration.nodes))
            .bind(Json(&value.declaration.edges))
            .execute(self.pool())
            .await?;
        Ok(())
    }

    async fn delete(self, id: StateGraphId) -> anyhow::Result<()> {
        let id = id.to_string();
        sqlx::query("DELETE FROM state_graph WHERE id = $1").bind(&id).execute(self.pool()).await?;
        Ok(())
    }

    async fn list(self) -> anyhow::Result<Vec<StoredStateGraph>> {
        let rows = sqlx::query("SELECT id, entry, nodes, edges FROM state_graph").fetch_all(self.pool()).await?;
        rows.iter().map(decode_row).collect()
    }
}

/// Commandes traitées en série par [`StateGraphStoreActor`] — voir la doc de
/// [`crate::session::store`] (`Command`) pour la raison de cette indirection
/// par acteur plutôt qu'un accès direct au store depuis chaque appelant.
enum Command {
    Get(StateGraphId, oneshot::Sender<anyhow::Result<Option<StoredStateGraph>>>),
    List(oneshot::Sender<anyhow::Result<Vec<StoredStateGraph>>>),
    Insert(StoredStateGraph, oneshot::Sender<anyhow::Result<()>>),
    Replace(StoredStateGraph, oneshot::Sender<anyhow::Result<()>>),
    Delete(StateGraphId, oneshot::Sender<anyhow::Result<()>>),
    Shutdown,
}

pub struct StateGraphStoreActor;

impl StateGraphStoreActor {
    pub fn create<Store>(store: Store) -> StateGraphStoreClient
    where
        Store: StateGraphStore + 'static,
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

        StateGraphStoreClient(cmd_tx.clone(), Arc::new(Handler(cmd_tx)))
    }
}

struct Handler(mpsc::UnboundedSender<Command>);

impl Drop for Handler {
    fn drop(&mut self) {
        let _ = self.0.send(Command::Shutdown);
    }
}

/// Client du stockage de graphes d'états — cheap à cloner (canal + `Arc`),
/// ferme l'acteur ([`Command::Shutdown`]) quand le dernier exemplaire est
/// droppé.
#[derive(Clone)]
pub struct StateGraphStoreClient(mpsc::UnboundedSender<Command>, Arc<Handler>);

#[async_trait]
impl StateGraphStore for StateGraphStoreClient {
    async fn get(self, id: StateGraphId) -> anyhow::Result<Option<StoredStateGraph>> {
        let (tx, rx) = oneshot::channel();
        self.0.send(Command::Get(id, tx))?;
        rx.await?
    }

    async fn insert(self, value: StoredStateGraph) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.0.send(Command::Insert(value, tx))?;
        rx.await?
    }

    async fn replace(self, value: StoredStateGraph) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.0.send(Command::Replace(value, tx))?;
        rx.await?
    }

    async fn delete(self, id: StateGraphId) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.0.send(Command::Delete(id, tx))?;
        rx.await?
    }

    async fn list(self) -> anyhow::Result<Vec<StoredStateGraph>> {
        let (tx, rx) = oneshot::channel();
        self.0.send(Command::List(tx))?;
        rx.await?
    }
}
