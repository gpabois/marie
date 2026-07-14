use serde::{Deserialize, Serialize};
use sqlx::Row as _;
use sqlx::postgres::PgRow;
use sqlx::types::Json;

use crate::{
    mode::state_graph::{Edge, Node, catalog::StateGraphId, declaration::StateGraphDeclaration},
    persistency::{PostgresStore, RedbStore},
};

/// Espace de clé (`RedbStore`) / nom de table (`PostgresStore`) dédié au
/// catalogue de graphes d'états — voir la doc de [`StateGraphStore`].
const NAMESPACE: &str = "state_graph";

/// Représentation persistée d'une entrée du catalogue de graphes d'états
/// (voir `network::cp::state::ControlPlaneStateMachineStore`), sur le même
/// modèle que `expert::catalog::store::StoredExpert` — sans chiffrement, une
/// déclaration de graphe ne porte aucune information sensible (voir
/// [`StateGraphDeclaration`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredStateGraph {
    pub id: StateGraphId,
    pub declaration: StateGraphDeclaration,
}

/// Encodage local (`RedbStore`) d'une entrée du catalogue : `redb` n'a pas de
/// notion de colonnes (voir `persistency::store::RedbStore`), donc `value`
/// reste un `StoredStateGraph` complet encodé en JSON pour ce backend — seul
/// `PostgresStore`, qui a de vraies colonnes, décompose ses attributs (voir
/// [`PostgresStore::get`] ci-dessous).
fn encode(state_graph: &StoredStateGraph) -> Vec<u8> {
    // Uniquement des `String`/`Value` : la sérialisation JSON ne peut pas
    // échouer en pratique (même choix que `RpcCall::new`).
    serde_json::to_vec(state_graph).unwrap()
}

fn decode(bytes: &[u8]) -> anyhow::Result<StoredStateGraph> {
    Ok(serde_json::from_slice(bytes)?)
}

/// Reconstitue un [`StoredStateGraph`] depuis une ligne de la table
/// `state_graph` (voir `migrations/0008_state_graph.sql`) — symétrique de
/// l'insertion dans [`PostgresStore::put`].
fn decode_row(row: &PgRow) -> anyhow::Result<StoredStateGraph> {
    let declaration = StateGraphDeclaration {
        nodes: row.try_get::<Json<Vec<Node>>, _>("nodes")?.0,
        edges: row.try_get::<Json<Vec<Edge>>, _>("edges")?.0,
        entry: row.try_get("entry")?,
    };

    Ok(StoredStateGraph { id: StateGraphId::new(row.try_get::<String, _>("id")?), declaration })
}

/// Stockage CRUD local du catalogue de graphes d'états (voir
/// `state_graph::catalog::store`) — sur le même modèle que
/// `expert::catalog::store::ExpertStore` (voir sa doc pour la justification
/// de l'absence de trait CRUD générique).
#[async_trait::async_trait]
pub trait StateGraphStore: Send + Sync {
    async fn get(&self, id: &StateGraphId) -> anyhow::Result<Option<StoredStateGraph>>;
    async fn put(&self, id: &StateGraphId, value: &StoredStateGraph) -> anyhow::Result<()>;
    async fn delete(&self, id: &StateGraphId) -> anyhow::Result<()>;
    /// Toutes les entrées actuellement stockées.
    async fn list(&self) -> anyhow::Result<Vec<StoredStateGraph>>;
}

#[async_trait::async_trait]
impl StateGraphStore for RedbStore {
    async fn get(&self, id: &StateGraphId) -> anyhow::Result<Option<StoredStateGraph>> {
        self.get_raw(NAMESPACE, &id.to_string()).await?.as_deref().map(decode).transpose()
    }

    async fn put(&self, id: &StateGraphId, value: &StoredStateGraph) -> anyhow::Result<()> {
        self.put_raw(NAMESPACE, &id.to_string(), encode(value)).await
    }

    async fn delete(&self, id: &StateGraphId) -> anyhow::Result<()> {
        self.delete_raw(NAMESPACE, &id.to_string()).await
    }

    async fn list(&self) -> anyhow::Result<Vec<StoredStateGraph>> {
        self.list_raw(NAMESPACE).await?.iter().map(|bytes| decode(bytes)).collect()
    }
}

#[async_trait::async_trait]
impl StateGraphStore for PostgresStore {
    async fn get(&self, id: &StateGraphId) -> anyhow::Result<Option<StoredStateGraph>> {
        let id = id.to_string();
        let row = sqlx::query("SELECT id, entry, nodes, edges FROM state_graph WHERE id = $1")
            .bind(&id)
            .fetch_optional(self.pool())
            .await?;
        row.as_ref().map(decode_row).transpose()
    }

    async fn put(&self, id: &StateGraphId, value: &StoredStateGraph) -> anyhow::Result<()> {
        let id = id.to_string();

        sqlx::query(
            "INSERT INTO state_graph (id, entry, nodes, edges) VALUES ($1, $2, $3, $4) \
             ON CONFLICT (id) DO UPDATE SET entry = EXCLUDED.entry, nodes = EXCLUDED.nodes, edges = EXCLUDED.edges",
        )
        .bind(&id)
        .bind(&value.declaration.entry)
        .bind(Json(&value.declaration.nodes))
        .bind(Json(&value.declaration.edges))
        .execute(self.pool())
        .await?;
        Ok(())
    }

    async fn delete(&self, id: &StateGraphId) -> anyhow::Result<()> {
        let id = id.to_string();
        sqlx::query("DELETE FROM state_graph WHERE id = $1").bind(&id).execute(self.pool()).await?;
        Ok(())
    }

    async fn list(&self) -> anyhow::Result<Vec<StoredStateGraph>> {
        let rows = sqlx::query("SELECT id, entry, nodes, edges FROM state_graph").fetch_all(self.pool()).await?;
        rows.iter().map(decode_row).collect()
    }
}
