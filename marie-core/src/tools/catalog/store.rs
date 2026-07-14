use serde::{Deserialize, Serialize};
use sqlx::Row as _;
use sqlx::postgres::PgRow;

use crate::{
    persistency::{PostgresStore, RedbStore},
    tools::{ToolSignature, catalog::ToolId, declaration::{ToolDeclaration, ToolScope}},
};

/// Espace de clé (`RedbStore`) / nom de table (`PostgresStore`) dédié au
/// catalogue de tools — voir la doc de [`ToolStore`].
const NAMESPACE: &str = "tool";

/// Représentation persistée d'une entrée du catalogue de tools (voir
/// `network::cp::state::ControlPlaneStateMachineStore`), sur le même modèle
/// que `model::catalog::store::StoredModel` — sans chiffrement, une
/// déclaration de tool ne porte aucune information sensible (voir
/// [`ToolDeclaration`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredTool {
    pub id: ToolId,
    pub declaration: ToolDeclaration,
}

/// Encodage local (`RedbStore`) d'une entrée du catalogue : `redb` n'a pas de
/// notion de colonnes (voir `persistency::store::RedbStore`), donc `value`
/// reste un `StoredTool` complet encodé en JSON pour ce backend — seul
/// `PostgresStore`, qui a de vraies colonnes, décompose ses attributs (voir
/// [`PostgresStore::get`] ci-dessous).
fn encode(tool: &StoredTool) -> Vec<u8> {
    // Uniquement des `String`/`Value` : la sérialisation JSON ne peut pas
    // échouer en pratique (même choix que `RpcCall::new`).
    serde_json::to_vec(tool).unwrap()
}

fn decode(bytes: &[u8]) -> anyhow::Result<StoredTool> {
    Ok(serde_json::from_slice(bytes)?)
}

fn scope_to_str(scope: ToolScope) -> &'static str {
    match scope {
        ToolScope::Global => "global",
        ToolScope::Session => "session",
    }
}

fn scope_from_str(scope: &str) -> anyhow::Result<ToolScope> {
    match scope {
        "global" => Ok(ToolScope::Global),
        "session" => Ok(ToolScope::Session),
        other => anyhow::bail!("portée de tool inconnue en base : {other}"),
    }
}

/// Reconstitue un [`StoredTool`] depuis une ligne de la table `tool` (voir
/// `migrations/0006_tool.sql`) — symétrique de l'insertion dans
/// [`PostgresStore::put`].
fn decode_row(row: &PgRow) -> anyhow::Result<StoredTool> {
    let signature = ToolSignature {
        name: row.try_get("name")?,
        description: row.try_get("description")?,
        parameters_schema: row.try_get("parameters_schema")?,
    };
    let scope = scope_from_str(&row.try_get::<String, _>("scope")?)?;

    Ok(StoredTool { id: ToolId::new(row.try_get::<String, _>("id")?), declaration: ToolDeclaration { signature, scope } })
}

/// Stockage CRUD local du catalogue de tools (voir `tools::catalog::store`)
/// — sur le même modèle que `model::catalog::store::ModelStore` (voir sa doc
/// pour la justification de l'absence de trait CRUD générique).
#[async_trait::async_trait]
pub trait ToolStore: Send + Sync {
    async fn get(&self, id: &ToolId) -> anyhow::Result<Option<StoredTool>>;
    async fn put(&self, id: &ToolId, value: &StoredTool) -> anyhow::Result<()>;
    async fn delete(&self, id: &ToolId) -> anyhow::Result<()>;
    /// Toutes les entrées actuellement stockées.
    async fn list(&self) -> anyhow::Result<Vec<StoredTool>>;
}

#[async_trait::async_trait]
impl ToolStore for RedbStore {
    async fn get(&self, id: &ToolId) -> anyhow::Result<Option<StoredTool>> {
        self.get_raw(NAMESPACE, &id.to_string()).await?.as_deref().map(decode).transpose()
    }

    async fn put(&self, id: &ToolId, value: &StoredTool) -> anyhow::Result<()> {
        self.put_raw(NAMESPACE, &id.to_string(), encode(value)).await
    }

    async fn delete(&self, id: &ToolId) -> anyhow::Result<()> {
        self.delete_raw(NAMESPACE, &id.to_string()).await
    }

    async fn list(&self) -> anyhow::Result<Vec<StoredTool>> {
        self.list_raw(NAMESPACE).await?.iter().map(|bytes| decode(bytes)).collect()
    }
}

#[async_trait::async_trait]
impl ToolStore for PostgresStore {
    async fn get(&self, id: &ToolId) -> anyhow::Result<Option<StoredTool>> {
        let id = id.to_string();
        let row = sqlx::query("SELECT id, name, description, parameters_schema, scope FROM tool WHERE id = $1")
            .bind(&id)
            .fetch_optional(self.pool())
            .await?;
        row.as_ref().map(decode_row).transpose()
    }

    async fn put(&self, id: &ToolId, value: &StoredTool) -> anyhow::Result<()> {
        let id = id.to_string();

        sqlx::query(
            "INSERT INTO tool (id, name, description, parameters_schema, scope) VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (id) DO UPDATE SET \
                name = EXCLUDED.name, description = EXCLUDED.description, \
                parameters_schema = EXCLUDED.parameters_schema, scope = EXCLUDED.scope",
        )
        .bind(&id)
        .bind(&value.declaration.signature.name)
        .bind(&value.declaration.signature.description)
        .bind(&value.declaration.signature.parameters_schema)
        .bind(scope_to_str(value.declaration.scope))
        .execute(self.pool())
        .await?;
        Ok(())
    }

    async fn delete(&self, id: &ToolId) -> anyhow::Result<()> {
        let id = id.to_string();
        sqlx::query("DELETE FROM tool WHERE id = $1").bind(&id).execute(self.pool()).await?;
        Ok(())
    }

    async fn list(&self) -> anyhow::Result<Vec<StoredTool>> {
        let rows = sqlx::query("SELECT id, name, description, parameters_schema, scope FROM tool").fetch_all(self.pool()).await?;
        rows.iter().map(decode_row).collect()
    }
}
