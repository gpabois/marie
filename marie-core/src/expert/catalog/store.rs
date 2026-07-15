use serde::{Deserialize, Serialize};
use sqlx::Row as _;
use sqlx::postgres::PgRow;
use sqlx::types::Json;

use crate::{
    expert::{catalog::ExpertId, declaration::Expert},
    model::catalog::ModelId,
    persistency::{PostgresStore, RedbStore},
    tools::catalog::ToolId,
};

/// Espace de clé (`RedbStore`) / nom de table (`PostgresStore`) dédié au
/// catalogue d'experts — voir la doc de [`ExpertStore`].
const NAMESPACE: &str = "expert";

/// Représentation persistée d'une entrée du catalogue d'experts (voir
/// `network::cp::state::ControlPlaneStateMachineStore`), sur le même modèle
/// que `tools::catalog::store::StoredTool` — sans chiffrement, une
/// déclaration d'expert ne porte aucune information sensible (voir
/// [`ExpertDeclaration`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredExpert {
    pub id: ExpertId,
    pub declaration: Expert,
}

/// Encodage local (`RedbStore`) d'une entrée du catalogue : `redb` n'a pas de
/// notion de colonnes (voir `persistency::store::RedbStore`), donc `value`
/// reste un `StoredExpert` complet encodé en JSON pour ce backend — seul
/// `PostgresStore`, qui a de vraies colonnes, décompose ses attributs (voir
/// [`PostgresStore::get`] ci-dessous).
fn encode(expert: &StoredExpert) -> Vec<u8> {
    // Uniquement des `String`/`Value` : la sérialisation JSON ne peut pas
    // échouer en pratique (même choix que `RpcCall::new`).
    serde_json::to_vec(expert).unwrap()
}

fn decode(bytes: &[u8]) -> anyhow::Result<StoredExpert> {
    Ok(serde_json::from_slice(bytes)?)
}

/// Reconstitue un [`StoredExpert`] depuis une ligne de la table `expert` (voir
/// `migrations/0007_expert.sql`) — symétrique de l'insertion dans
/// [`PostgresStore::put`].
fn decode_row(row: &PgRow) -> anyhow::Result<StoredExpert> {
    let declaration = Expert {
        id: row.try_get::<String, _>("id")?.parse()?,
        prompt: row.try_get("prompt")?,
        model_id: ModelId::new(row.try_get::<String, _>("model_id")?),
        allowed_tools: row.try_get::<Json<Vec<ToolId>>, _>("allowed_tools")?.0,
    };

    Ok(StoredExpert { id: ExpertId::new(row.try_get::<String, _>("id")?), declaration })
}

/// Stockage CRUD local du catalogue d'experts (voir
/// `expert::catalog::store`) — sur le même modèle que
/// `tools::catalog::store::ToolStore` (voir sa doc pour la justification de
/// l'absence de trait CRUD générique).
#[async_trait::async_trait]
pub trait ExpertStore: Send + Sync {
    async fn get(&self, id: &ExpertId) -> anyhow::Result<Option<StoredExpert>>;
    async fn put(&self, id: &ExpertId, value: &StoredExpert) -> anyhow::Result<()>;
    async fn delete(&self, id: &ExpertId) -> anyhow::Result<()>;
    /// Toutes les entrées actuellement stockées.
    async fn list(&self) -> anyhow::Result<Vec<StoredExpert>>;
}

#[async_trait::async_trait]
impl ExpertStore for RedbStore {
    async fn get(&self, id: &ExpertId) -> anyhow::Result<Option<StoredExpert>> {
        self.get_raw(NAMESPACE, &id.to_string()).await?.as_deref().map(decode).transpose()
    }

    async fn put(&self, id: &ExpertId, value: &StoredExpert) -> anyhow::Result<()> {
        self.put_raw(NAMESPACE, &id.to_string(), encode(value)).await
    }

    async fn delete(&self, id: &ExpertId) -> anyhow::Result<()> {
        self.delete_raw(NAMESPACE, &id.to_string()).await
    }

    async fn list(&self) -> anyhow::Result<Vec<StoredExpert>> {
        self.list_raw(NAMESPACE).await?.iter().map(|bytes| decode(bytes)).collect()
    }
}

#[async_trait::async_trait]
impl ExpertStore for PostgresStore {
    async fn get(&self, id: &ExpertId) -> anyhow::Result<Option<StoredExpert>> {
        let id = id.to_string();
        let row = sqlx::query("SELECT id, prompt, model_id, allowed_tools FROM expert WHERE id = $1")
            .bind(&id)
            .fetch_optional(self.pool())
            .await?;
        row.as_ref().map(decode_row).transpose()
    }

    async fn put(&self, id: &ExpertId, value: &StoredExpert) -> anyhow::Result<()> {
        let id = id.to_string();
        let model_id = value.declaration.model_id.to_string();

        sqlx::query(
            "INSERT INTO expert (id, prompt, model_id, allowed_tools) VALUES ($1, $2, $3, $4) \
             ON CONFLICT (id) DO UPDATE SET \
                prompt = EXCLUDED.prompt, model_id = EXCLUDED.model_id, allowed_tools = EXCLUDED.allowed_tools",
        )
        .bind(&id)
        .bind(&value.declaration.prompt)
        .bind(&model_id)
        .bind(Json(&value.declaration.allowed_tools))
        .execute(self.pool())
        .await?;
        Ok(())
    }

    async fn delete(&self, id: &ExpertId) -> anyhow::Result<()> {
        let id = id.to_string();
        sqlx::query("DELETE FROM expert WHERE id = $1").bind(&id).execute(self.pool()).await?;
        Ok(())
    }

    async fn list(&self) -> anyhow::Result<Vec<StoredExpert>> {
        let rows = sqlx::query("SELECT id, prompt, model_id, allowed_tools FROM expert").fetch_all(self.pool()).await?;
        rows.iter().map(decode_row).collect()
    }
}
