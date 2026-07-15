use serde::{Deserialize, Serialize};
use sqlx::Row as _;
use sqlx::postgres::PgRow;

use crate::{
    model::{
        catalog::ModelId,
        declaration::{EncryptedModel, Model},
    },
    persistency::{PostgresStore, RedbStore},
    secret::{EncryptedSecret, SecretManager, SecretResult},
};

/// Espace de clé (`RedbStore`) / nom de table (`PostgresStore`) dédié au
/// catalogue de modèles — voir la doc de [`ModelStore`].
const NAMESPACE: &str = "model";

/// Représentation persistée d'une entrée du catalogue (voir
/// `network::cp::state::ControlPlaneStateMachineStore`) : `id` est porté par
/// la valeur elle-même (pas seulement par la clé de stockage), pour permettre
/// à [`ModelStore::list`] de reconstituer le catalogue complet à froid sans
/// avoir à re-parser les clés. `declaration` a déjà sa clé API chiffrée (voir
/// [`encrypt_for_storage`]) — jamais en clair sur disque.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredModel {
    pub id: ModelId,
    pub declaration: EncryptedModel,
}

/// Discriminant SQL du seul `EncryptedModel` connu — colonne `kind` de la
/// table `model` (voir `migrations/0005_model.sql`), sur le même principe
/// qu'un tag d'enum : une variante future non `OpenAICompatible` prendrait une
/// valeur différente, et laisserait les colonnes propres à celle-ci NULL.
const KIND_OPENAI_COMPATIBLE: &str = "openai_compatible";

/// Encodage local (`RedbStore`) d'une entrée du catalogue : `redb` n'a pas de
/// notion de colonnes (voir `persistency::store::RedbStore`), donc `value`
/// reste un `StoredModel` complet encodé en JSON pour ce backend — seul
/// `PostgresStore`, qui a de vraies colonnes, décompose ses attributs (voir
/// [`PostgresStore::get`] ci-dessous).
fn encode(model: &StoredModel) -> Vec<u8> {
    // Uniquement des `String`/`Vec<u8>` : la sérialisation JSON ne peut pas
    // échouer en pratique (même choix que `RpcCall::new`).
    serde_json::to_vec(model).unwrap()
}

fn decode(bytes: &[u8]) -> anyhow::Result<StoredModel> {
    Ok(serde_json::from_slice(bytes)?)
}

/// Reconstitue un [`StoredModel`] depuis une ligne de la table `model` (voir
/// `migrations/0005_model.sql`) — symétrique de l'insertion dans
/// [`PostgresStore::put`].
fn decode_row(row: &PgRow) -> anyhow::Result<StoredModel> {
    let kind: String = row.try_get("kind")?;

    let declaration = match kind.as_str() {
        KIND_OPENAI_COMPATIBLE => EncryptedModel::OpenAICompatible {
            base_url: row.try_get("base_url")?,
            client_id: row.try_get("client_id")?,
            api_key: EncryptedSecret {
                ciphertext: row.try_get("api_key_ciphertext")?,
                nonce: row.try_get("api_key_nonce")?,
                algorithm: row.try_get("api_key_algorithm")?,
            },
            model: row.try_get("model_name")?,
            system_prompt: row.try_get("system_prompt")?,
        },
        other => anyhow::bail!("discriminant de modèle inconnu en base : {other}"),
    };

    Ok(StoredModel { id: ModelId::new(row.try_get::<String, _>("id")?), declaration })
}

/// Chiffre `declaration` pour stockage au repos (voir [`StoredModel`]) : la
/// clé API est chiffrée avec [`SecretManager::derive_storage_key`], une clé
/// stable dérivée de la master key du cluster — contrairement à
/// [`SecretManager::derive_node_key`], elle ne dépend pas d'un `PeerId`
/// (régénéré à chaque démarrage, voir `network::cp::derive_node_id`), donc un
/// nœud peut déchiffrer à froid ce qu'il a persisté lors d'un précédent
/// démarrage.
pub fn encrypt_for_storage(declaration: &Model, secret: &SecretManager) -> SecretResult<EncryptedModel> {
    let storage_key = secret.derive_storage_key()?;
    let api_key = secret.encrypt_api_key(declaration.api_key(), &storage_key)?;
    Ok(declaration.encrypt(api_key))
}

/// Déchiffre une déclaration lue depuis le stockage local (voir
/// [`encrypt_for_storage`]).
pub fn decrypt_from_storage(encrypted: &EncryptedModel, secret: &SecretManager) -> SecretResult<Model> {
    let storage_key = secret.derive_storage_key()?;
    let api_key = secret.decrypt_api_key(encrypted.api_key(), &storage_key)?;
    Ok(encrypted.clone().decrypt(api_key))
}

/// Stockage CRUD local du catalogue de modèles (voir
/// `model::catalog::store`) — utilisé pour la récupération à froid du
/// catalogue au démarrage d'un control plane (voir
/// `network::cp::start_control_plane`), la source de vérité restant l'état
/// Raft répliqué (`ControlPlaneState::models`). Implémenté directement pour
/// [`RedbStore`] et [`PostgresStore`], sur le même principe que
/// `persistency::SessionStore` (voir sa doc pour la justification de
/// l'absence de trait CRUD générique).
#[async_trait::async_trait]
pub trait ModelStore: Send + Sync {
    async fn get(&self, id: &ModelId) -> anyhow::Result<Option<StoredModel>>;
    async fn put(&self, id: &ModelId, value: &StoredModel) -> anyhow::Result<()>;
    async fn delete(&self, id: &ModelId) -> anyhow::Result<()>;
    /// Toutes les entrées actuellement stockées.
    async fn list(&self) -> anyhow::Result<Vec<StoredModel>>;
}

#[async_trait::async_trait]
impl ModelStore for RedbStore {
    async fn get(&self, id: &ModelId) -> anyhow::Result<Option<StoredModel>> {
        self.get_raw(NAMESPACE, &id.to_string()).await?.as_deref().map(decode).transpose()
    }

    async fn put(&self, id: &ModelId, value: &StoredModel) -> anyhow::Result<()> {
        self.put_raw(NAMESPACE, &id.to_string(), encode(value)).await
    }

    async fn delete(&self, id: &ModelId) -> anyhow::Result<()> {
        self.delete_raw(NAMESPACE, &id.to_string()).await
    }

    async fn list(&self) -> anyhow::Result<Vec<StoredModel>> {
        self.list_raw(NAMESPACE).await?.iter().map(|bytes| decode(bytes)).collect()
    }
}

#[async_trait::async_trait]
impl ModelStore for PostgresStore {
    async fn get(&self, id: &ModelId) -> anyhow::Result<Option<StoredModel>> {
        let id = id.to_string();
        let row = sqlx::query(
            "SELECT id, kind, base_url, client_id, api_key_ciphertext, api_key_nonce, api_key_algorithm, model_name, system_prompt \
             FROM model WHERE id = $1",
        )
        .bind(&id)
        .fetch_optional(self.pool())
        .await?;
        row.as_ref().map(decode_row).transpose()
    }

    async fn put(&self, id: &ModelId, value: &StoredModel) -> anyhow::Result<()> {
        let id = id.to_string();

        let EncryptedModel::OpenAICompatible { base_url, client_id, api_key, model, system_prompt } = &value.declaration;

        sqlx::query(
            "INSERT INTO model (id, kind, base_url, client_id, api_key_ciphertext, api_key_nonce, api_key_algorithm, model_name, system_prompt) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
             ON CONFLICT (id) DO UPDATE SET \
                kind = EXCLUDED.kind, base_url = EXCLUDED.base_url, client_id = EXCLUDED.client_id, \
                api_key_ciphertext = EXCLUDED.api_key_ciphertext, api_key_nonce = EXCLUDED.api_key_nonce, \
                api_key_algorithm = EXCLUDED.api_key_algorithm, model_name = EXCLUDED.model_name, \
                system_prompt = EXCLUDED.system_prompt",
        )
        .bind(&id)
        .bind(KIND_OPENAI_COMPATIBLE)
        .bind(base_url)
        .bind(client_id)
        .bind(&api_key.ciphertext)
        .bind(&api_key.nonce)
        .bind(&api_key.algorithm)
        .bind(model)
        .bind(system_prompt)
        .execute(self.pool())
        .await?;
        Ok(())
    }

    async fn delete(&self, id: &ModelId) -> anyhow::Result<()> {
        let id = id.to_string();
        sqlx::query("DELETE FROM model WHERE id = $1").bind(&id).execute(self.pool()).await?;
        Ok(())
    }

    async fn list(&self) -> anyhow::Result<Vec<StoredModel>> {
        let rows = sqlx::query(
            "SELECT id, kind, base_url, client_id, api_key_ciphertext, api_key_nonce, api_key_algorithm, model_name, system_prompt FROM model",
        )
        .fetch_all(self.pool())
        .await?;
        rows.iter().map(decode_row).collect()
    }
}
