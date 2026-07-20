use std::sync::Arc;

use async_trait::async_trait;
use sqlx::Row as _;
use sqlx::postgres::PgRow;
use tokio::select;
use tokio::sync::{mpsc, oneshot};

use crate::{
    model::{
        catalog::ModelId,
        model::{EncryptedModel, Model},
    },
    secret::{Encryptable, EncryptedSecret, KeyEpoch, SecretManager, SecretResult},
    store::PgStore,
};

/// Représentation persistée d'une entrée du catalogue : `id` est porté par
/// la valeur elle-même (pas seulement par la clé de stockage), pour
/// permettre à [`ModelStore::list`] de reconstituer le catalogue complet à
/// froid sans avoir à re-parser les clés.
///
/// `declaration` est privé à dessein : la seule façon publique d'en obtenir
/// un est [`Self::encrypt`], qui chiffre toujours avec
/// [`SecretManager::derive_storage_key`] — une clé stable, indépendante du
/// `PeerId` — jamais avec la clé de transit RPC
/// ([`SecretManager::for_peer`]/`derive_node_key`, régénérée à chaque
/// redémarrage, voir `network::cp::derive_node_id`). `EncryptedModel` est
/// aussi le type des payloads RPC en transit (voir
/// `crate::model::rpc::InsertModel`) : si l'un de ceux-là était persisté tel
/// quel au lieu de repartir du [`Model`] en clair (déjà déchiffré côté RPC)
/// et rechiffré via [`Self::encrypt`], l'entrée deviendrait indéchiffrable
/// dès le prochain redémarrage du nœud (nouveau `PeerId`, donc nouvelle clé
/// de nœud). Ne dérive volontairement pas `Deserialize` : `serde_json`
/// permettrait sinon de reconstruire un `StoredModel` en contournant
/// [`Self::encrypt`], ce qui viderait cette garantie de son sens.
#[derive(Debug, Clone)]
pub struct StoredModel {
    pub id: ModelId,
    declaration: EncryptedModel,
}

impl StoredModel {
    /// Seule façon de construire un [`StoredModel`] destiné à être écrit
    /// (voir [`ModelStore::insert`]/[`ModelStore::replace`]) — voir la doc
    /// du champ `declaration` pour la raison de ce chiffrement systématique
    /// avec la clé de stockage.
    pub fn encrypt(model: &Model, secret: &SecretManager) -> SecretResult<Self> {
        let storage_key = secret.derive_storage_key()?;
        let declaration = model.clone().encrypt(&storage_key)?;
        Ok(Self { id: ModelId::new(model.id()), declaration })
    }

    /// Déchiffre la déclaration lue depuis le stockage local (voir
    /// [`Self::encrypt`]) — sous l'epoch qui l'a chiffrée (voir
    /// `EncryptedSecret::key_epoch`), pas nécessairement l'epoch courante du
    /// nœud : une rotation de master key en cours peut laisser coexister des
    /// lignes chiffrées sous des epochs différentes tant que la passe de
    /// re-chiffrement (voir `model::catalog::rotate`) ne les a pas toutes
    /// migrées.
    pub fn decrypt(&self, secret: &SecretManager) -> SecretResult<Model> {
        let epoch = self.declaration.api_key().key_epoch;
        let storage_key = secret.derive_storage_key_for_epoch(epoch)?;
        Model::decrypt(self.declaration.clone(), &storage_key)
    }

    /// Epoch de la master key qui a chiffré `api_key` (voir
    /// `EncryptedSecret::key_epoch`) — utilisé par `model::catalog::rotate`
    /// pour savoir si une ligne est déjà à l'epoch courante sans avoir à la
    /// déchiffrer.
    pub fn key_epoch(&self) -> KeyEpoch {
        self.declaration.api_key().key_epoch
    }
}

/// Discriminant SQL du seul `EncryptedModel` connu — colonne `kind` de la
/// table `model` (voir `migrations/0002_model.sql`), sur le même principe
/// qu'un tag d'enum : une variante future non `OpenAICompatible` prendrait
/// une valeur différente, et laisserait les colonnes propres à celle-ci
/// NULL.
const KIND_OPENAI_COMPATIBLE: &str = "openai_compatible";

/// Reconstitue un [`StoredModel`] depuis une ligne de la table `model` (voir
/// `migrations/0002_model.sql`) — symétrique de l'insertion dans
/// [`PgStore::insert`]/[`PgStore::replace`].
fn decode_row(row: &PgRow) -> anyhow::Result<StoredModel> {
    let kind: String = row.try_get("kind")?;
    let id: String = row.try_get("id")?;

    let declaration = match kind.as_str() {
        KIND_OPENAI_COMPATIBLE => EncryptedModel::OpenAICompatible {
            id: id.clone(),
            base_url: row.try_get("base_url")?,
            client_id: row.try_get("client_id")?,
            api_key: EncryptedSecret {
                key_epoch: row.try_get::<i32, _>("api_key_epoch")? as u32,
                ciphertext: row.try_get("api_key_ciphertext")?,
                nonce: row.try_get("api_key_nonce")?,
                algorithm: row.try_get("api_key_algorithm")?,
            },
            model: row.try_get("model_name")?,
            system_prompt: row.try_get("system_prompt")?,
        },
        other => anyhow::bail!("discriminant de modèle inconnu en base : {other}"),
    };

    Ok(StoredModel { id: ModelId::new(id), declaration })
}

/// Stockage CRUD local du catalogue de modèles (voir `model::catalog::store`),
/// sur le même principe que [`crate::session::store::SessionStore`] (voir sa
/// doc pour la justification du `self` par valeur + `Clone` plutôt que
/// `&self`, et du découpage `insert`/`replace`).
#[async_trait]
pub trait ModelStore: Send + Sync + Clone {
    async fn get(self, id: ModelId) -> anyhow::Result<Option<StoredModel>>;
    async fn insert(self, value: StoredModel) -> anyhow::Result<()>;
    async fn replace(self, value: StoredModel) -> anyhow::Result<()>;
    async fn delete(self, id: ModelId) -> anyhow::Result<()>;
    /// Toutes les entrées actuellement stockées.
    async fn list(self) -> anyhow::Result<Vec<StoredModel>>;
}

#[async_trait]
impl ModelStore for PgStore {
    async fn get(self, id: ModelId) -> anyhow::Result<Option<StoredModel>> {
        let id = id.to_string();
        let row = sqlx::query(
            "SELECT id, kind, base_url, client_id, api_key_ciphertext, api_key_nonce, api_key_algorithm, api_key_epoch, model_name, system_prompt \
             FROM model WHERE id = $1",
        )
        .bind(&id)
        .fetch_optional(self.pool())
        .await?;
        row.as_ref().map(decode_row).transpose()
    }

    async fn insert(self, value: StoredModel) -> anyhow::Result<()> {
        let id = value.id.to_string();
        let EncryptedModel::OpenAICompatible { base_url, client_id, api_key, model, system_prompt, .. } = &value.declaration;

        sqlx::query(
            "INSERT INTO model (id, kind, base_url, client_id, api_key_ciphertext, api_key_nonce, api_key_algorithm, api_key_epoch, model_name, system_prompt) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(&id)
        .bind(KIND_OPENAI_COMPATIBLE)
        .bind(base_url)
        .bind(client_id)
        .bind(&api_key.ciphertext)
        .bind(&api_key.nonce)
        .bind(&api_key.algorithm)
        .bind(api_key.key_epoch as i32)
        .bind(model)
        .bind(system_prompt)
        .execute(self.pool())
        .await?;
        Ok(())
    }

    async fn replace(self, value: StoredModel) -> anyhow::Result<()> {
        let id = value.id.to_string();
        let EncryptedModel::OpenAICompatible { base_url, client_id, api_key, model, system_prompt, .. } = &value.declaration;

        sqlx::query(
            "UPDATE model SET \
                kind = $2, base_url = $3, client_id = $4, api_key_ciphertext = $5, \
                api_key_nonce = $6, api_key_algorithm = $7, api_key_epoch = $8, model_name = $9, system_prompt = $10 \
             WHERE id = $1",
        )
        .bind(&id)
        .bind(KIND_OPENAI_COMPATIBLE)
        .bind(base_url)
        .bind(client_id)
        .bind(&api_key.ciphertext)
        .bind(&api_key.nonce)
        .bind(&api_key.algorithm)
        .bind(api_key.key_epoch as i32)
        .bind(model)
        .bind(system_prompt)
        .execute(self.pool())
        .await?;
        Ok(())
    }

    async fn delete(self, id: ModelId) -> anyhow::Result<()> {
        let id = id.to_string();
        sqlx::query("DELETE FROM model WHERE id = $1").bind(&id).execute(self.pool()).await?;
        Ok(())
    }

    async fn list(self) -> anyhow::Result<Vec<StoredModel>> {
        let rows = sqlx::query(
            "SELECT id, kind, base_url, client_id, api_key_ciphertext, api_key_nonce, api_key_algorithm, api_key_epoch, model_name, system_prompt FROM model",
        )
        .fetch_all(self.pool())
        .await?;
        rows.iter().map(decode_row).collect()
    }
}

/// Commandes traitées en série par [`ModelStoreActor`] — voir la doc de
/// [`crate::session::store`] (`Command`) pour la raison de cette indirection
/// par acteur plutôt qu'un accès direct au store depuis chaque appelant.
enum Command {
    Get(ModelId, oneshot::Sender<anyhow::Result<Option<StoredModel>>>),
    List(oneshot::Sender<anyhow::Result<Vec<StoredModel>>>),
    Insert(StoredModel, oneshot::Sender<anyhow::Result<()>>),
    Replace(StoredModel, oneshot::Sender<anyhow::Result<()>>),
    Delete(ModelId, oneshot::Sender<anyhow::Result<()>>),
    Shutdown,
}

pub struct ModelStoreActor;

impl ModelStoreActor {
    pub fn create<Store>(store: Store) -> ModelStoreClient
    where
        Store: ModelStore + 'static,
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

        ModelStoreClient(cmd_tx.clone(), Arc::new(Handler(cmd_tx)))
    }
}

struct Handler(mpsc::UnboundedSender<Command>);

impl Drop for Handler {
    fn drop(&mut self) {
        let _ = self.0.send(Command::Shutdown);
    }
}

/// Client du stockage de modèles — cheap à cloner (canal + `Arc`), ferme
/// l'acteur ([`Command::Shutdown`]) quand le dernier exemplaire est droppé.
#[derive(Clone)]
pub struct ModelStoreClient(mpsc::UnboundedSender<Command>, Arc<Handler>);

#[async_trait]
impl ModelStore for ModelStoreClient {
    async fn get(self, id: ModelId) -> anyhow::Result<Option<StoredModel>> {
        let (tx, rx) = oneshot::channel();
        self.0.send(Command::Get(id, tx))?;
        rx.await?
    }

    async fn insert(self, value: StoredModel) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.0.send(Command::Insert(value, tx))?;
        rx.await?
    }

    async fn replace(self, value: StoredModel) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.0.send(Command::Replace(value, tx))?;
        rx.await?
    }

    async fn delete(self, id: ModelId) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.0.send(Command::Delete(id, tx))?;
        rx.await?
    }

    async fn list(self) -> anyhow::Result<Vec<StoredModel>> {
        let (tx, rx) = oneshot::channel();
        self.0.send(Command::List(tx))?;
        rx.await?
    }
}
