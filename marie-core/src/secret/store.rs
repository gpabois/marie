use std::sync::Arc;

use async_trait::async_trait;
#[cfg(feature = "catalog")]
use sqlx::Row as _;
#[cfg(feature = "catalog")]
use sqlx::postgres::PgRow;

use crate::secret::{EncryptedSecret, KeyEpoch, in_memory::InMemorySecretStore, vault::SecretRef};
#[cfg(feature = "catalog")]
use crate::store::PgStore;


#[derive(Debug, Clone)]
pub struct StoredSecret {
    pub id: SecretRef,
    pub key_epoch: KeyEpoch,
    pub ciphertext: Vec<u8>,
    pub nonce: Vec<u8>,
    pub algorithm: String,
}

impl From<StoredSecret> for EncryptedSecret {
    fn from(value: StoredSecret) -> Self {
        Self {
            key_epoch: value.key_epoch,
            ciphertext: value.ciphertext,
            nonce: value.nonce,
            algorithm: value.algorithm
        }
    }
}

/// Stockage bas niveau des secrets chiffrés (voir [`super::vault::Vault`]) —
/// `crate::Error` plutôt qu'un `type Error` associé (comme portait l'ancien
/// `SecretStore`) : un `Error` associé empêcherait `dyn StoreSecret` d'être
/// un type unique utilisable derrière [`SecretStore`] (il faudrait fixer
/// `Error` au type concret d'un unique backend), alors que [`crate::Error`]
/// encaisse aussi bien une `sqlx::Error` (voir `impl StoreSecret for
/// PgStore`) qu'une erreur d'un backend en mémoire (voir
/// [`crate::secret::in_memory::InMemorySecretStore`]), sans que les deux
/// aient besoin de partager un type d'erreur concret commun — même idiome
/// que [`crate::catalog::store::StoreCatalog`].
#[async_trait]
pub trait StoreSecret {
    async fn insert_secret(&self, secret: StoredSecret) -> crate::Result<()>;
    async fn replace_secret(&self, secret: StoredSecret) -> crate::Result<()>;
    async fn remove_secret(&self, secret: &SecretRef) -> crate::Result<()>;
    async fn get_secret(&self, secret_ref: &SecretRef) -> crate::Result<Option<StoredSecret>>;
}

/// Type opaque enveloppant n'importe quel backend [`StoreSecret`] derrière
/// un unique `Arc<dyn StoreSecret + Send + Sync + 'static>` — permet à
/// [`super::vault::Vault`] de ne connaître ni `PgStore` ni
/// [`crate::secret::in_memory::InMemorySecretStore`] par leur type concret,
/// pour pouvoir substituer ce dernier en test unitaire sans passer par
/// Postgres (même principe que [`crate::catalog::store::CatalogStore`]).
#[derive(Clone)]
pub struct SecretStore(Arc<dyn StoreSecret + Send + Sync + 'static>);

impl SecretStore {
    pub fn new(store: impl StoreSecret + Send + Sync + 'static) -> Self {
        Self(Arc::new(store))
    }

    pub fn in_memory() -> Self {
        Self::new(InMemorySecretStore::new())
    }
}

#[async_trait]
impl StoreSecret for SecretStore {
    async fn insert_secret(&self, secret: StoredSecret) -> crate::Result<()> {
        self.0.insert_secret(secret).await
    }

    async fn replace_secret(&self, secret: StoredSecret) -> crate::Result<()> {
        self.0.replace_secret(secret).await
    }

    async fn remove_secret(&self, secret: &SecretRef) -> crate::Result<()> {
        self.0.remove_secret(secret).await
    }

    async fn get_secret(&self, secret_ref: &SecretRef) -> crate::Result<Option<StoredSecret>> {
        self.0.get_secret(secret_ref).await
    }
}

/// Reconstitue un [`StoredSecret`] depuis une ligne de la table `secret`
/// (voir `migrations/0011_secret.sql`) — symétrique de l'insertion dans
/// [`PgStore::insert_secret`]/[`PgStore::replace_secret`].
#[cfg(feature = "catalog")]
fn decode_row(row: &PgRow) -> Result<StoredSecret, sqlx::Error> {
    let id: String = row.try_get("id")?;
    Ok(StoredSecret {
        id: id.parse().unwrap(),
        key_epoch: row.try_get::<i32, _>("key_epoch")? as u32,
        ciphertext: row.try_get("ciphertext")?,
        nonce: row.try_get("nonce")?,
        algorithm: row.try_get("algorithm")?,
    })
}

/// Implémentation PostgreSQL de [`StoreSecret`], contre la table `secret`
/// (voir `migrations/0011_secret.sql`) — gardée derrière `catalog` (seule
/// feature à tirer `sqlx` et à exposer le type [`PgStore`], voir `Cargo.toml`
/// et `crate::store`).
#[cfg(feature = "catalog")]
#[async_trait]
impl StoreSecret for PgStore {
    async fn insert_secret(&self, secret: StoredSecret) -> crate::Result<()> {
        let id = secret.id.to_string();
        sqlx::query("INSERT INTO secret (id, key_epoch, ciphertext, nonce, algorithm) VALUES ($1, $2, $3, $4, $5)")
            .bind(&id)
            .bind(secret.key_epoch as i32)
            .bind(&secret.ciphertext)
            .bind(&secret.nonce)
            .bind(&secret.algorithm)
            .execute(self.pool())
            .await?;
        Ok(())
    }

    async fn replace_secret(&self, secret: StoredSecret) -> crate::Result<()> {
        let id = secret.id.to_string();
        sqlx::query("UPDATE secret SET key_epoch = $2, ciphertext = $3, nonce = $4, algorithm = $5 WHERE id = $1")
            .bind(&id)
            .bind(secret.key_epoch as i32)
            .bind(&secret.ciphertext)
            .bind(&secret.nonce)
            .bind(&secret.algorithm)
            .execute(self.pool())
            .await?;
        Ok(())
    }

    async fn remove_secret(&self, secret: &SecretRef) -> crate::Result<()> {
        let id = secret.to_string();
        sqlx::query("DELETE FROM secret WHERE id = $1").bind(&id).execute(self.pool()).await?;
        Ok(())
    }

    async fn get_secret(&self, secret_ref: &SecretRef) -> crate::Result<Option<StoredSecret>> {
        let id = secret_ref.to_string();
        let row = sqlx::query("SELECT id, key_epoch, ciphertext, nonce, algorithm FROM secret WHERE id = $1")
            .bind(&id)
            .fetch_optional(self.pool())
            .await?;
        Ok(row.as_ref().map(decode_row).transpose()?)
    }
}
