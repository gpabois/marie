use std::{fmt, str::FromStr};

use async_trait::async_trait;
use bytemuck::{Pod, Zeroable};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
#[cfg(feature = "catalog")]
use sqlx::Row as _;
#[cfg(feature = "catalog")]
use sqlx::postgres::PgRow;

use crate::{id::ID, secret::{EncryptedSecret, KeyEpoch}};
#[cfg(feature = "catalog")]
use crate::store::PgStore;

/// Référence opaque vers un secret stocké (voir [`StoredSecret`]) — un simple
/// alias sur [`ID`], sur le même principe que `session::SessionId`/
/// `workspace::WorkspaceId`.
#[derive(Debug, Hash, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Pod, Zeroable, JsonSchema)]
#[repr(C)]
pub struct SecretRef(pub(crate) ID);

impl SecretRef {
    #[must_use]
    pub fn new(id: ID) -> Self {
        Self(id)
    }
}

impl fmt::Display for SecretRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl FromStr for SecretRef {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

impl From<ID> for SecretRef {
    fn from(id: ID) -> Self {
        Self(id)
    }
}

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

#[async_trait]
pub trait SecretStore {
    type Error;

    async fn insert_secret(&self, secret: StoredSecret) -> Result<(), Self::Error>;
    async fn replace_secret(&self, secret: StoredSecret) -> Result<(), Self::Error>;
    async fn remove_secret(&self, secret: &SecretRef) -> Result<(), Self::Error>;
    async fn get_secret(&self, secret_ref: &SecretRef) -> Result<Option<StoredSecret>, Self::Error>;
}

/// Reconstitue un [`StoredSecret`] depuis une ligne de la table `secret`
/// (voir `migrations/0011_secret.sql`) — symétrique de l'insertion dans
/// [`PgStore::insert`]/[`PgStore::replace`].
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

#[async_trait]
impl SecretStore for PgStore {
    type Error = sqlx::Error;

    async fn insert_secret(&self, secret: StoredSecret) -> Result<(), Self::Error> {
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

    async fn replace_secret(&self, secret: StoredSecret) -> Result<(), Self::Error> {
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

    async fn remove_secret(&self, secret: &SecretRef) -> Result<(), Self::Error> {
        let id = secret.to_string();
        sqlx::query("DELETE FROM secret WHERE id = $1").bind(&id).execute(self.pool()).await?;
        Ok(())
    }

    async fn get_secret(&self, secret_ref: &SecretRef) -> Result<Option<StoredSecret>, Self::Error> {
        let id = secret_ref.to_string();
        let row = sqlx::query("SELECT id, key_epoch, ciphertext, nonce, algorithm FROM secret WHERE id = $1")
            .bind(&id)
            .fetch_optional(self.pool())
            .await?;
        row.as_ref().map(decode_row).transpose()
    }
}