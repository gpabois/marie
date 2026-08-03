use std::{fmt, str::FromStr};

use bytemuck::{Pod, Zeroable};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{di::{Constructible, Get}, id::{self, ID}, secret::{EncryptedSecret, SecretCodec, SecretError, SecretManager, store::{SecretStore, StoreSecret as _, StoredSecret}}};

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


#[derive(Debug, Error)]
pub enum VaultError {
    /// Pas de `#[from]` : [`crate::Error`] n'implémente volontairement pas
    /// `std::error::Error` (voir sa doc), donc `thiserror` ne peut pas en
    /// dériver `source()` automatiquement — conversion manuelle via
    /// `.map_err(VaultError::StorageError)` sur chaque appel à
    /// [`StoreSecret`], même idiome que `CatalogError::StoreError`.
    #[error("erreur lors des opérations de stockage du coffre-fort : {0}")]
    StorageError(crate::Error),
    #[error("erreur lors des opérations de chiffrement/déchiffrement du coffre-fort: {0}")]
    SecretError(#[from] SecretError),
    #[error("le secret référencé n'existe pas ou plus : {0}")]
    UnknownSecret(SecretRef)
}

/// Façade de haut niveau au-dessus du chiffrement (voir [`SecretManager`])
/// et du stockage bas niveau — `store` est le type opaque [`SecretStore`]
/// plutôt qu'un `PgStore` concret, pour pouvoir le construire en test avec
/// [`SecretStore::in_memory`] sans passer par Postgres, même principe que
/// [`crate::catalog::Catalog`].
#[derive(Clone)]
pub struct Vault {
    secrets: SecretManager,
    store: SecretStore
}

impl<C> Constructible<C> for Vault 
    where C: Get<SecretManager> + Get<SecretStore>
{
    fn construct(container: &C, _: ()) -> Self {
        Self::new(container.get(), container.get())
    }
}

impl Vault {
    pub fn new(secrets: SecretManager, store: SecretStore) -> Self {
        Self { secrets, store }
    }

    pub fn in_memory(secrets: SecretManager) -> Self {
        Self::new(secrets, SecretStore::in_memory())
    }

    pub async fn store(&self, namespace: &str, secret: &[u8]) -> Result<SecretRef, VaultError> {
        let id = SecretRef(id::generate_id());
        let epoch = self.secrets.current_epoch();
        let key = self.secrets.derive_key(epoch, namespace.as_bytes())?;
        let encrypted = key.encrypt(secret)?;
        let store = StoredSecret {
            id,
            key_epoch: encrypted.key_epoch,
            ciphertext: encrypted.ciphertext,
            nonce: encrypted.nonce,
            algorithm: encrypted.algorithm
        };

        self.store.insert_secret(store).await.map_err(VaultError::StorageError)?;
        Ok(id)
    }

    pub async fn update(&self, id: SecretRef, namespace: &str, secret: &[u8]) -> Result<(), VaultError> {
        let epoch = self.secrets.current_epoch();
        let key = self.secrets.derive_key(epoch, namespace.as_bytes())?;
        let encrypted = key.encrypt(secret)?;

        let store = StoredSecret {
            id,
            key_epoch: encrypted.key_epoch,
            ciphertext: encrypted.ciphertext,
            nonce: encrypted.nonce,
            algorithm: encrypted.algorithm
        };

        self.store.replace_secret(store).await.map_err(VaultError::StorageError)?;
        Ok(())
    }

    /// Supprime définitivement un secret (voir
    /// [`StoreSecret::remove_secret`]) — contrairement au catalogue
    /// applicatif ([`crate::catalog::Catalog::delete`]), pas de soft-delete
    /// ici : un secret laissé en base après que son détenteur (ex: un
    /// `Model`) a été supprimé n'a plus aucun usage légitime.
    pub async fn remove(&self, id: SecretRef) -> Result<(), VaultError> {
        self.store.remove_secret(&id).await.map_err(VaultError::StorageError)?;
        Ok(())
    }

    pub async fn get_decrypted(&self, id: SecretRef, namespace: &str) -> Result<Vec<u8>, VaultError> {
        let Some(secret) = self.store.get_secret(&id).await.map_err(VaultError::StorageError)? else { return Err(VaultError::UnknownSecret(id)) };
        let key = self.secrets.derive_key(secret.key_epoch, namespace.as_bytes())?;
        let secret = key.decrypt(EncryptedSecret::from(secret))?;
        Ok(secret)
    }

    pub async fn get_decrypted_str(&self, id: SecretRef, namespace: &str) -> Result<String, VaultError> {
        let raw = self.get_decrypted(id, namespace).await?;
        Ok(String::from_utf8(raw).unwrap())
    }
}