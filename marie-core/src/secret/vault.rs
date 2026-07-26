use thiserror::Error;

use crate::{id::{self}, secret::{EncryptedSecret, SecretCodec, SecretError, SecretManager, store::{SecretRef, SecretStore as _, StoredSecret}}, store::PgStore};

#[derive(Debug, Error)]
pub enum VaultError {
    #[error("erreur lors des opérations de stockage du coffre-fort : {0}")]
    StorageError(#[from] sqlx::Error),
    #[error("erreur lors des opérations de chiffrement/déchiffrement du coffre-fort: {0}")]
    SecretError(#[from] SecretError)
}

#[derive(Clone)]
pub struct Vault {
    secrets: SecretManager,
    store: PgStore
}

impl Vault {
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

        self.store.insert_secret(store).await?;
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

        self.store.replace_secret(store).await?;
        Ok(())
    }

    pub async fn get_decrypted(&self, id: SecretRef, namespace: &str) -> Result<Option<Vec<u8>>, VaultError> {
        let Some(secret) = self.store.get_secret(&id).await? else { return Ok(None) };
        let key = self.secrets.derive_key(secret.key_epoch, namespace.as_bytes())?;
        let secret = key.decrypt(EncryptedSecret::from(secret))?;
        Ok(Some(secret))
    }

    pub async fn get_decrypted_str(&self, id: SecretRef, namespace: &str) -> Result<Option<String>, VaultError> {
        let Some(raw) = self.get_decrypted(id, namespace).await? else { return Ok(None) };
        Ok(Some(String::from_utf8(raw).unwrap()))
    }
}