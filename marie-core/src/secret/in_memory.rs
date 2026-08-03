use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use parking_lot::Mutex;

use crate::secret::{
    store::{StoreSecret, StoredSecret},
    vault::SecretRef,
};

/// Implémentation en mémoire de [`StoreSecret`] — même rôle que
/// [`crate::store::PgStore`] mais sans dépendance à Postgres, pour tester
/// [`super::vault::Vault`] (via [`super::store::SecretStore::in_memory`])
/// sans base de données, même principe que
/// [`crate::catalog::in_memory::InMemoryCatalog`].
#[derive(Default, Clone)]
pub struct InMemorySecretStore {
    secrets: Arc<Mutex<HashMap<SecretRef, StoredSecret>>>,
}

impl InMemorySecretStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl StoreSecret for InMemorySecretStore {
    async fn insert_secret(&self, secret: StoredSecret) -> crate::Result<()> {
        self.secrets.lock().insert(secret.id, secret);
        Ok(())
    }

    async fn replace_secret(&self, secret: StoredSecret) -> crate::Result<()> {
        self.secrets.lock().insert(secret.id, secret);
        Ok(())
    }

    async fn remove_secret(&self, secret: &SecretRef) -> crate::Result<()> {
        self.secrets.lock().remove(secret);
        Ok(())
    }

    async fn get_secret(&self, secret_ref: &SecretRef) -> crate::Result<Option<StoredSecret>> {
        Ok(self.secrets.lock().get(secret_ref).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stored(id: SecretRef) -> StoredSecret {
        StoredSecret {
            id,
            key_epoch: 0,
            ciphertext: vec![1, 2, 3],
            nonce: vec![4, 5, 6],
            algorithm: "test".to_string(),
        }
    }

    #[tokio::test]
    async fn test_insert_then_get_roundtrips() {
        let store = InMemorySecretStore::new();
        let id = SecretRef::new(crate::id::generate_id());

        store.insert_secret(stored(id)).await.unwrap();

        let got = store.get_secret(&id).await.unwrap().unwrap();
        assert_eq!(got.ciphertext, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_replace_overwrites_existing() {
        let store = InMemorySecretStore::new();
        let id = SecretRef::new(crate::id::generate_id());

        store.insert_secret(stored(id)).await.unwrap();

        let mut updated = stored(id);
        updated.ciphertext = vec![9, 9, 9];
        store.replace_secret(updated).await.unwrap();

        let got = store.get_secret(&id).await.unwrap().unwrap();
        assert_eq!(got.ciphertext, vec![9, 9, 9]);
    }

    #[tokio::test]
    async fn test_remove_makes_secret_unreadable() {
        let store = InMemorySecretStore::new();
        let id = SecretRef::new(crate::id::generate_id());

        store.insert_secret(stored(id)).await.unwrap();
        store.remove_secret(&id).await.unwrap();

        assert!(store.get_secret(&id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_get_unknown_secret_returns_none() {
        let store = InMemorySecretStore::new();
        let id = SecretRef::new(crate::id::generate_id());

        assert!(store.get_secret(&id).await.unwrap().is_none());
    }
}
