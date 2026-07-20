use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use marie_core::model::catalog::rotate::{assert_no_rows_at_epoch, reencrypt_model_store};
use marie_core::model::catalog::store::{ModelStore, ModelStoreActor, StoredModel};
use marie_core::model::{Model, ModelId};
use marie_core::secret::SecretManager;

/// Store mémoire minimal pour exercer `model::catalog::rotate` depuis
/// l'extérieur du crate, sur le même principe que
/// `marie-test/tests/session_store.rs::MemoryStore` (pas de Postgres dans ce
/// crate de tests).
#[derive(Clone, Default)]
struct MemoryModelStore(Arc<Mutex<HashMap<ModelId, StoredModel>>>);

#[async_trait]
impl ModelStore for MemoryModelStore {
    async fn get(self, id: ModelId) -> anyhow::Result<Option<StoredModel>> {
        Ok(self.0.lock().unwrap().get(&id).cloned())
    }

    async fn insert(self, value: StoredModel) -> anyhow::Result<()> {
        self.0.lock().unwrap().insert(value.id.clone(), value);
        Ok(())
    }

    async fn replace(self, value: StoredModel) -> anyhow::Result<()> {
        self.0.lock().unwrap().insert(value.id.clone(), value);
        Ok(())
    }

    async fn delete(self, id: ModelId) -> anyhow::Result<()> {
        self.0.lock().unwrap().remove(&id);
        Ok(())
    }

    async fn list(self) -> anyhow::Result<Vec<StoredModel>> {
        Ok(self.0.lock().unwrap().values().cloned().collect())
    }
}

fn key(byte: u8) -> [u8; 32] {
    [byte; 32]
}

fn sample_model(id: &str, api_key: &str) -> Model {
    Model::OpenAICompatible {
        id: id.to_string(),
        base_url: "https://example.test".to_string(),
        client_id: "client".to_string(),
        api_key: api_key.to_string(),
        model: "gpt-test".to_string(),
        system_prompt: None,
    }
}

/// Scénario complet du runbook de rotation (voir la doc de
/// `marie_core::secret::SecretManager`) contre un store en mémoire : chiffré
/// sous l'epoch 0 -> ajout de l'epoch 1 (sans bascule) -> bascule -> passe de
/// re-chiffrement -> retrait de l'epoch 0 -> tout reste lisible, sauf une
/// entrée volontairement laissée de côté (simulant une ligne oubliée par la
/// passe), qui devient bien indéchiffrable et fait échouer le garde-fou.
#[tokio::test]
async fn rotation_migrates_and_retires_old_epoch() {
    let secret = SecretManager::new(&key(1));
    let store = ModelStoreActor::create(MemoryModelStore::default());

    let originals = vec![sample_model("model-a", "api-key-a"), sample_model("model-b", "api-key-b")];

    for model in &originals {
        let stored = StoredModel::encrypt(model, &secret).unwrap();
        store.clone().insert(stored).await.unwrap();
    }

    // Étape 2 : charger l'epoch 1 sans basculer — les données existantes
    // (epoch 0) restent lisibles.
    secret.add_epoch(1, key(2));
    for model in &originals {
        let stored = store.clone().get(ModelId::new(model.id())).await.unwrap().unwrap();
        assert_eq!(stored.key_epoch(), 0);
        assert_eq!(&stored.decrypt(&secret).unwrap(), model);
    }

    // Étape 3 : basculer — les nouvelles écritures utilisent désormais
    // l'epoch 1, les anciennes restent à l'epoch 0.
    secret.set_current_epoch(1).unwrap();
    let extra = sample_model("model-c", "api-key-c");
    let stored_extra = StoredModel::encrypt(&extra, &secret).unwrap();
    assert_eq!(stored_extra.key_epoch(), 1);
    store.clone().insert(stored_extra).await.unwrap();

    for model in &originals {
        let stored = store.clone().get(ModelId::new(model.id())).await.unwrap().unwrap();
        assert_eq!(stored.key_epoch(), 0);
    }

    // Étape 4 : re-chiffrer.
    let report = reencrypt_model_store(store.clone(), &secret).await.unwrap();
    assert_eq!(report.migrated, 2); // model-a, model-b
    assert_eq!(report.already_current, 1); // model-c, déjà à jour

    for stored in store.clone().list().await.unwrap() {
        assert_eq!(stored.key_epoch(), 1);
    }

    // Les clés API en clair sont inchangées après re-chiffrement.
    for model in &originals {
        let stored = store.clone().get(ModelId::new(model.id())).await.unwrap().unwrap();
        assert_eq!(&stored.decrypt(&secret).unwrap(), model);
    }

    // Étape 5 : plus aucune ligne sous l'ancienne epoch.
    assert_no_rows_at_epoch(store.clone(), 0).await.unwrap();

    // Étape 6 : retirer — tout ce qui a été migré reste lisible.
    secret.retire_epoch(0).unwrap();
    for stored in store.clone().list().await.unwrap() {
        assert!(stored.decrypt(&secret).is_ok());
    }

    // Contrôle négatif : une entrée volontairement laissée sous l'epoch 0
    // (simule une ligne oubliée par la passe de re-chiffrement, chiffrée
    // avec la même master key `key(1)` que l'epoch 0 déjà retirée de
    // `secret`) devient indéchiffrable, et le garde-fou le détecte.
    let stale_secret = SecretManager::new(&key(1));
    let missed = StoredModel::encrypt(&sample_model("model-missed", "api-key-missed"), &stale_secret).unwrap();
    store.clone().insert(missed.clone()).await.unwrap();

    assert!(assert_no_rows_at_epoch(store.clone(), 0).await.is_err());
    assert!(missed.decrypt(&secret).is_err());
}
