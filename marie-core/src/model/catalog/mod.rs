// pub mod store;
use loro::{LoroDoc, LoroMap, ToJson};
use serde::{Deserialize, Serialize};

use super::Model;

pub use crate::model::model::ModelId;
use crate::secret::{Encryptable, EncryptedSecret};


pub struct ModelCatalog {
    state: LoroDoc,
}


impl ModelCatalog {
    pub fn new() -> ModelCatalog {
        let state = LoroDoc::new();
        state.get_map("models");

        Self {
            state
        }
    }

    pub fn insert(&mut self, model: Model) {
        let value = serde_json::to_value(&model).unwrap();
        let models = self.state.get_map("models");
        models.insert(model.id(), value);
    }

    pub fn update(&mut self, changeset: ModelChangeSet) {
        let models = self.state.get_map("models");
        let mut map = models.ensure_mergeable_map(&changeset.id).unwrap();
        changeset.operations.into_iter().for_each(move |change| {
            change.apply(&mut map);
        });
        
    }

    pub fn get(&self, id: &str) -> Option<Model> {
        let models = self.state.get_map("models");
        let value = models.get(id)?;
        let value = value.as_value()?;
        serde_json::from_value(value.to_json_value()).ok()
    }

}

#[derive(Serialize, Deserialize)]
pub struct ModelChangeSet {
    pub id: String,
    pub operations: Vec<ModelChange>
}

impl Encryptable for ModelChangeSet {
    type Encrypted = EncryptedModelChangeSet;

    fn encrypt<C>(self, codec: &C) -> crate::secret::SecretResult<Self::Encrypted> where C: crate::secret::SecretCodec {
        Ok(EncryptedModelChangeSet { 
            id: self.id, 
            operations: self.operations
                .into_iter()
                .map(move |change| change.encrypt(codec)) 
                .collect::<Result<Vec<_>,_>>()?
        })
    }

    fn decrypt<C>(encrypted: Self::Encrypted, codec: &C) -> crate::secret::SecretResult<Self> where C: crate::secret::SecretCodec {
        Ok(ModelChangeSet { 
            id: encrypted.id, 
            operations: encrypted.operations
                .into_iter()
                .map(move |change| ModelChange::decrypt(change, codec)) 
                .collect::<Result<Vec<_>,_>>()?
        })
    }
}

#[derive(Serialize, Deserialize)]
pub struct EncryptedModelChangeSet {
    id: String,
    operations: Vec<EncryptedModelChange>
}

#[derive(Serialize, Deserialize)]
pub enum ModelChange {
    SetModel(String),
    SetApiKey(String),
    SetClientId(String)
}

impl Encryptable for ModelChange {
    type Encrypted = EncryptedModelChange;

    fn encrypt<C>(self, codec: &C) -> crate::secret::SecretResult<Self::Encrypted> where C: crate::secret::SecretCodec {
        Ok(match self {
            ModelChange::SetModel(model) => EncryptedModelChange::SetModel(model),
            ModelChange::SetApiKey(api_key) => EncryptedModelChange::SetApiKey(codec.encrypt_str(api_key)?),
            ModelChange::SetClientId(client_id) => EncryptedModelChange::SetClientId(client_id),
        })
    }

    fn decrypt<C>(encrypted: Self::Encrypted, codec: &C) -> crate::secret::SecretResult<Self> where C: crate::secret::SecretCodec {
        Ok(match encrypted {
            EncryptedModelChange::SetModel(model) => ModelChange::SetModel(model),
            EncryptedModelChange::SetApiKey(encrypted_secret) => ModelChange::SetApiKey(codec.decrypt_str(encrypted_secret)?),
            EncryptedModelChange::SetClientId(client_id) => ModelChange::SetClientId(client_id),
        })
    }
}

impl ModelChange {
    fn apply(self, model: &mut LoroMap) {
        match self {
            ModelChange::SetModel(name) => {
                model.insert("model", name);
            },
            ModelChange::SetApiKey(api_key) => {
                model.insert("api_key", api_key);
            },
            ModelChange::SetClientId(client_id) => {
                model.insert("client_id", client_id);
            },
        }
    }
}

#[derive(Serialize, Deserialize)]
pub enum EncryptedModelChange {
    SetModel(String),
    SetApiKey(EncryptedSecret),
    SetClientId(String)
}