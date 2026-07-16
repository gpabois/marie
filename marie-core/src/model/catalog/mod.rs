use libp2p::rendezvous::client;
// pub mod store;
use loro::{LoroDoc, LoroMap, ToJson};

use super::Model;

pub use crate::model::model::ModelId;

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

    }

    pub fn get(&self, id: &str) -> Option<Model> {
        let models = self.state.get_map("models");
        let value = models.get(id)?;
        let value = value.as_value()?;
        serde_json::from_value(value.to_json_value()).ok()
    }
}

pub struct ModelChangeSet {
    id: String,
    operations: Vec<ModelChange>
}

pub enum ModelChange {
    SetModel(String),
    SetApiKey(String),
    SetClientId(String)
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