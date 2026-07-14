pub mod store;

use std::{collections::HashMap, ops::Deref};

use serde::{Deserialize, Serialize};

use crate::model::declaration::Model;

pub use crate::model::declaration::ModelId;

/// Catalogue des modèles connus du cluster, répliqué via Raft (voir
/// `network::cp::state::ControlPlaneState::models`). Lecture seule depuis
/// l'extérieur (voir [`Deref`]) : toute mutation passe par
/// [`Self::insert`]/[`Self::remove`], appelées uniquement depuis
/// `network::cp::state::apply_request` sur des commandes déjà committées par
/// le cluster — jamais directement.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ModelCatalog(HashMap<ModelId, Model>);

impl Deref for ModelCatalog {
    type Target = HashMap<ModelId, Model>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ModelCatalog {
    pub fn insert(&mut self, id: ModelId, declaration: Model) -> Option<Model> {
        self.0.insert(id, declaration)
    }

    pub fn remove(&mut self, id: &ModelId) -> Option<Model> {
        self.0.remove(id)
    }
}

