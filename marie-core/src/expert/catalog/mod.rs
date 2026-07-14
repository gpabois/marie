pub mod store;

use std::{collections::HashMap, ops::Deref};

use serde::{Deserialize, Serialize};

use crate::expert::declaration::ExpertDeclaration;

pub use crate::expert::declaration::ExpertId;

/// Catalogue des experts connus du cluster, répliqué via Raft (voir
/// `network::cp::state::ControlPlaneState::experts`). Lecture seule depuis
/// l'extérieur (voir [`Deref`]) : toute mutation passe par
/// [`Self::insert`]/[`Self::remove`], appelées uniquement depuis
/// `network::cp::state::apply_request` sur des commandes déjà committées par
/// le cluster — jamais directement.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ExpertCatalog(HashMap<ExpertId, ExpertDeclaration>);

impl Deref for ExpertCatalog {
    type Target = HashMap<ExpertId, ExpertDeclaration>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ExpertCatalog {
    pub fn insert(&mut self, id: ExpertId, declaration: ExpertDeclaration) -> Option<ExpertDeclaration> {
        self.0.insert(id, declaration)
    }

    pub fn remove(&mut self, id: &ExpertId) -> Option<ExpertDeclaration> {
        self.0.remove(id)
    }
}
