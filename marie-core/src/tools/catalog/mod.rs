pub mod store;

use std::{collections::HashMap, ops::Deref};

use serde::{Deserialize, Serialize};

use crate::tools::declaration::ToolDeclaration;

pub use crate::tools::declaration::ToolId;

/// Catalogue des tools connus du cluster, répliqué via Raft (voir
/// `network::cp::state::ControlPlaneState::tools`). Lecture seule depuis
/// l'extérieur (voir [`Deref`]) : toute mutation passe par
/// [`Self::insert`]/[`Self::remove`], appelées uniquement depuis
/// `network::cp::state::apply_request` sur des commandes déjà committées par
/// le cluster — jamais directement.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ToolCatalog(HashMap<ToolId, ToolDeclaration>);

impl Deref for ToolCatalog {
    type Target = HashMap<ToolId, ToolDeclaration>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ToolCatalog {
    pub fn insert(&mut self, id: ToolId, declaration: ToolDeclaration) -> Option<ToolDeclaration> {
        self.0.insert(id, declaration)
    }

    pub fn remove(&mut self, id: &ToolId) -> Option<ToolDeclaration> {
        self.0.remove(id)
    }
}
