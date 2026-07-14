pub mod store;

use std::{collections::HashMap, ops::Deref};

use serde::{Deserialize, Serialize};

use crate::mode::state_graph::declaration::StateGraphDeclaration;

pub use crate::mode::state_graph::declaration::StateGraphId;

/// Catalogue des graphes d'états connus du cluster, répliqué via Raft (voir
/// `network::cp::state::ControlPlaneState::state_graphs`). Lecture seule
/// depuis l'extérieur (voir [`Deref`]) : toute mutation passe par
/// [`Self::insert`]/[`Self::remove`], appelées uniquement depuis
/// `network::cp::state::apply_request` sur des commandes déjà committées par
/// le cluster — jamais directement.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StateGraphCatalog(HashMap<StateGraphId, StateGraphDeclaration>);

impl Deref for StateGraphCatalog {
    type Target = HashMap<StateGraphId, StateGraphDeclaration>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl StateGraphCatalog {
    pub fn insert(&mut self, id: StateGraphId, declaration: StateGraphDeclaration) -> Option<StateGraphDeclaration> {
        self.0.insert(id, declaration)
    }

    pub fn remove(&mut self, id: &StateGraphId) -> Option<StateGraphDeclaration> {
        self.0.remove(id)
    }
}
