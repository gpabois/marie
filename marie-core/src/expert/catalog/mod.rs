#[cfg(feature = "catalog")]
pub mod store;

use std::borrow::Borrow;

use loro::{LoroDoc, ToJson};

pub use super::{Expert, ExpertId};

/// Catalogue des experts connus du cluster, sur le même principe que
/// [`crate::model::catalog::ModelCatalog`] : un état CRDT (`loro`) plutôt
/// qu'une structure locale opaque, pour permettre une fusion décentralisée
/// entre control planes (voir la doc de `ModelCatalog` pour la justification
/// du choix CRDT plutôt qu'un simple log Raft).
pub struct ExpertCatalog {
    state: LoroDoc,
}

impl ExpertCatalog {
    pub fn new() -> ExpertCatalog {
        let state = LoroDoc::new();
        state.get_map("experts");

        Self { state }
    }

    pub fn insert(&mut self, expert: Expert) {
        let key: &str = expert.id.borrow();
        let value = serde_json::to_value(&expert).unwrap();
        let experts = self.state.get_map("experts");
        experts.insert(key, value).unwrap();
    }

    pub fn get(&self, id: &str) -> Option<Expert> {
        let experts = self.state.get_map("experts");
        let value = experts.get(id)?;
        let value = value.as_value()?;
        serde_json::from_value(value.to_json_value()).ok()
    }

    pub fn remove(&mut self, id: &str) -> Option<Expert> {
        let removed = self.get(id);
        let experts = self.state.get_map("experts");
        let _ = experts.delete(id);
        removed
    }

    pub fn list(&self) -> Vec<Expert> {
        let experts = self.state.get_map("experts");
        experts
            .values()
            .filter_map(|value| value.as_value().and_then(|v| serde_json::from_value(v.to_json_value()).ok()))
            .collect()
    }
}
