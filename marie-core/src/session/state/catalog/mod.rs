pub mod store;

use std::borrow::Borrow;

use loro::{LoroDoc, ToJson};

pub use crate::session::state::declaration::StateGraphId;
use crate::session::state::declaration::StateGraphDeclaration;

/// Catalogue des graphes d'états connus du nœud qui les sert, sur le même
/// principe que [`crate::expert::catalog::ExpertCatalog`]/
/// [`crate::model::catalog::ModelCatalog`] : un état CRDT (`loro`) hébergé sur
/// un pair choisi par hash-ring (voir
/// [`crate::session::state::client::StateGraphClient::select_catalog`]),
/// plutôt qu'une réplication Raft.
pub struct StateGraphCatalog {
    state: LoroDoc,
}

impl StateGraphCatalog {
    pub fn new() -> StateGraphCatalog {
        let state = LoroDoc::new();
        state.get_map("state_graphs");

        Self { state }
    }

    pub fn insert(&mut self, id: StateGraphId, declaration: StateGraphDeclaration) {
        let key = id.to_string();
        let value = serde_json::to_value(&declaration).unwrap();
        let graphs = self.state.get_map("state_graphs");
        graphs.insert(&key, value).unwrap();
    }

    pub fn get(&self, id: &str) -> Option<StateGraphDeclaration> {
        let graphs = self.state.get_map("state_graphs");
        let value = graphs.get(id)?;
        let value = value.as_value()?;
        serde_json::from_value(value.to_json_value()).ok()
    }

    pub fn remove(&mut self, id: &str) -> Option<StateGraphDeclaration> {
        let removed = self.get(id);
        let graphs = self.state.get_map("state_graphs");
        let _ = graphs.delete(id);
        removed
    }

    pub fn list(&self) -> Vec<StateGraphDeclaration> {
        let graphs = self.state.get_map("state_graphs");
        graphs
            .values()
            .filter_map(|value| value.as_value().and_then(|v| serde_json::from_value(v.to_json_value()).ok()))
            .collect()
    }
}

impl Default for StateGraphCatalog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn declaration() -> StateGraphDeclaration {
        StateGraphDeclaration {
            nodes: vec![crate::session::state::Node::new("start", None)],
            edges: vec![],
            entry: "start".to_string(),
        }
    }

    #[test]
    fn test_insert_then_get() {
        let mut catalog = StateGraphCatalog::new();
        let id = StateGraphId::new("greeting");
        catalog.insert(id.clone(), declaration());

        assert_eq!(catalog.get(id.borrow()), Some(declaration()));
    }

    #[test]
    fn test_get_unknown_returns_none() {
        let catalog = StateGraphCatalog::new();
        assert_eq!(catalog.get("missing"), None);
    }

    #[test]
    fn test_remove_returns_previous_value() {
        let mut catalog = StateGraphCatalog::new();
        let id = StateGraphId::new("greeting");
        catalog.insert(id.clone(), declaration());

        assert_eq!(catalog.remove(id.borrow()), Some(declaration()));
        assert_eq!(catalog.get(id.borrow()), None);
    }

    #[test]
    fn test_list_returns_all_entries() {
        let mut catalog = StateGraphCatalog::new();
        catalog.insert(StateGraphId::new("a"), declaration());
        catalog.insert(StateGraphId::new("b"), declaration());

        assert_eq!(catalog.list().len(), 2);
    }
}
