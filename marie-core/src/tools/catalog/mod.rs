use std::borrow::Borrow;

use loro::{LoroDoc, ToJson};

pub use super::{Tool, ToolId};

/// Catalogue des tools connus du cluster, sur le même principe que
/// [`crate::model::catalog::ModelCatalog`]/[`crate::expert::catalog::ExpertCatalog`] :
/// un état CRDT (`loro`) plutôt qu'une structure locale opaque, pour
/// permettre une fusion décentralisée entre control planes.
pub struct ToolCatalog {
    state: LoroDoc,
}

impl ToolCatalog {
    pub fn new() -> ToolCatalog {
        let state = LoroDoc::new();
        state.get_map("tools");

        Self { state }
    }

    pub fn insert(&mut self, id: ToolId, tool: Tool) {
        let key: &str = id.borrow();
        let value = serde_json::to_value(&tool).unwrap();
        let tools = self.state.get_map("tools");
        tools.insert(key, value).unwrap();
    }

    pub fn get(&self, id: &str) -> Option<Tool> {
        let tools = self.state.get_map("tools");
        let value = tools.get(id)?;
        let value = value.as_value()?;
        serde_json::from_value(value.to_json_value()).ok()
    }

    pub fn remove(&mut self, id: &str) -> Option<Tool> {
        let removed = self.get(id);
        let tools = self.state.get_map("tools");
        let _ = tools.delete(id);
        removed
    }

    pub fn list(&self) -> Vec<Tool> {
        let tools = self.state.get_map("tools");
        tools
            .values()
            .filter_map(|value| value.as_value().and_then(|v| serde_json::from_value(v.to_json_value()).ok()))
            .collect()
    }
}
