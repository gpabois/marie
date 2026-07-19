use loro::{LoroDoc, ToJson};

pub use super::{Session, SessionId};

/// Catalogue des sessions connues du nœud qui les sert, sur le même principe
/// que [`crate::expert::catalog::ExpertCatalog`]/[`crate::model::catalog::ModelCatalog`] :
/// un état CRDT (`loro`) plutôt qu'une structure locale opaque, pour
/// permettre une fusion décentralisée entre control planes.
pub struct SessionCatalog {
    state: LoroDoc,
}

impl SessionCatalog {
    pub fn new() -> SessionCatalog {
        let state = LoroDoc::new();
        state.get_map("sessions");

        Self { state }
    }

    pub fn insert(&mut self, session: Session) {
        let key = session.id.to_string();
        let value = serde_json::to_value(&session).unwrap();
        let sessions = self.state.get_map("sessions");
        sessions.insert(&key, value).unwrap();
    }

    pub fn get(&self, id: &str) -> Option<Session> {
        let sessions = self.state.get_map("sessions");
        let value = sessions.get(id)?;
        let value = value.as_value()?;
        serde_json::from_value(value.to_json_value()).ok()
    }

    pub fn remove(&mut self, id: &str) -> Option<Session> {
        let removed = self.get(id);
        let sessions = self.state.get_map("sessions");
        let _ = sessions.delete(id);
        removed
    }

    pub fn list(&self) -> Vec<Session> {
        let sessions = self.state.get_map("sessions");
        sessions
            .values()
            .filter_map(|value| value.as_value().and_then(|v| serde_json::from_value(v.to_json_value()).ok()))
            .collect()
    }
}
