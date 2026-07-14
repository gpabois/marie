use anyhow::bail;
use serde_json::Value;
use yrs::{Any, Array, ArrayPrelim, Doc, Map, MapPrelim, Out, ReadTxn, StateVector, Transact, updates::decoder::Decode};

use crate::{
    agent::context::ContextEntry,
    workspace::{WorkspaceApi, WorkspaceId},
};

/// Workspace porté par un `yrs::Doc`, sur exactement le même principe que
/// `session::crdt::YrsSession` (voir sa doc pour la justification détaillée
/// du choix CRDT) : l'appartenance des sessions et l'état partagé sont
/// amenés à être écrits en continu par plusieurs agents concurrents,
/// potentiellement sur des workers différents — un diff incrémental,
/// échangé directement entre pairs, s'y prête mieux qu'une réplication Raft.
pub struct YrsWorkspace {
    doc: Doc,
    id: WorkspaceId,
    sessions: yrs::ArrayRef,
    context: yrs::ArrayRef,
    state: yrs::MapRef,
}

impl YrsWorkspace {
    /// Crée un workspace vierge.
    pub fn new(id: WorkspaceId) -> Self {
        let doc = Doc::new();
        let root = doc.get_or_insert_map("workspace");

        let mut txn = doc.transact_mut();
        root.insert(&mut txn, "id", id.to_string());
        let sessions = root.insert(&mut txn, "sessions", ArrayPrelim::default());
        let context = root.insert(&mut txn, "context", ArrayPrelim::default());
        let state = root.insert(&mut txn, "state", MapPrelim::default());
        drop(txn);

        Self { doc, id, sessions, context, state }
    }

    /// Reconstruit le handle à partir d'un `Doc` déjà peuplé — typiquement
    /// après application d'un diff reçu d'un pair (voir [`Self::apply_diff`]).
    pub fn open(doc: Doc) -> anyhow::Result<Self> {
        let root = doc.get_or_insert_map("workspace");
        let txn = doc.transact();

        let Some(Out::Any(Any::String(id_str))) = root.get(&txn, "id") else {
            bail!("doc de workspace invalide : champ 'id' manquant ou invalide");
        };
        let id: WorkspaceId = id_str.parse()?;

        let Some(Out::YArray(sessions)) = root.get(&txn, "sessions") else {
            bail!("doc de workspace invalide : champ 'sessions' manquant ou invalide");
        };
        let Some(Out::YArray(context)) = root.get(&txn, "context") else {
            bail!("doc de workspace invalide : champ 'context' manquant ou invalide");
        };
        let Some(Out::YMap(state)) = root.get(&txn, "state") else {
            bail!("doc de workspace invalide : champ 'state' manquant ou invalide");
        };

        drop(txn);
        Ok(Self { doc, id, sessions, context, state })
    }

    /// Construit un workspace à partir d'un diff *complet* (encodé depuis un
    /// vecteur d'état vide, voir [`Self::diff_since`]) — le seul chemin sûr
    /// pour un workspace jamais vu localement (voir la doc équivalente sur
    /// `session::crdt::YrsSession::from_diff` pour la justification).
    pub fn from_diff(diff: &[u8]) -> anyhow::Result<Self> {
        let doc = Doc::new();
        doc.transact_mut().apply_update(yrs::Update::decode_v1(diff)?)?;
        Self::open(doc)
    }

    pub fn doc(&self) -> &Doc {
        &self.doc
    }

    /// Vecteur d'état courant : à envoyer à un pair pour qu'il calcule le
    /// diff qui nous manque (voir [`Self::diff_since`]).
    pub fn state_vector(&self) -> StateVector {
        self.doc.transact().state_vector()
    }

    /// Diff à destination d'un pair dont on connaît le vecteur d'état
    /// (`remote_sv`) — voir `RpcCall::FETCH_WORKSPACE`.
    pub fn diff_since(&self, remote_sv: &StateVector) -> Vec<u8> {
        self.doc.transact().encode_diff_v1(remote_sv)
    }

    /// Applique un diff reçu d'un pair (voir [`Self::diff_since`]).
    pub fn apply_diff(&mut self, diff: &[u8]) -> anyhow::Result<()> {
        let update = yrs::Update::decode_v1(diff)?;
        self.doc.transact_mut().apply_update(update)?;
        Ok(())
    }
}

impl WorkspaceApi for YrsWorkspace {
    fn id(&self) -> WorkspaceId {
        self.id
    }

    fn add_session(&mut self, session_id: crate::session::SessionId) -> anyhow::Result<()> {
        let target = session_id.to_string();
        let mut txn = self.doc.transact_mut();

        let already_present = self.sessions.iter(&txn).any(|out| matches!(out, Out::Any(Any::String(s)) if s.as_ref() == target));
        if !already_present {
            self.sessions.push_back(&mut txn, target);
        }
        Ok(())
    }

    fn remove_session(&mut self, session_id: crate::session::SessionId) -> anyhow::Result<()> {
        let target = session_id.to_string();
        let mut txn = self.doc.transact_mut();

        let index = self.sessions.iter(&txn).position(|out| matches!(out, Out::Any(Any::String(s)) if s.as_ref() == target));
        if let Some(index) = index {
            self.sessions.remove(&mut txn, index as u32);
        }
        Ok(())
    }

    fn sessions(&self) -> Vec<crate::session::SessionId> {
        let txn = self.doc.transact();
        self.sessions
            .iter(&txn)
            .filter_map(|out| match out {
                Out::Any(Any::String(s)) => s.parse().ok(),
                _ => None,
            })
            .collect()
    }

    fn push_context_entry(&mut self, entry: &ContextEntry) -> anyhow::Result<()> {
        let json = to_json(entry)?;
        let mut txn = self.doc.transact_mut();
        self.context.push_back(&mut txn, json);
        Ok(())
    }

    fn context(&self) -> Vec<ContextEntry> {
        let txn = self.doc.transact();
        self.context
            .iter(&txn)
            .filter_map(|out| match out {
                Out::Any(Any::String(json)) => from_json(&json).ok(),
                _ => None,
            })
            .collect()
    }

    fn set_value(&mut self, key: &str, value: &Value) -> anyhow::Result<()> {
        let json = serde_json::to_string(value)?;
        let mut txn = self.doc.transact_mut();
        self.state.insert(&mut txn, key, json);
        Ok(())
    }

    fn remove_value(&mut self, key: &str) -> anyhow::Result<()> {
        let mut txn = self.doc.transact_mut();
        self.state.remove(&mut txn, key);
        Ok(())
    }

    fn value(&self, key: &str) -> Option<Value> {
        let txn = self.doc.transact();
        match self.state.get(&txn, key) {
            Some(Out::Any(Any::String(json))) => serde_json::from_str(&json).ok(),
            _ => None,
        }
    }

    fn values(&self) -> std::collections::HashMap<String, Value> {
        let txn = self.doc.transact();
        self.state
            .iter(&txn)
            .filter_map(|(key, out)| match out {
                Out::Any(Any::String(json)) => serde_json::from_str::<Value>(&json).ok().map(|value| (key.to_string(), value)),
                _ => None,
            })
            .collect()
    }
}

fn to_json(value: &impl serde::Serialize) -> anyhow::Result<String> {
    Ok(serde_json::to_string(value)?)
}

fn from_json<T: serde::de::DeserializeOwned>(json: &str) -> anyhow::Result<T> {
    Ok(serde_json::from_str(json)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{agent::role::Role, id::IdGenerator};

    #[test]
    fn test_add_session_is_idempotent() {
        let ids = IdGenerator::default();
        let mut workspace = YrsWorkspace::new(ids.next_id());
        let session_id = ids.next_id();

        workspace.add_session(session_id).unwrap();
        workspace.add_session(session_id).unwrap();

        assert_eq!(workspace.sessions(), vec![session_id]);
    }

    #[test]
    fn test_remove_session_no_op_if_absent() {
        let ids = IdGenerator::default();
        let mut workspace = YrsWorkspace::new(ids.next_id());
        assert!(workspace.remove_session(ids.next_id()).is_ok());
        assert!(workspace.sessions().is_empty());
    }

    #[test]
    fn test_add_then_remove_session() {
        let ids = IdGenerator::default();
        let mut workspace = YrsWorkspace::new(ids.next_id());
        let a = ids.next_id();
        let b = ids.next_id();

        workspace.add_session(a).unwrap();
        workspace.add_session(b).unwrap();
        workspace.remove_session(a).unwrap();

        assert_eq!(workspace.sessions(), vec![b]);
    }

    #[test]
    fn test_shared_context_round_trip() {
        let ids = IdGenerator::default();
        let mut workspace = YrsWorkspace::new(ids.next_id());

        workspace.push_context_entry(&ContextEntry { role: Role::User, content: "bonjour".to_string() }).unwrap();
        workspace.push_context_entry(&ContextEntry { role: Role::Assistant, content: "salut !".to_string() }).unwrap();

        let context = workspace.context();
        assert_eq!(context.len(), 2);
        assert_eq!(context[1].content, "salut !");
    }

    #[test]
    fn test_kv_store_set_get_remove() {
        let ids = IdGenerator::default();
        let mut workspace = YrsWorkspace::new(ids.next_id());

        workspace.set_value("budget", &Value::from(42)).unwrap();
        assert_eq!(workspace.value("budget"), Some(Value::from(42)));

        workspace.set_value("budget", &Value::from(43)).unwrap();
        assert_eq!(workspace.value("budget"), Some(Value::from(43)));

        workspace.remove_value("budget").unwrap();
        assert_eq!(workspace.value("budget"), None);
    }

    #[test]
    fn test_sync_via_diff() {
        let ids = IdGenerator::default();
        let mut owner = YrsWorkspace::new(ids.next_id());
        let session_id = ids.next_id();
        owner.add_session(session_id).unwrap();
        owner.set_value("k", &Value::from("v1")).unwrap();

        let remote_sv = StateVector::default();
        let diff = owner.diff_since(&remote_sv);

        let remote_doc = Doc::new();
        remote_doc.transact_mut().apply_update(yrs::Update::decode_v1(&diff).unwrap()).unwrap();
        let mut receiver = YrsWorkspace::open(remote_doc).unwrap();

        assert_eq!(receiver.sessions(), vec![session_id]);
        assert_eq!(receiver.value("k"), Some(Value::from("v1")));

        owner.set_value("k", &Value::from("v2")).unwrap();
        let diff2 = owner.diff_since(&receiver.state_vector());
        receiver.apply_diff(&diff2).unwrap();

        assert_eq!(receiver.value("k"), Some(Value::from("v2")));
    }

    #[test]
    fn test_open_round_trip() {
        let ids = IdGenerator::default();
        let workspace_id = ids.next_id();
        let mut workspace = YrsWorkspace::new(workspace_id);
        workspace.add_session(ids.next_id()).unwrap();

        let diff = workspace.diff_since(&StateVector::default());
        let doc = Doc::new();
        doc.transact_mut().apply_update(yrs::Update::decode_v1(&diff).unwrap()).unwrap();

        let reopened = YrsWorkspace::open(doc).unwrap();
        assert_eq!(reopened.id(), workspace_id);
    }
}
