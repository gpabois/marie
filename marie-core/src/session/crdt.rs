use anyhow::bail;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use crate::id::ID;
use yrs::{
    Any, Array, ArrayPrelim, Doc, GetString, Map, MapPrelim, Out, ReadTxn, StateVector, Text,
    TextPrelim, Transact, updates::decoder::Decode,
};

use crate::{
    agent::{context::ContextEntry, frame::AgentFrame, status::AgentStatus},
    mode::SessionMode,
    session::{SessionApi, SessionId, SessionLog},
};

/// Session portée par un `yrs::Doc` plutôt que par de simples structs Rust.
///
/// Un job `RunAgent` n'est pas garanti de s'exécuter deux fois sur le même
/// worker (réassignation après un healthcheck manqué, voir
/// `network::cp::reconcile`) : le frame et son historique doivent donc
/// pouvoir voyager d'un pair à l'autre. Contrairement au job lui-même (dont
/// seule l'affectation a besoin de consensus Raft, voir
/// `ControlPlaneState::sessions`), ce contenu est volumineux et écrit en
/// continu (contexte, stdio/stderr streamés) : un diff CRDT incrémental,
/// échangé directement entre le pair qui détient la session et celui qui la
/// reprend, s'y prête mieux qu'une réplication Raft ou qu'une copie
/// intégrale à chaque réassignation.
pub struct YrsSession {
    doc: Doc,
    id: SessionId,
    frames: yrs::MapRef,
    logs: yrs::ArrayRef,
    /// Pile des [`SessionMode`] de la session (voir [`Self::push_mode`]/
    /// [`Self::pop_mode`]) — le sommet (dernier élément) est le mode
    /// courant ; une pile vide signifie [`SessionMode::Simple`] (voir
    /// [`Self::current_mode`]), jamais stocké explicitement.
    modes: yrs::ArrayRef,
    /// Store clé-valeur libre de la session, sur exactement le même principe
    /// que `workspace::crdt::YrsWorkspace::state` — c'est le backend de
    /// `/session/var` dans le VFS (voir `persistency::var::VarFileSystem`).
    state: yrs::MapRef,
}

impl YrsSession {
    /// Crée une session vierge, pour un worker qui prend en charge un
    /// `GlobalAgentId` dont aucun pair connu n'a encore la trace (voir
    /// `ControlPlaneState::sessions`). N'est appelé qu'après vérification que
    /// `id` est déjà rattaché à un workspace (voir
    /// `session::client::SessionClient::acquire` et
    /// `workspace::client::WorkspaceClient::create_session`, seul point de
    /// création d'un `SessionId`) — `YrsSession::new` lui-même ne le
    /// vérifie pas, il fait confiance à son appelant.
    pub fn new(id: SessionId) -> Self {
        let doc = Doc::new();
        let root = doc.get_or_insert_map("session");

        let mut txn = doc.transact_mut();
        root.insert(&mut txn, "id", id.to_string());
        let frames = root.insert(&mut txn, "frames", MapPrelim::default());
        let logs = root.insert(&mut txn, "logs", ArrayPrelim::default());
        let modes = root.insert(&mut txn, "modes", ArrayPrelim::default());
        let state = root.insert(&mut txn, "state", MapPrelim::default());
        drop(txn);

        Self { doc, id, frames, logs, modes, state }
    }

    /// Reconstruit le handle à partir d'un `Doc` déjà peuplé — typiquement
    /// après application d'un diff reçu d'un pair (voir [`Self::apply_diff`]).
    pub fn open(doc: Doc) -> anyhow::Result<Self> {
        let root = doc.get_or_insert_map("session");
        let txn = doc.transact();

        let Some(Out::Any(Any::String(id_str))) = root.get(&txn, "id") else {
            bail!("doc de session invalide : champ 'id' manquant ou invalide");
        };
        let id: SessionId = id_str.parse()?;

        let Some(Out::YMap(frames)) = root.get(&txn, "frames") else {
            bail!("doc de session invalide : champ 'frames' manquant ou invalide");
        };
        let Some(Out::YArray(logs)) = root.get(&txn, "logs") else {
            bail!("doc de session invalide : champ 'logs' manquant ou invalide");
        };
        let Some(Out::YArray(modes)) = root.get(&txn, "modes") else {
            bail!("doc de session invalide : champ 'modes' manquant ou invalide");
        };
        let Some(Out::YMap(state)) = root.get(&txn, "state") else {
            bail!("doc de session invalide : champ 'state' manquant ou invalide");
        };

        drop(txn);
        Ok(Self { doc, id, frames, logs, modes, state })
    }

    /// Construit une session à partir d'un diff *complet* (encodé depuis un
    /// vecteur d'état vide, voir [`Self::diff_since`]) — équivalent à
    /// `Doc::new()` + `apply_update` + [`Self::open`], le seul chemin sûr
    /// pour une session jamais vue localement : appliquer un diff sur une
    /// session fraîchement créée via [`Self::new`] créerait une racine "session"
    /// concurrente à celle du diff plutôt que de la rejoindre (les deux
    /// définissent indépendamment "id"/"frames"/"logs", départagés par un
    /// tie-break de dernière écriture qui ne garantit pas de conserver ceux
    /// qu'on vient de créer).
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
    /// (`remote_sv`) — voir `RpcCall::FETCH_SESSION`.
    pub fn diff_since(&self, remote_sv: &StateVector) -> Vec<u8> {
        self.doc.transact().encode_diff_v1(remote_sv)
    }

    /// Applique un diff reçu d'un pair (voir [`Self::diff_since`]).
    pub fn apply_diff(&mut self, diff: &[u8]) -> anyhow::Result<()> {
        let update = yrs::Update::decode_v1(diff)?;
        self.doc.transact_mut().apply_update(update)?;
        Ok(())
    }

    fn frame_map(&self, txn: &impl ReadTxn, local_id: ID) -> Option<yrs::MapRef> {
        match self.frames.get(txn, &local_id.to_string()) {
            Some(Out::YMap(map)) => Some(map),
            _ => None,
        }
    }

    fn append_text_field(&mut self, local_id: ID, field: &str, chunk: &str) -> anyhow::Result<()> {
        let mut txn = self.doc.transact_mut();
        let Some(map) = self.frame_map(&txn, local_id) else {
            bail!("frame inconnu de cette session : {local_id}");
        };
        let Some(Out::YText(text)) = map.get(&txn, field) else {
            bail!("champ '{field}' invalide pour le frame {local_id}");
        };
        text.push(&mut txn, chunk);
        Ok(())
    }
}

impl SessionApi for YrsSession {
    fn id(&self) -> SessionId {
        self.id
    }

    /// Enregistre l'état intégral d'un frame — utilisé à la prise en charge
    /// initiale d'un frame que ce worker n'a encore jamais vu.
    fn put_frame(&mut self, local_id: ID, frame: &AgentFrame) -> anyhow::Result<()> {
        let status_json = to_json(&frame.status)?;

        let mut txn = self.doc.transact_mut();
        let map = self.frames.insert(&mut txn, local_id.to_string(), MapPrelim::default());

        map.insert(&mut txn, "model_id", frame.model_id.clone());
        map.insert(&mut txn, "status", status_json);

        let allowed_tools = map.insert(&mut txn, "allowed_tools", ArrayPrelim::default());
        for tool in &frame.allowed_tools {
            allowed_tools.push_back(&mut txn, tool.clone());
        }

        let context = map.insert(&mut txn, "context", ArrayPrelim::default());
        for entry in frame.context.iter() {
            context.push_back(&mut txn, to_json(entry)?);
        }

        map.insert(&mut txn, "stdio", TextPrelim::new(frame.stdio.clone()));
        map.insert(&mut txn, "stderr", TextPrelim::new(frame.stderr.clone()));

        Ok(())
    }

    /// Reconstruit un frame à partir de son état synchronisé, s'il est connu
    /// localement.
    fn frame(&self, local_id: ID) -> Option<AgentFrame> {
        let txn = self.doc.transact();
        let map = self.frame_map(&txn, local_id)?;

        let Some(Out::Any(Any::String(model_id))) = map.get(&txn, "model_id") else {
            return None;
        };

        let status = match map.get(&txn, "status") {
            Some(Out::Any(Any::String(json))) => from_json(&json).unwrap_or_default(),
            _ => AgentStatus::default(),
        };

        let allowed_tools = match map.get(&txn, "allowed_tools") {
            Some(Out::YArray(array)) => array
                .iter(&txn)
                .filter_map(|out| match out {
                    Out::Any(Any::String(s)) => Some(s.to_string()),
                    _ => None,
                })
                .collect(),
            _ => vec![],
        };

        let context: Vec<ContextEntry> = match map.get(&txn, "context") {
            Some(Out::YArray(array)) => array
                .iter(&txn)
                .filter_map(|out| match out {
                    Out::Any(Any::String(json)) => from_json(&json).ok(),
                    _ => None,
                })
                .collect(),
            _ => vec![],
        };

        let stdio = match map.get(&txn, "stdio") {
            Some(Out::YText(text)) => text.get_string(&txn),
            _ => String::new(),
        };
        let stderr = match map.get(&txn, "stderr") {
            Some(Out::YText(text)) => text.get_string(&txn),
            _ => String::new(),
        };

        Some(AgentFrame {
            session_id: self.id,
            id: local_id,
            model_id: model_id.to_string(),
            status,
            allowed_tools,
            context: context.into(),
            stdio,
            stderr,
        })
    }

    /// Remplace le statut d'un frame connu (transition de cycle de vie, voir
    /// [`AgentStatus`]).
    fn set_status(&mut self, local_id: ID, status: &AgentStatus) -> anyhow::Result<()> {
        let status_json = to_json(status)?;

        let mut txn = self.doc.transact_mut();
        let Some(map) = self.frame_map(&txn, local_id) else {
            bail!("frame inconnu de cette session : {local_id}");
        };
        map.insert(&mut txn, "status", status_json);
        Ok(())
    }

    /// Ajoute une entrée au contexte d'un frame connu (nouveau message
    /// échangé avec le modèle).
    fn push_context_entry(&mut self, local_id: ID, entry: &ContextEntry) -> anyhow::Result<()> {
        let entry_json = to_json(entry)?;

        let mut txn = self.doc.transact_mut();
        let Some(map) = self.frame_map(&txn, local_id) else {
            bail!("frame inconnu de cette session : {local_id}");
        };
        let Some(Out::YArray(context)) = map.get(&txn, "context") else {
            bail!("champ 'context' invalide pour le frame {local_id}");
        };
        context.push_back(&mut txn, entry_json);
        Ok(())
    }

    /// Ajoute `chunk` à la sortie standard streamée d'un frame connu.
    fn append_stdio(&mut self, local_id: ID, chunk: &str) -> anyhow::Result<()> {
        self.append_text_field(local_id, "stdio", chunk)
    }

    /// Ajoute `chunk` à la sortie d'erreur streamée d'un frame connu.
    fn append_stderr(&mut self, local_id: ID, chunk: &str) -> anyhow::Result<()> {
        self.append_text_field(local_id, "stderr", chunk)
    }

    /// Ajoute une entrée au journal de la session (voir [`SessionLog`]).
    fn push_log(&mut self, log: &SessionLog) -> anyhow::Result<()> {
        let log_json = to_json(log)?;
        let mut txn = self.doc.transact_mut();
        self.logs.push_back(&mut txn, log_json);
        Ok(())
    }

    /// Journal complet de la session, dans l'ordre d'ajout.
    fn logs(&self) -> Vec<SessionLog> {
        let txn = self.doc.transact();
        self.logs
            .iter(&txn)
            .filter_map(|out| match out {
                Out::Any(Any::String(json)) => from_json(&json).ok(),
                _ => None,
            })
            .collect()
    }

    /// Empile `mode` au sommet de la pile de modes de la session (voir
    /// `YrsSession::modes`). Rejette [`SessionMode::Simple`] : c'est le mode
    /// implicite d'une pile vide, il n'y a rien à empiler pour y revenir —
    /// [`Self::pop_mode`] suffit.
    fn push_mode(&mut self, mode: &SessionMode) -> anyhow::Result<()> {
        if matches!(mode, SessionMode::Simple) {
            bail!("le mode 'simple' ne se pousse pas : dépiler (pop_mode) suffit à y revenir");
        }

        let mode_json = to_json(mode)?;
        let mut txn = self.doc.transact_mut();
        self.modes.push_back(&mut txn, mode_json);
        Ok(())
    }

    /// Remplace le mode au sommet de la pile par `mode`, sans changer la
    /// profondeur de la pile (contrairement à [`Self::push_mode`]/
    /// [`Self::pop_mode`]) — pour persister la *progression* d'un mode déjà
    /// empilé (ex: `mode::state_graph::StateGraph::current` après un
    /// `advance`, voir `network::worker::mod::drive_state_graph`), pas pour
    /// en empiler un nouveau. Échoue s'il n'y a rien à remplacer (pile vide)
    /// — voir [`Self::push_mode`] pour empiler un premier mode. Comme
    /// [`Self::push_mode`], rejette [`SessionMode::Simple`].
    fn update_current_mode(&mut self, mode: &SessionMode) -> anyhow::Result<()> {
        if matches!(mode, SessionMode::Simple) {
            bail!("le mode 'simple' ne se pousse pas : dépiler (pop_mode) suffit à y revenir");
        }

        let mode_json = to_json(mode)?;
        let mut txn = self.doc.transact_mut();
        let len = self.modes.len(&txn);
        let Some(last_index) = len.checked_sub(1) else {
            bail!("aucun mode empilé à mettre à jour (voir push_mode)");
        };
        self.modes.remove(&mut txn, last_index);
        self.modes.insert(&mut txn, last_index, mode_json);
        Ok(())
    }

    /// Dépile et retourne le mode au sommet de la pile, ou `None` si elle
    /// est déjà vide.
    fn pop_mode(&mut self) -> anyhow::Result<Option<SessionMode>> {
        let mut txn = self.doc.transact_mut();
        let len = self.modes.len(&txn);
        let Some(last_index) = len.checked_sub(1) else {
            return Ok(None);
        };

        let popped = match self.modes.get(&txn, last_index) {
            Some(Out::Any(Any::String(json))) => Some(from_json(&json)?),
            _ => None,
        };
        self.modes.remove(&mut txn, last_index);

        Ok(popped)
    }

    /// Mode au sommet de la pile — [`SessionMode::Simple`] si elle est vide.
    fn current_mode(&self) -> SessionMode {
        let txn = self.doc.transact();
        let len = self.modes.len(&txn);
        let Some(last_index) = len.checked_sub(1) else {
            return SessionMode::Simple;
        };

        match self.modes.get(&txn, last_index) {
            Some(Out::Any(Any::String(json))) => from_json(&json).unwrap_or(SessionMode::Simple),
            _ => SessionMode::Simple,
        }
    }

    /// Pile complète des modes, du fond (index 0) au sommet — pour
    /// inspection/debug ; [`Self::current_mode`] suffit au pilotage normal.
    fn mode_stack(&self) -> Vec<SessionMode> {
        let txn = self.doc.transact();
        self.modes
            .iter(&txn)
            .filter_map(|out| match out {
                Out::Any(Any::String(json)) => from_json(&json).ok(),
                _ => None,
            })
            .collect()
    }

    /// Définit (crée ou remplace) une valeur du store clé-valeur libre de la
    /// session — voir `workspace::crdt::YrsWorkspace::set_value`, backend de
    /// `/session/var` dans le VFS (voir `persistency::var::VarFileSystem`).
    fn set_value(&mut self, key: &str, value: &Value) -> anyhow::Result<()> {
        let json = serde_json::to_string(value)?;
        let mut txn = self.doc.transact_mut();
        self.state.insert(&mut txn, key, json);
        Ok(())
    }

    /// Retire une clé du store clé-valeur libre — sans effet si elle n'existe
    /// pas.
    fn remove_value(&mut self, key: &str) -> anyhow::Result<()> {
        let mut txn = self.doc.transact_mut();
        self.state.remove(&mut txn, key);
        Ok(())
    }

    /// Valeur associée à `key` dans le store clé-valeur libre, si connue.
    fn value(&self, key: &str) -> Option<Value> {
        let txn = self.doc.transact();
        match self.state.get(&txn, key) {
            Some(Out::Any(Any::String(json))) => serde_json::from_str(&json).ok(),
            _ => None,
        }
    }

    /// Snapshot complet du store clé-valeur libre.
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

fn to_json(value: &impl Serialize) -> anyhow::Result<String> {
    Ok(serde_json::to_string(value)?)
}

fn from_json<T: DeserializeOwned>(json: &str) -> anyhow::Result<T> {
    Ok(serde_json::from_str(json)?)
}

#[cfg(test)]
mod tests {
    use crate::id::IdGenerator;

    use super::*;
    use crate::agent::role::Role;

    fn frame(session_id: ID, local_id: ID) -> AgentFrame {
        AgentFrame {
            session_id,
            id: local_id,
            model_id: "gpt-test".to_string(),
            status: AgentStatus::Initial,
            allowed_tools: vec!["search".to_string()],
            context: vec![ContextEntry { role: Role::User, content: "bonjour".to_string() }].into(),
            stdio: String::new(),
            stderr: String::new(),
        }
    }

    #[test]
    fn test_put_and_read_frame() {
        let ids = IdGenerator::default();
        let session_id = ids.next_id();
        let local_id = ids.next_id();

        let mut session = YrsSession::new(session_id);
        session.put_frame(local_id, &frame(session_id, local_id)).unwrap();
        session.append_stdio(local_id, "salut").unwrap();
        session
            .push_context_entry(local_id, &ContextEntry { role: Role::Assistant, content: "salut !".to_string() })
            .unwrap();

        let got = session.frame(local_id).unwrap();
        assert_eq!(got.model_id, "gpt-test");
        assert_eq!(got.stdio, "salut");
        assert_eq!(got.context.len(), 2);
        assert_eq!(got.context[1].content, "salut !");
    }

    #[test]
    fn test_sync_via_diff() {
        let ids = IdGenerator::default();
        let session_id = ids.next_id();
        let local_id = ids.next_id();

        let mut owner = YrsSession::new(session_id);
        owner.put_frame(local_id, &frame(session_id, local_id)).unwrap();
        owner.append_stdio(local_id, "partie 1").unwrap();

        // Le nouveau worker part d'un vecteur d'état vide (jamais vu cette session) :
        // il ne doit pas appeler `new` (qui créerait sa propre racine, en conflit avec
        // celle reçue) mais reconstruire son handle depuis un `Doc` vierge une fois le
        // diff appliqué — voir `YrsSession::open`.
        let remote_sv = StateVector::default();
        let diff = owner.diff_since(&remote_sv);

        let remote_doc = Doc::new();
        remote_doc.transact_mut().apply_update(yrs::Update::decode_v1(&diff).unwrap()).unwrap();
        let mut receiver = YrsSession::open(remote_doc).unwrap();

        let got = receiver.frame(local_id).unwrap();
        assert_eq!(got.stdio, "partie 1");

        // Nouvelle écriture côté propriétaire d'origine : seul le delta doit transiter.
        owner.append_stdio(local_id, " partie 2").unwrap();
        let diff2 = owner.diff_since(&receiver.state_vector());
        receiver.apply_diff(&diff2).unwrap();

        assert_eq!(receiver.frame(local_id).unwrap().stdio, "partie 1 partie 2");
    }

    #[test]
    fn test_open_round_trip() {
        let ids = IdGenerator::default();
        let session_id = ids.next_id();
        let local_id = ids.next_id();

        let mut session = YrsSession::new(session_id);
        session.put_frame(local_id, &frame(session_id, local_id)).unwrap();

        let diff = session.diff_since(&StateVector::default());
        let doc = Doc::new();
        doc.transact_mut().apply_update(yrs::Update::decode_v1(&diff).unwrap()).unwrap();

        let reopened = YrsSession::open(doc).unwrap();
        assert_eq!(reopened.id(), session_id);
        assert_eq!(reopened.frame(local_id).unwrap().model_id, "gpt-test");
    }

    #[test]
    fn test_current_mode_defaults_to_simple() {
        let session = YrsSession::new(IdGenerator::default().next_id());
        assert_eq!(session.current_mode(), crate::mode::SessionMode::Simple);
    }

    #[test]
    fn test_push_mode_rejects_simple() {
        let mut session = YrsSession::new(IdGenerator::default().next_id());
        assert!(session.push_mode(&crate::mode::SessionMode::Simple).is_err());
    }

    #[test]
    fn test_push_then_pop_mode_round_trip() {
        use crate::mode::orchestration::{Orchestration, OrchestrationStrategy};

        let mut session = YrsSession::new(IdGenerator::default().next_id());
        let mode = crate::mode::SessionMode::Orchestration(Orchestration::new(OrchestrationStrategy::Sequential));

        session.push_mode(&mode).unwrap();
        assert_eq!(session.current_mode(), mode);
        assert_eq!(session.mode_stack(), vec![mode.clone()]);

        let popped = session.pop_mode().unwrap();
        assert_eq!(popped, Some(mode));
        assert_eq!(session.current_mode(), crate::mode::SessionMode::Simple);
        assert!(session.mode_stack().is_empty());
    }

    #[test]
    fn test_pop_mode_on_empty_stack_returns_none() {
        let mut session = YrsSession::new(IdGenerator::default().next_id());
        assert_eq!(session.pop_mode().unwrap(), None);
    }

    #[test]
    fn test_update_current_mode_replaces_top_without_changing_depth() {
        use crate::mode::orchestration::{Orchestration, OrchestrationStrategy};

        let mut session = YrsSession::new(IdGenerator::default().next_id());
        let initial = crate::mode::SessionMode::Orchestration(Orchestration::new(OrchestrationStrategy::Sequential));
        session.push_mode(&initial).unwrap();

        let mut updated_orchestration = Orchestration::new(OrchestrationStrategy::Sequential);
        updated_orchestration.add_child(crate::agent::GlobalAgentId::new(IdGenerator::default().next_id(), IdGenerator::default().next_id()));
        let updated = crate::mode::SessionMode::Orchestration(updated_orchestration);

        session.update_current_mode(&updated).unwrap();

        assert_eq!(session.current_mode(), updated);
        assert_eq!(session.mode_stack().len(), 1);
    }

    #[test]
    fn test_update_current_mode_fails_on_empty_stack() {
        use crate::mode::orchestration::{Orchestration, OrchestrationStrategy};

        let mut session = YrsSession::new(IdGenerator::default().next_id());
        let mode = crate::mode::SessionMode::Orchestration(Orchestration::new(OrchestrationStrategy::Sequential));

        assert!(session.update_current_mode(&mode).is_err());
    }

    #[test]
    fn test_mode_stack_survives_diff_round_trip() {
        use crate::mode::orchestration::{Orchestration, OrchestrationStrategy};

        let mut owner = YrsSession::new(IdGenerator::default().next_id());
        let mode = crate::mode::SessionMode::Orchestration(Orchestration::new(OrchestrationStrategy::Parallel));
        owner.push_mode(&mode).unwrap();

        let diff = owner.diff_since(&StateVector::default());
        let doc = Doc::new();
        doc.transact_mut().apply_update(yrs::Update::decode_v1(&diff).unwrap()).unwrap();
        let receiver = YrsSession::open(doc).unwrap();

        assert_eq!(receiver.current_mode(), mode);
    }
}
