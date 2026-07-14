use std::io::Cursor;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use libp2p::PeerId;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{Entry, EntryPayload, LogId, Snapshot, SnapshotMeta, StorageError, StorageIOError, StoredMembership};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::warn;

use crate::{
    expert::{
        catalog::{ExpertCatalog, ExpertId, store::{ExpertStore, StoredExpert}},
        declaration::ExpertDeclaration,
    },
    job::{Job, JobId, JobKind, JobState},
    mode::state_graph::{
        catalog::{StateGraphCatalog, StateGraphId, store::{StateGraphStore, StoredStateGraph}},
        declaration::StateGraphDeclaration,
    },
    model::{
        catalog::{
            ModelCatalog, ModelId,
            store::{ModelStore, StoredModel, encrypt_for_storage},
        },
        declaration::Model,
    },
    network::{
        cp::types::{ControlPlaneRequest, ControlPlaneResponse, RaftNode, RaftNodeId, TypeConfig},
        worker::info::WorkerInfo,
    },
    secret::SecretManager,
    session::SessionId,
    tools::{
        catalog::{ToolCatalog, ToolId, store::{StoredTool, ToolStore}},
        declaration::ToolDeclaration,
    },
    workspace::WorkspaceId,
};

/// Définition d'un job (ce qu'il faut exécuter) + son état de cycle de vie.
/// La définition est immuable après soumission ; seul `state` change au fil
/// des transitions (`AssignJob`, `CommitState`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRecord {
    pub job: Job,
    pub state: JobState,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ControlPlaneState {
    pub jobs: HashMap<JobId, JobRecord>,
    pub models: ModelCatalog,
    pub tools: ToolCatalog,
    pub experts: ExpertCatalog,
    pub state_graphs: StateGraphCatalog,
    pub workers: HashMap<PeerId, WorkerInfo>,
    /// Nœuds `Persistency` connus (voir `network::persistency`) — détenteurs
    /// de secours pour toute session, ajoutés en fin de liste par
    /// `network::cp::session_holders_for` (servie via
    /// `RpcCall::SESSION_HOLDERS`) : les workers vivants sont essayés en
    /// premier, ce nœud durable en dernier recours (ex: reprise d'une
    /// session sans job actif, ou après un redémarrage complet du cluster).
    pub persistency_nodes: HashSet<PeerId>,
    /// Workspace auquel appartient chaque session qui en a un (voir
    /// `RpcCall::SET_SESSION_WORKSPACE`) — une session absente de cette map
    /// n'appartient à aucun workspace. Sert uniquement à dériver les
    /// détenteurs d'un workspace (voir `network::cp::workspace_holders_for`)
    /// à partir de ceux de ses sessions membres : contrairement au contenu
    /// d'un workspace lui-même (voir `workspace::crdt::YrsWorkspace`, un
    /// CRDT gossipé), cette appartenance change rarement une fois établie,
    /// d'où une réplication Raft plutôt qu'un champ de plus dans le CRDT.
    pub session_workspaces: HashMap<SessionId, WorkspaceId>,
}

impl ControlPlaneState {
    /// Workers actuellement affectés à un job `RunAgent` de `session_id`
    /// (`Scheduled`/`Running`) — dérivé de `jobs`, jamais stocké séparément :
    /// une session peut avoir plusieurs frames actifs en parallèle sur des
    /// workers différents (voir `session::crdt::YrsSession`), donc "le"
    /// détenteur n'existe pas. Utilisé pour répondre à
    /// `RpcCall::SESSION_HOLDERS`, que le worker prenant en charge un nouveau
    /// frame de cette session interroge lui-même pour savoir où synchroniser
    /// son état CRDT (voir `network::cp::session_holders_for` et
    /// `session::client::SessionClient::acquire`).
    pub fn session_holders(&self, session_id: SessionId) -> HashSet<PeerId> {
        self.jobs
            .values()
            .filter(|record| {
                let JobKind::RunAgent(agent_id) = &record.job.kind;
                agent_id.session_id() == session_id
            })
            .filter_map(|record| match record.state {
                JobState::Scheduled { worker } | JobState::Running { worker } => Some(worker),
                _ => None,
            })
            .collect()
    }
}

/// Applique une commande répliquée à l'état applicatif — appelé uniquement pour
/// des entrées déjà committées par une majorité du cluster (voir `RaftStateMachine::apply`).
fn apply_request(state: &mut ControlPlaneState, request: ControlPlaneRequest) -> ControlPlaneResponse {
    match request {
        ControlPlaneRequest::SubmitJob(job) => {
            state.jobs.insert(job.id, JobRecord { job, state: JobState::Pending });
            ControlPlaneResponse::Ok
        }
        ControlPlaneRequest::AssignJob { job_id, worker } => {
            let Some(record) = state.jobs.get_mut(&job_id) else {
                return ControlPlaneResponse::Rejected { reason: format!("job {job_id} inconnu") };
            };
            record.state = JobState::Scheduled { worker };
            ControlPlaneResponse::Ok
        }
        ControlPlaneRequest::CommitState { job_id, new_state } => {
            let Some(record) = state.jobs.get_mut(&job_id) else {
                return ControlPlaneResponse::Rejected { reason: format!("job {job_id} inconnu") };
            };
            record.state = new_state;
            ControlPlaneResponse::Ok
        }
        ControlPlaneRequest::RegisterWorker { worker } => {
            state.workers.insert(worker.peer_id, worker);
            ControlPlaneResponse::Ok
        }
        ControlPlaneRequest::UnregisterWorker { peer_id } => {
            state.workers.remove(&peer_id);
            ControlPlaneResponse::Ok
        }
        ControlPlaneRequest::RegisterPersistency { peer_id } => {
            state.persistency_nodes.insert(peer_id);
            ControlPlaneResponse::Ok
        }
        ControlPlaneRequest::SetModel { id, declaration } => {
            state.models.insert(id, declaration);
            ControlPlaneResponse::Ok
        }
        ControlPlaneRequest::RemoveModel { id } => {
            state.models.remove(&id);
            ControlPlaneResponse::Ok
        }
        ControlPlaneRequest::SetTool { id, declaration } => {
            state.tools.insert(id, declaration);
            ControlPlaneResponse::Ok
        }
        ControlPlaneRequest::RemoveTool { id } => {
            state.tools.remove(&id);
            ControlPlaneResponse::Ok
        }
        ControlPlaneRequest::SetExpert { id, declaration } => {
            state.experts.insert(id, declaration);
            ControlPlaneResponse::Ok
        }
        ControlPlaneRequest::RemoveExpert { id } => {
            state.experts.remove(&id);
            ControlPlaneResponse::Ok
        }
        ControlPlaneRequest::SetStateGraph { id, declaration } => {
            state.state_graphs.insert(id, declaration);
            ControlPlaneResponse::Ok
        }
        ControlPlaneRequest::RemoveStateGraph { id } => {
            state.state_graphs.remove(&id);
            ControlPlaneResponse::Ok
        }
        ControlPlaneRequest::SetSessionWorkspace { session_id, workspace_id } => {
            match workspace_id {
                Some(workspace_id) => {
                    state.session_workspaces.insert(session_id, workspace_id);
                }
                None => {
                    state.session_workspaces.remove(&session_id);
                }
            }
            ControlPlaneResponse::Ok
        }
    }
}

/// Mutation du catalogue de modèles à répercuter sur le stockage local (voir
/// [`ControlPlaneStateMachineStore::persist_model_mutation`]), extraite par
/// avant coup d'une [`ControlPlaneRequest`] (voir
/// [`RaftStateMachine::apply`](ControlPlaneStateMachineStore)) — l'entrée du
/// log est consommée par [`apply_request`], donc ce qu'il faut persister doit
/// être capturé avant.
enum ModelMutation {
    Set(ModelId, Model),
    Remove(ModelId),
}

fn model_mutation_of(request: &ControlPlaneRequest) -> Option<ModelMutation> {
    match request {
        ControlPlaneRequest::SetModel { id, declaration } => Some(ModelMutation::Set(id.clone(), declaration.clone())),
        ControlPlaneRequest::RemoveModel { id } => Some(ModelMutation::Remove(id.clone())),
        _ => None,
    }
}

/// Mutation du catalogue de tools à répercuter sur le stockage local (voir
/// [`ControlPlaneStateMachineStore::persist_tool_mutation`]), sur le même
/// modèle que [`ModelMutation`].
enum ToolMutation {
    Set(ToolId, ToolDeclaration),
    Remove(ToolId),
}

fn tool_mutation_of(request: &ControlPlaneRequest) -> Option<ToolMutation> {
    match request {
        ControlPlaneRequest::SetTool { id, declaration } => Some(ToolMutation::Set(id.clone(), declaration.clone())),
        ControlPlaneRequest::RemoveTool { id } => Some(ToolMutation::Remove(id.clone())),
        _ => None,
    }
}

/// Mutation du catalogue d'experts à répercuter sur le stockage local (voir
/// [`ControlPlaneStateMachineStore::persist_expert_mutation`]), sur le même
/// modèle que [`ModelMutation`]/[`ToolMutation`].
enum ExpertMutation {
    Set(ExpertId, ExpertDeclaration),
    Remove(ExpertId),
}

fn expert_mutation_of(request: &ControlPlaneRequest) -> Option<ExpertMutation> {
    match request {
        ControlPlaneRequest::SetExpert { id, declaration } => Some(ExpertMutation::Set(id.clone(), declaration.clone())),
        ControlPlaneRequest::RemoveExpert { id } => Some(ExpertMutation::Remove(id.clone())),
        _ => None,
    }
}

/// Mutation du catalogue de graphes d'états à répercuter sur le stockage
/// local (voir [`ControlPlaneStateMachineStore::persist_state_graph_mutation`]),
/// sur le même modèle que [`ModelMutation`]/[`ToolMutation`]/[`ExpertMutation`].
enum StateGraphMutation {
    Set(StateGraphId, StateGraphDeclaration),
    Remove(StateGraphId),
}

fn state_graph_mutation_of(request: &ControlPlaneRequest) -> Option<StateGraphMutation> {
    match request {
        ControlPlaneRequest::SetStateGraph { id, declaration } => Some(StateGraphMutation::Set(id.clone(), declaration.clone())),
        ControlPlaneRequest::RemoveStateGraph { id } => Some(StateGraphMutation::Remove(id.clone())),
        _ => None,
    }
}

/// Snapshot sérialisé : état applicatif + métadonnées du dernier log appliqué.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct SerializableControlPlaneState {
    state: ControlPlaneState,
    last_applied_log: Option<LogId<RaftNodeId>>,
    last_membership: StoredMembership<RaftNodeId, RaftNode>,
}

impl SerializableControlPlaneState {
    fn new(state: ControlPlaneState) -> Self {
        Self {state, ..Default::default()}
    }
}


/// Implémentation concrète branchée sur openraft, protégée par un RwLock
/// pour permettre des lectures concurrentes (dashboard, monitoring) pendant
/// que le scheduler écrit.
#[derive(Clone)]
pub struct ControlPlaneStateMachineStore {
    inner: Arc<RwLock<SerializableControlPlaneState>>,
    /// Dernier snapshot construit, conservé pour pouvoir le renvoyer tel quel
    /// à un follower qui a trop de retard sur le log (InstallSnapshot RPC).
    current_snapshot: Arc<RwLock<Option<Snapshot<TypeConfig>>>>,
    /// Compteur incrémenté à chaque snapshot construit, pour garantir l'unicité
    /// du `snapshot_id` même si deux snapshots partagent le même `last_applied_log`.
    snapshot_idx: Arc<std::sync::atomic::AtomicU64>,
    /// Stockage local chiffré du catalogue de modèles (voir
    /// `model::catalog::store`) — mis à jour automatiquement à chaque mutation
    /// appliquée ([`Self::persist_model_mutation`]) ou snapshot reçu, pour
    /// permettre une récupération à froid sans dépendre du reste du cluster
    /// (voir `network::cp::start_control_plane`).
    model_store: Arc<dyn ModelStore>,
    /// Stockage local du catalogue de tools (voir `tools::catalog::store`),
    /// sur le même modèle que `model_store` — pas de chiffrement, une
    /// déclaration de tool ne porte aucun secret (voir
    /// [`crate::tools::declaration::ToolDeclaration`]).
    tool_store: Arc<dyn ToolStore>,
    /// Stockage local du catalogue d'experts (voir `expert::catalog::store`),
    /// sur le même modèle que `tool_store` — pas de chiffrement, une
    /// déclaration d'expert ne porte aucun secret (voir
    /// [`crate::expert::declaration::ExpertDeclaration`]).
    expert_store: Arc<dyn ExpertStore>,
    /// Stockage local du catalogue de graphes d'états (voir
    /// `mode::state_graph::catalog::store`), sur le même modèle que
    /// `expert_store` — pas de chiffrement, une déclaration de graphe ne
    /// porte aucun secret (voir
    /// [`crate::mode::state_graph::declaration::StateGraphDeclaration`]).
    state_graph_store: Arc<dyn StateGraphStore>,
    /// Secret du cluster, utilisé pour chiffrer/déchiffrer les clés API au
    /// repos (voir `SecretManager::derive_storage_key`).
    secret: Arc<SecretManager>,
}

impl ControlPlaneStateMachineStore {
    pub fn new(
        state: ControlPlaneState,
        model_store: Arc<dyn ModelStore>,
        tool_store: Arc<dyn ToolStore>,
        expert_store: Arc<dyn ExpertStore>,
        state_graph_store: Arc<dyn StateGraphStore>,
        secret: Arc<SecretManager>,
    ) -> Self {
        Self {
            inner: Arc::new(RwLock::new(SerializableControlPlaneState::new(state))),
            current_snapshot: Arc::new(RwLock::new(None)),
            snapshot_idx: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            model_store,
            tool_store,
            expert_store,
            state_graph_store,
            secret,
        }
    }

    /// Accès direct en lecture pour le reste du système (scheduler, API HTTP
    /// de monitoring) — cohérence "eventually", suffisant pour du non-critique.
    pub async fn read_state(&self) -> ControlPlaneState {
        self.inner.read().await.state.clone()
    }

    /// Répercute une mutation du catalogue sur le stockage local — best
    /// effort : un échec n'invalide pas l'entrée déjà committée par le
    /// cluster (source de vérité), il ne fait que dégrader la récupération à
    /// froid de ce nœud (voir `network::cp::start_control_plane`).
    async fn persist_model_mutation(&self, mutation: ModelMutation) {
        match mutation {
            ModelMutation::Set(id, declaration) => match encrypt_for_storage(&declaration, &self.secret) {
                Ok(encrypted) => {
                    let stored = StoredModel { id: id.clone(), declaration: encrypted };
                    if let Err(error) = self.model_store.put(&id, &stored).await {
                        warn!(%error, %id, "échec de la persistance locale du modèle (récupération à froid dégradée)");
                    }
                }
                Err(error) => warn!(%error, %id, "échec du chiffrement du modèle pour stockage local"),
            },
            ModelMutation::Remove(id) => {
                if let Err(error) = self.model_store.delete(&id).await {
                    warn!(%error, %id, "échec de la suppression locale du modèle");
                }
            }
        }
    }

    /// Répercute une mutation du catalogue de tools sur le stockage local,
    /// sur le même modèle que [`Self::persist_model_mutation`] — best effort,
    /// sans chiffrement à effectuer.
    async fn persist_tool_mutation(&self, mutation: ToolMutation) {
        match mutation {
            ToolMutation::Set(id, declaration) => {
                let stored = StoredTool { id: id.clone(), declaration };
                if let Err(error) = self.tool_store.put(&id, &stored).await {
                    warn!(%error, %id, "échec de la persistance locale du tool (récupération à froid dégradée)");
                }
            }
            ToolMutation::Remove(id) => {
                if let Err(error) = self.tool_store.delete(&id).await {
                    warn!(%error, %id, "échec de la suppression locale du tool");
                }
            }
        }
    }

    /// Répercute une mutation du catalogue d'experts sur le stockage local,
    /// sur le même modèle que [`Self::persist_tool_mutation`] — best effort,
    /// sans chiffrement à effectuer.
    async fn persist_expert_mutation(&self, mutation: ExpertMutation) {
        match mutation {
            ExpertMutation::Set(id, declaration) => {
                let stored = StoredExpert { id: id.clone(), declaration };
                if let Err(error) = self.expert_store.put(&id, &stored).await {
                    warn!(%error, %id, "échec de la persistance locale de l'expert (récupération à froid dégradée)");
                }
            }
            ExpertMutation::Remove(id) => {
                if let Err(error) = self.expert_store.delete(&id).await {
                    warn!(%error, %id, "échec de la suppression locale de l'expert");
                }
            }
        }
    }

    /// Répercute une mutation du catalogue de graphes d'états sur le
    /// stockage local, sur le même modèle que [`Self::persist_expert_mutation`]
    /// — best effort, sans chiffrement à effectuer.
    async fn persist_state_graph_mutation(&self, mutation: StateGraphMutation) {
        match mutation {
            StateGraphMutation::Set(id, declaration) => {
                let stored = StoredStateGraph { id: id.clone(), declaration };
                if let Err(error) = self.state_graph_store.put(&id, &stored).await {
                    warn!(%error, %id, "échec de la persistance locale du graphe d'états (récupération à froid dégradée)");
                }
            }
            StateGraphMutation::Remove(id) => {
                if let Err(error) = self.state_graph_store.delete(&id).await {
                    warn!(%error, %id, "échec de la suppression locale du graphe d'états");
                }
            }
        }
    }
}

impl RaftSnapshotBuilder<TypeConfig> for ControlPlaneStateMachineStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<RaftNodeId>> {
        let (data, last_applied_log, last_membership) = {
            let inner = self.inner.read().await;
            let data = serde_json::to_vec(&inner.state).map_err(|e| StorageIOError::read_state_machine(&e))?;
            (data, inner.last_applied_log, inner.last_membership.clone())
        };

        let snapshot_idx = self.snapshot_idx.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let snapshot_id = match last_applied_log {
            Some(log_id) => format!("{log_id}-{snapshot_idx}"),
            None => format!("--{snapshot_idx}"),
        };

        let meta = SnapshotMeta { last_log_id: last_applied_log, last_membership, snapshot_id };

        let snapshot = Snapshot { meta: meta.clone(), snapshot: Box::new(Cursor::new(data)) };

        *self.current_snapshot.write().await = Some(snapshot.clone());

        Ok(snapshot)
    }
}

impl RaftStateMachine<TypeConfig> for ControlPlaneStateMachineStore {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<RaftNodeId>>, StoredMembership<RaftNodeId, RaftNode>), StorageError<RaftNodeId>> {
        let inner = self.inner.read().await;
        Ok((inner.last_applied_log, inner.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<ControlPlaneResponse>, StorageError<RaftNodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let mut inner = self.inner.write().await;
        let mut responses = Vec::new();
        // Mutations du catalogue à répercuter sur le stockage local une fois
        // le verrou d'écriture sur `inner` relâché (voir la boucle
        // ci-dessous) — appliquées par *tout* nœud control plane qui traite
        // ces entrées, pas seulement celui qui les a proposées : c'est ce qui
        // tient le stockage local de chacun à jour automatiquement.
        let mut model_mutations = Vec::new();
        let mut tool_mutations = Vec::new();
        let mut expert_mutations = Vec::new();
        let mut state_graph_mutations = Vec::new();

        for entry in entries {
            inner.last_applied_log = Some(entry.log_id);

            let response = match entry.payload {
                EntryPayload::Blank => ControlPlaneResponse::Ok,
                EntryPayload::Normal(request) => {
                    if let Some(mutation) = model_mutation_of(&request) {
                        model_mutations.push(mutation);
                    }
                    if let Some(mutation) = tool_mutation_of(&request) {
                        tool_mutations.push(mutation);
                    }
                    if let Some(mutation) = expert_mutation_of(&request) {
                        expert_mutations.push(mutation);
                    }
                    if let Some(mutation) = state_graph_mutation_of(&request) {
                        state_graph_mutations.push(mutation);
                    }
                    apply_request(&mut inner.state, request)
                }
                EntryPayload::Membership(membership) => {
                    inner.last_membership = StoredMembership::new(Some(entry.log_id), membership);
                    ControlPlaneResponse::Ok
                }
            };

            responses.push(response);
        }

        drop(inner);
        for mutation in model_mutations {
            self.persist_model_mutation(mutation).await;
        }
        for mutation in tool_mutations {
            self.persist_tool_mutation(mutation).await;
        }
        for mutation in expert_mutations {
            self.persist_expert_mutation(mutation).await;
        }
        for mutation in state_graph_mutations {
            self.persist_state_graph_mutation(mutation).await;
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, StorageError<RaftNodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<RaftNodeId, RaftNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<RaftNodeId>> {
        let data = snapshot.into_inner();
        let state: ControlPlaneState =
            serde_json::from_slice(&data).map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;

        // Répercute le catalogue reçu via ce snapshot sur le stockage local :
        // sans cela, un nœud qui rattrape le cluster par snapshot plutôt que
        // par `apply` (retard trop important) garderait un stockage local
        // périmé, dégradant sa propre récupération à froid lors d'un futur
        // redémarrage.
        for (id, declaration) in state.models.iter() {
            self.persist_model_mutation(ModelMutation::Set(id.clone(), declaration.clone())).await;
        }
        for (id, declaration) in state.tools.iter() {
            self.persist_tool_mutation(ToolMutation::Set(id.clone(), declaration.clone())).await;
        }
        for (id, declaration) in state.experts.iter() {
            self.persist_expert_mutation(ExpertMutation::Set(id.clone(), declaration.clone())).await;
        }
        for (id, declaration) in state.state_graphs.iter() {
            self.persist_state_graph_mutation(StateGraphMutation::Set(id.clone(), declaration.clone())).await;
        }

        {
            let mut inner = self.inner.write().await;
            inner.state = state;
            inner.last_applied_log = meta.last_log_id;
            inner.last_membership = meta.last_membership.clone();
        }

        *self.current_snapshot.write().await = Some(Snapshot { meta: meta.clone(), snapshot: Box::new(Cursor::new(data)) });

        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, StorageError<RaftNodeId>> {
        Ok(self.current_snapshot.read().await.clone())
    }
}