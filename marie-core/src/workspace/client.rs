use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::bail;
use futures::{Stream, StreamExt as _};
use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{RwLock, broadcast};
use tokio_stream::wrappers::{BroadcastStream, errors::BroadcastStreamRecvError};
use tracing::debug;
use yrs::{StateVector, updates::{decoder::Decode, encoder::Encode}};

use crate::{
    agent::context::ContextEntry,
    network::{actor::{NetworkService, NetworkEvent, NetworkEventHandler}, cp::rpc::{RpcCall, SetSessionWorkspaceRequest, WorkspaceFetchRequest}},
    session::SessionId,
    workspace::{WorkspaceApi, WorkspaceId, crdt::YrsWorkspace, sync::{WORKSPACE_SYNC_TOPIC, WorkspaceSyncMessage}},
};

/// Capacité du canal de diffusion locale des [`WorkspaceEvent`] — même
/// raisonnement que `session::client::SESSION_EVENTS_CAPACITY`.
const WORKSPACE_EVENTS_CAPACITY: usize = 256;

/// Topic gossipsub sur lequel les événements de cycle de vie d'un workspace
/// sont diffusés à tout pair intéressé — voir [`WorkspaceClient::emit`] et
/// [`WorkspaceClient::new`]. Ne transporte que des événements de cycle de
/// vie (petits, peu fréquents), jamais le contenu du workspace lui-même
/// (voir [`WORKSPACE_SYNC_TOPIC`] pour ça).
const WORKSPACE_EVENTS_TOPIC: &str = "marie/worker/workspace-events/1.0.0";

/// Événement de cycle de vie d'un workspace, diffusé localement (voir
/// [`WorkspaceClient::subscribe`]) et gossipé au reste du cluster (voir
/// [`WORKSPACE_EVENTS_TOPIC`]) — sur le même principe que
/// `session::client::SessionEvent`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkspaceEvent {
    /// Le workspace est désormais connu localement — créé vierge ou
    /// synchronisé depuis un détenteur précédent (voir
    /// [`WorkspaceClient::acquire`]).
    Created { workspace_id: WorkspaceId },
    /// Une session vient d'être rattachée au workspace (voir
    /// [`WorkspaceClient::add_session`]).
    SessionAdded { workspace_id: WorkspaceId, session_id: SessionId },
    /// Une session vient d'être détachée du workspace (voir
    /// [`WorkspaceClient::remove_session`]).
    SessionRemoved { workspace_id: WorkspaceId, session_id: SessionId },
    /// Une entrée a été ajoutée au fil de contexte partagé (voir
    /// [`WorkspaceClient::push_context_entry`]).
    ContextAppended { workspace_id: WorkspaceId, entry: ContextEntry },
    /// Une valeur du store clé-valeur partagé a été définie (créée ou
    /// remplacée, voir [`WorkspaceClient::set_value`]).
    ValueChanged { workspace_id: WorkspaceId, key: String, value: Value },
    /// Une valeur du store clé-valeur partagé a été retirée (voir
    /// [`WorkspaceClient::remove_value`]).
    ValueRemoved { workspace_id: WorkspaceId, key: String },
    /// Le workspace n'est plus détenu localement par ce worker (voir
    /// [`WorkspaceClient::remove`]).
    Removed { workspace_id: WorkspaceId },
}

/// Flux de [`WorkspaceEvent`] retourné par [`WorkspaceClient::subscribe`] —
/// même motif que `session::client::SessionEventHandler`.
pub struct WorkspaceEventHandler(BroadcastStream<WorkspaceEvent>);

impl Stream for WorkspaceEventHandler {
    type Item = WorkspaceEvent;

    fn poll_next(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        loop {
            return match std::pin::Pin::new(&mut self.0).poll_next(cx) {
                std::task::Poll::Ready(Some(Ok(event))) => std::task::Poll::Ready(Some(event)),
                std::task::Poll::Ready(Some(Err(BroadcastStreamRecvError::Lagged(skipped)))) => {
                    debug!(skipped, "abonné WorkspaceEvent en retard, événements perdus");
                    continue;
                }
                std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
                std::task::Poll::Pending => std::task::Poll::Pending,
            };
        }
    }
}

/// Workspace détenu localement, avec le curseur nécessaire pour ne publier
/// que les deltas — même motif que `session::client::SessionEntry`.
struct WorkspaceEntry {
    workspace: YrsWorkspace,
    last_synced: StateVector,
}

impl WorkspaceEntry {
    fn new(workspace: YrsWorkspace) -> Self {
        let last_synced = workspace.state_vector();
        Self { workspace, last_synced }
    }
}

/// Pont entre le stockage local des workspaces CRDT (voir
/// `workspace::crdt::YrsWorkspace`) et le réseau — sur exactement le même
/// principe que `session::client::SessionClient` (voir sa doc pour la
/// justification détaillée), à ceci près qu'un workspace n'est jamais
/// directement exécuté par un job : ses détenteurs se déduisent de ceux de
/// ses sessions membres (voir [`Self::acquire`] et
/// `network::cp::workspace_holders_for`).
///
/// Bon marché à cloner (comme [`NetworkClient`]) : pensé pour être threadé
/// dans les tâches de fond au même titre que lui.
#[derive(Clone)]
pub struct WorkspaceClient {
    network: NetworkService,
    workspaces: Arc<RwLock<HashMap<WorkspaceId, WorkspaceEntry>>>,
    events: broadcast::Sender<WorkspaceEvent>,
    /// Nœuds `Persistency` découverts directement par ce nœud (voir
    /// `NetworkEvent::PersistencyPeerDiscovered`) — indépendant de ce que
    /// sait le control plane, pour amorcer un workspace même si celui-ci
    /// est injoignable (voir [`Self::acquire`]).
    known_persistency_peers: Arc<RwLock<HashSet<PeerId>>>,
}

impl WorkspaceClient {
    /// S'abonne lui-même au flux d'événements réseau de `network` (voir
    /// `NetworkClient::subscribe_events`) et démarre sa propre tâche de fond
    /// pour traiter les messages gossipés sur [`WORKSPACE_EVENTS_TOPIC`]
    /// (réémis aux abonnés locaux, voir [`Self::subscribe`]) et
    /// [`WORKSPACE_SYNC_TOPIC`] (fusionnés dans les workspaces détenus
    /// localement).
    pub fn new(network: NetworkService) -> Self {
        let (events, _) = broadcast::channel(WORKSPACE_EVENTS_CAPACITY);
        network.subscribe(WORKSPACE_EVENTS_TOPIC);
        network.subscribe(WORKSPACE_SYNC_TOPIC);

        let workspaces = Arc::new(RwLock::new(HashMap::new()));
        let known_persistency_peers = Arc::new(RwLock::new(HashSet::new()));
        tokio::spawn(ingest_network_events(
            network.subscribe_events(),
            events.clone(),
            workspaces.clone(),
            known_persistency_peers.clone(),
        ));

        Self { network, workspaces, events, known_persistency_peers }
    }

    /// S'abonne aux événements de cycle de vie des workspaces — les siens
    /// comme ceux gossipés par d'autres pairs (voir [`WorkspaceEvent`]).
    pub fn subscribe(&self) -> WorkspaceEventHandler {
        WorkspaceEventHandler(BroadcastStream::new(self.events.subscribe()))
    }

    /// Diffuse `event` aux abonnés locaux et au reste du cluster via
    /// gossipsub — best-effort dans les deux cas, même principe que
    /// `session::client::SessionClient::emit`.
    fn emit(&self, event: WorkspaceEvent) {
        if let Err(error) = self.network.publish(WORKSPACE_EVENTS_TOPIC, &event) {
            debug!(%error, ?event, "publication gossip de l'événement de workspace échouée");
        }
        let _ = self.events.send(event);
    }

    /// Diffuse `diff` aux autres détenteurs de `workspace_id` via
    /// [`WORKSPACE_SYNC_TOPIC`] — best-effort.
    fn publish_sync(&self, workspace_id: WorkspaceId, diff: Vec<u8>) {
        let message = WorkspaceSyncMessage { workspace_id, diff };
        if let Err(error) = self.network.publish(WORKSPACE_SYNC_TOPIC, &message) {
            debug!(%error, %workspace_id, "publication du diff de workspace échouée");
        }
    }

    /// Prend en charge `workspace_id` : localise une copie existante (voir
    /// [`Self::locate_workspace`]) et s'y synchronise, ou en crée une
    /// vierge si aucune n'est trouvée (ce nœud est le premier à s'y
    /// intéresser). Ne fait rien si déjà détenu localement — dans ce cas
    /// [`WorkspaceEvent::Created`] n'est pas réémis.
    pub async fn acquire(&self, workspace_id: WorkspaceId) -> anyhow::Result<()> {
        if self.workspaces.read().await.contains_key(&workspace_id) {
            return Ok(());
        }

        let workspace = match self.locate_workspace(workspace_id).await {
            Some(workspace) => workspace,
            None => YrsWorkspace::new(workspace_id),
        };

        self.workspaces.write().await.insert(workspace_id, WorkspaceEntry::new(workspace));
        self.emit(WorkspaceEvent::Created { workspace_id });
        Ok(())
    }

    /// Localise une copie existante de `workspace_id` — sur exactement le
    /// même principe que `session::client::SessionClient::locate_session`
    /// (voir sa doc) : d'abord le control plane (voir
    /// [`RpcCall::WORKSPACE_HOLDERS`]), puis, à défaut, les nœuds
    /// `Persistency` découverts directement par ce nœud.
    async fn locate_workspace(&self, workspace_id: WorkspaceId) -> Option<YrsWorkspace> {
        let cp_holders: Vec<PeerId> = self
            .network
            .rpc(RpcCall::new(RpcCall::WORKSPACE_HOLDERS, workspace_id))
            .await
            .unwrap_or_else(|error| {
                debug!(%error, %workspace_id, "interrogation du control plane pour les détenteurs de workspace échouée");
                Vec::new()
            });

        if !cp_holders.is_empty() {
            match self.fetch_from_any(workspace_id, &cp_holders).await {
                Ok(workspace) => return Some(workspace),
                Err(error) => debug!(%error, %workspace_id, "aucun détenteur indiqué par le control plane n'a répondu"),
            }
        }

        let persistency_peers: Vec<PeerId> = self.known_persistency_peers.read().await.iter().copied().collect();
        if !persistency_peers.is_empty() {
            match self.fetch_from_any(workspace_id, &persistency_peers).await {
                Ok(workspace) => return Some(workspace),
                Err(error) => debug!(%error, %workspace_id, "aucun nœud persistency connu localement n'a répondu"),
            }
        }

        None
    }

    /// Crée une nouvelle session et l'attache immédiatement à `workspace_id`
    /// (voir [`Self::add_session`]) — seul point d'entrée pour faire naître
    /// une session : une session ne doit jamais exister sans appartenir à un
    /// workspace dès sa création, faute de quoi `/session/files` n'a pas de
    /// racine où se rattacher (voir `persistency::inode::PostgresInodeCatalog::for_session`,
    /// dont la racine est un sous-arbre de celle du workspace) et
    /// `session::client::SessionClient::acquire`/`vfs` refusent de la
    /// prendre en charge (voir leur doc). `workspace_id` doit déjà être
    /// acquis localement (voir [`Self::acquire`]), comme pour
    /// [`Self::add_session`].
    pub async fn create_session(&self, workspace_id: WorkspaceId) -> anyhow::Result<SessionId> {
        let session_id = crate::id::generate_id();
        self.add_session(workspace_id, session_id).await?;
        Ok(session_id)
    }

    /// Rattache `session_id` au workspace (voir
    /// [`WorkspaceApi::add_session`]), diffuse [`WorkspaceEvent::SessionAdded`]
    /// et publie le delta CRDT résultant. Déclare aussi best-effort cette
    /// appartenance au control plane (voir [`RpcCall::SET_SESSION_WORKSPACE`]
    /// et `ControlPlaneState::session_workspaces`) pour que de futurs appels
    /// à [`Self::acquire`] par d'autres pairs sachent retrouver ce
    /// workspace via ses sessions membres — un échec de cette notification
    /// n'invalide pas le rattachement local, qui reste de toute façon
    /// gossipé aux autres détenteurs actifs via [`WORKSPACE_SYNC_TOPIC`].
    pub async fn add_session(&self, workspace_id: WorkspaceId, session_id: SessionId) -> anyhow::Result<()> {
        let diff = {
            let mut workspaces = self.workspaces.write().await;
            let Some(entry) = workspaces.get_mut(&workspace_id) else {
                bail!("workspace {workspace_id} inconnu de ce nœud");
            };
            entry.workspace.add_session(session_id)?;
            self.diff_and_bump(entry)
        };

        self.publish_sync(workspace_id, diff);
        self.emit(WorkspaceEvent::SessionAdded { workspace_id, session_id });

        let request = SetSessionWorkspaceRequest { session_id, workspace_id: Some(workspace_id) };
        if let Err(error) = self.network.rpc::<crate::network::cp::rpc::Void>(RpcCall::new(RpcCall::SET_SESSION_WORKSPACE, request)).await {
            debug!(%error, %workspace_id, %session_id, "déclaration de l'appartenance au control plane échouée");
        }

        Ok(())
    }

    /// Détache `session_id` du workspace (voir
    /// [`WorkspaceApi::remove_session`]), diffuse
    /// [`WorkspaceEvent::SessionRemoved`] et publie le delta CRDT résultant.
    /// Efface aussi best-effort l'appartenance déclarée au control plane
    /// (voir [`Self::add_session`]).
    pub async fn remove_session(&self, workspace_id: WorkspaceId, session_id: SessionId) -> anyhow::Result<()> {
        let diff = {
            let mut workspaces = self.workspaces.write().await;
            let Some(entry) = workspaces.get_mut(&workspace_id) else {
                bail!("workspace {workspace_id} inconnu de ce nœud");
            };
            entry.workspace.remove_session(session_id)?;
            self.diff_and_bump(entry)
        };

        self.publish_sync(workspace_id, diff);
        self.emit(WorkspaceEvent::SessionRemoved { workspace_id, session_id });

        let request = SetSessionWorkspaceRequest { session_id, workspace_id: None };
        if let Err(error) = self.network.rpc::<crate::network::cp::rpc::Void>(RpcCall::new(RpcCall::SET_SESSION_WORKSPACE, request)).await {
            debug!(%error, %workspace_id, %session_id, "effacement de l'appartenance au control plane échoué");
        }

        Ok(())
    }

    /// Sessions actuellement membres du workspace, ou vide s'il est inconnu
    /// de ce nœud.
    pub async fn sessions(&self, workspace_id: WorkspaceId) -> Vec<SessionId> {
        match self.workspaces.read().await.get(&workspace_id) {
            Some(entry) => entry.workspace.sessions(),
            None => Vec::new(),
        }
    }

    /// Ajoute une entrée au fil de contexte partagé du workspace (voir
    /// [`WorkspaceApi::push_context_entry`]), diffuse
    /// [`WorkspaceEvent::ContextAppended`] et publie le delta CRDT résultant.
    pub async fn push_context_entry(&self, workspace_id: WorkspaceId, entry: ContextEntry) -> anyhow::Result<()> {
        let diff = {
            let mut workspaces = self.workspaces.write().await;
            let Some(workspace_entry) = workspaces.get_mut(&workspace_id) else {
                bail!("workspace {workspace_id} inconnu de ce nœud");
            };
            workspace_entry.workspace.push_context_entry(&entry)?;
            self.diff_and_bump(workspace_entry)
        };

        self.publish_sync(workspace_id, diff);
        self.emit(WorkspaceEvent::ContextAppended { workspace_id, entry });
        Ok(())
    }

    /// Fil de contexte partagé complet, ou vide si le workspace est inconnu
    /// de ce nœud.
    pub async fn context(&self, workspace_id: WorkspaceId) -> Vec<ContextEntry> {
        match self.workspaces.read().await.get(&workspace_id) {
            Some(entry) => entry.workspace.context(),
            None => Vec::new(),
        }
    }

    /// Définit une valeur du store clé-valeur partagé (voir
    /// [`WorkspaceApi::set_value`]), diffuse [`WorkspaceEvent::ValueChanged`]
    /// et publie le delta CRDT résultant.
    pub async fn set_value(&self, workspace_id: WorkspaceId, key: String, value: Value) -> anyhow::Result<()> {
        let diff = {
            let mut workspaces = self.workspaces.write().await;
            let Some(entry) = workspaces.get_mut(&workspace_id) else {
                bail!("workspace {workspace_id} inconnu de ce nœud");
            };
            entry.workspace.set_value(&key, &value)?;
            self.diff_and_bump(entry)
        };

        self.publish_sync(workspace_id, diff);
        self.emit(WorkspaceEvent::ValueChanged { workspace_id, key, value });
        Ok(())
    }

    /// Retire une clé du store clé-valeur partagé (voir
    /// [`WorkspaceApi::remove_value`]), diffuse
    /// [`WorkspaceEvent::ValueRemoved`] et publie le delta CRDT résultant.
    pub async fn remove_value(&self, workspace_id: WorkspaceId, key: String) -> anyhow::Result<()> {
        let diff = {
            let mut workspaces = self.workspaces.write().await;
            let Some(entry) = workspaces.get_mut(&workspace_id) else {
                bail!("workspace {workspace_id} inconnu de ce nœud");
            };
            entry.workspace.remove_value(&key)?;
            self.diff_and_bump(entry)
        };

        self.publish_sync(workspace_id, diff);
        self.emit(WorkspaceEvent::ValueRemoved { workspace_id, key });
        Ok(())
    }

    /// Valeur associée à `key`, ou `None` si absente ou si le workspace est
    /// inconnu de ce nœud.
    pub async fn value(&self, workspace_id: WorkspaceId, key: &str) -> Option<Value> {
        self.workspaces.read().await.get(&workspace_id)?.workspace.value(key)
    }

    /// Snapshot complet du store clé-valeur partagé, ou vide si le workspace
    /// est inconnu de ce nœud — voir `persistency::var::WorkspaceVarStore`,
    /// utilisé pour lister `/var` dans le VFS.
    pub async fn values(&self, workspace_id: WorkspaceId) -> HashMap<String, Value> {
        match self.workspaces.read().await.get(&workspace_id) {
            Some(entry) => entry.workspace.values(),
            None => HashMap::new(),
        }
    }

    /// Retire le workspace du stockage local de ce nœud et diffuse
    /// [`WorkspaceEvent::Removed`]. Ne fait rien si non détenu. Purement
    /// local : les autres détenteurs actifs, s'il y en a, conservent leur
    /// copie.
    pub async fn remove(&self, workspace_id: WorkspaceId) {
        if self.workspaces.write().await.remove(&workspace_id).is_some() {
            self.emit(WorkspaceEvent::Removed { workspace_id });
        }
    }

    /// Calcule le diff depuis le dernier envoi/réception et avance le
    /// curseur — à appeler juste après toute mutation locale, avant de
    /// relâcher le verrou d'écriture.
    fn diff_and_bump(&self, entry: &mut WorkspaceEntry) -> Vec<u8> {
        let diff = entry.workspace.diff_since(&entry.last_synced);
        entry.last_synced = entry.workspace.state_vector();
        diff
    }

    /// Récupère l'état CRDT complet d'un workspace en interrogeant
    /// `holders` dans l'ordre jusqu'à ce que l'un réponde.
    async fn fetch_from_any(&self, workspace_id: WorkspaceId, holders: &[PeerId]) -> anyhow::Result<YrsWorkspace> {
        let mut last_error = None;

        for &holder in holders {
            match self.fetch_from(workspace_id, holder).await {
                Ok(workspace) => return Ok(workspace),
                Err(error) => {
                    debug!(%error, %workspace_id, %holder, "récupération de workspace échouée, essai du détenteur suivant");
                    last_error = Some(error);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("aucun détenteur connu pour le workspace {workspace_id}")))
    }

    /// Récupère l'état CRDT complet d'un workspace auprès d'un détenteur
    /// connu (voir [`RpcCall::FETCH_WORKSPACE`]).
    async fn fetch_from(&self, workspace_id: WorkspaceId, holder: PeerId) -> anyhow::Result<YrsWorkspace> {
        let request = WorkspaceFetchRequest { workspace_id, state_vector: StateVector::default().encode_v1() };
        let diff: Vec<u8> = self.network.rpc_to(RpcCall::new(RpcCall::FETCH_WORKSPACE, request), holder).await?;
        YrsWorkspace::from_diff(&diff)
    }

    /// Répond à une demande [`RpcCall::FETCH_WORKSPACE`] d'un pair : le
    /// diff depuis son vecteur d'état, si nous détenons encore ce workspace.
    pub async fn serve_fetch(&self, request: WorkspaceFetchRequest) -> anyhow::Result<Vec<u8>> {
        let remote_sv = StateVector::decode_v1(&request.state_vector).map_err(|error| anyhow::anyhow!(error))?;

        let workspaces = self.workspaces.read().await;
        let Some(entry) = workspaces.get(&request.workspace_id) else {
            bail!("workspace {} inconnu de ce nœud", request.workspace_id);
        };

        Ok(entry.workspace.diff_since(&remote_sv))
    }
}

/// Tâche de fond démarrée par [`WorkspaceClient::new`] — sur exactement le
/// même principe que `session::client::ingest_network_events` (voir sa doc,
/// notamment la note sur la fenêtre de course entre [`WorkspaceClient::acquire`]
/// et un diff reçu entre-temps).
async fn ingest_network_events(
    mut network_events: NetworkEventHandler,
    events: broadcast::Sender<WorkspaceEvent>,
    workspaces: Arc<RwLock<HashMap<WorkspaceId, WorkspaceEntry>>>,
    known_persistency_peers: Arc<RwLock<HashSet<PeerId>>>,
) {
    while let Some(event) = network_events.next().await {
        let NetworkEvent::GossipMessageReceived { topic, data, .. } = event else {
            if let NetworkEvent::PersistencyPeerDiscovered { peer_id, .. } = event {
                known_persistency_peers.write().await.insert(peer_id);
            }
            continue;
        };

        if topic == WORKSPACE_EVENTS_TOPIC {
            if let Ok(event) = serde_json::from_slice::<WorkspaceEvent>(&data) {
                let _ = events.send(event);
            }
            continue;
        }

        if topic == WORKSPACE_SYNC_TOPIC {
            let Ok(message) = serde_json::from_slice::<WorkspaceSyncMessage>(&data) else {
                continue;
            };

            let mut workspaces = workspaces.write().await;
            let Some(entry) = workspaces.get_mut(&message.workspace_id) else {
                continue;
            };

            if let Err(error) = entry.workspace.apply_diff(&message.diff) {
                debug!(%error, workspace_id = %message.workspace_id, "diff de workspace reçu illisible, ignoré");
                continue;
            }
            entry.last_synced = entry.workspace.state_vector();
        }
    }
}
