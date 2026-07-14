pub mod rpc;
pub mod state;
pub mod log_store;
pub mod types;
pub mod network;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Instant, Duration};

use anyhow::bail;
use futures::StreamExt as _;
use libp2p::PeerId;
use openraft::error::{ClientWriteError, ForwardToLeader, RaftError};
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use openraft::{ChangeMembers, Config, Raft};
use serde::{Deserialize, Serialize};
use tokio::sync::{oneshot, watch};
use tokio::time::{interval, sleep};
use tracing::{debug, info, warn};

use crate::{
    agent::{GlobalAgentId, status::YieldStatus},
    expert::catalog::{ExpertCatalog, ExpertId, store::ExpertStore},
    hitl::{
        HumanInputAnswer,
        client::{HITL_TOPIC, HitlGossipMessage},
    },
    job::{Job, JobId, JobKind, JobState},
    mode::state_graph::catalog::{StateGraphCatalog, StateGraphId, store::StateGraphStore},
    model::{
        catalog::{
            ModelCatalog, ModelId,
            store::{ModelStore, decrypt_from_storage},
        },
        declaration::EncryptedModel,
    },
    network::{
        actor::{NetworkActor, NetworkClient},
        cp::{
            log_store::{LogStore, RaftLogBackend},
            network::NetworkFactory,
            rpc::{
                JobStateReport, RpcCall, RpcResult, RunJobRequest, SetExpertRequest, SetModelRequest,
                SetSessionWorkspaceRequest, SetStateGraphRequest, SetToolRequest,
            },
            state::{ControlPlaneState, ControlPlaneStateMachineStore},
            types::{ControlPlaneRequest, RaftNode, RaftNodeId, TypeConfig},
        },
        peer::NodeKind,
        start_swarm,
        worker::info::WorkerInfo,
    },
    secret::{SecretError, SecretManager},
    session::SessionId,
    tools::catalog::{ToolCatalog, ToolId, store::ToolStore},
    workspace::WorkspaceId,
};

/// Fenêtre de découverte mDNS/identify avant de figer l'élection du nœud bootstrap.
///
/// À l'expiration de ce délai, chaque nœud `ControlPlane` calcule *localement*
/// et *sans aucun message d'élection* lequel des pairs connus (lui compris) a
/// le `node_id` le plus faible, et considère ce pair comme le nœud bootstrap.
/// Voir [`elect_bootstrap_leader`].
const BOOTSTRAP_DELAY: Duration = Duration::from_secs(3);

pub struct NodeHealth {
    pub last_seen: Instant,
    pub rtt: Option<Duration>, // Round-Trip Time (latence)
    pub status: NodeStatus,
}

pub enum NodeStatus {
    Alive,
    Dead
}

/// Délai au-delà duquel un job `Yielded` sans reprise déjà en vol (voir
/// [`agent_has_active_job`]) est considéré bloqué par le watchdog (voir
/// [`watch_stuck_yields`]) — volontairement bien plus long que
/// [`RECONCILE_INTERVAL`] : ni `RunExhausted` (déjà résolu immédiatement par
/// [`on_job_terminated`], ce délai n'est qu'un filet de sécurité en cas de
/// message perdu) ni `WaitingChildren` (borné par la durée d'exécution des
/// agents enfants) ne devraient légitimement rester bloqués aussi longtemps.
const YIELD_WATCHDOG_TIMEOUT: Duration = Duration::from_secs(120);

/// Nombre de reprises automatiques tentées par le watchdog avant
/// d'abandonner un agent et de se contenter d'un avertissement — évite de le
/// ressusciter indéfiniment s'il est structurellement bloqué (condition
/// d'attente qui ne se résoudra jamais).
const MAX_WATCHDOG_RESUME_ATTEMPTS: u32 = 3;

/// État local (non répliqué, comme [`NodeHealth`]/`assignments` — voir
/// [`reconcile`]) du watchdog des jobs `Yielded` : depuis quand ce nœud
/// observe un agent sans reprise déjà en vol, et combien de reprises
/// automatiques ont déjà été tentées pour lui. Perdu à chaque redémarrage ou
/// changement de leader — sans conséquence sur la correction, seulement sur
/// la réactivité (le nouveau leader repart avec une fenêtre de grâce
/// fraîche avant de considérer un agent bloqué).
struct YieldWatch {
    first_seen: Instant,
    attempts: u32,
}

/// Topic gossipsub (`node_gossip`) sur lequel les nœuds `ControlPlane` se
/// tiennent mutuellement informés des enregistrements RPC dynamiques — voir
/// [`DynamicRpcRegistry`] et [`RpcRegistryGossip`].
const RPC_REGISTRY_TOPIC: &str = "marie/cp/rpc-registry/1.0.0";

/// Message gossipé entre nœuds `ControlPlane` pour propager les
/// enregistrements RPC dynamiques appris directement (voir
/// `RpcCall::REGISTER_RPC`) à tout le cluster de control planes, même à ceux
/// qui n'ont pas de connexion directe avec l'exécuteur concerné.
///
/// Limite assumée : si le nœud à l'origine de l'enregistrement disparaît sans
/// avoir pu gossiper l'`Unregister` correspondant (crash plutôt qu'arrêt
/// propre), les autres nœuds gardent une entrée périmée jusqu'à ce qu'un
/// relais échoué la purge (voir l'auto-guérison dans `execute_rpc`). Pas de
/// TTL/heartbeat ici : jugé disproportionné pour ce cas d'usage.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum RpcRegistryGossip {
    Register { name: String, peer_id: PeerId },
    Unregister { name: String, peer_id: PeerId },
}

/// Registre des noms de RPC enregistrés dynamiquement par des pairs
/// volontaires pour les exécuter (voir `NetworkClient::register_rpc`).
///
/// Local à ce nœud control plane — pas répliqué par Raft (voir
/// [`RpcRegistryGossip`] pour la justification et le mécanisme de propagation
/// retenu à la place). Alimenté par les enregistrements directs (pairs
/// connectés à ce nœud) et par le gossip des autres control planes.
#[derive(Default)]
struct DynamicRpcRegistry {
    executors: HashMap<String, HashSet<PeerId>>,
}

impl DynamicRpcRegistry {
    /// Enregistre `peer_id` comme exécuteur de `name`. Retourne `true` si
    /// c'est une nouveauté (à gossiper), `false` si déjà connu.
    fn register(&mut self, name: String, peer_id: PeerId) -> bool {
        self.executors.entry(name).or_default().insert(peer_id)
    }

    /// Applique un message gossipé par un autre control plane : n'est jamais
    /// re-gossipé (évite les boucles).
    fn apply_gossip(&mut self, msg: RpcRegistryGossip) {
        match msg {
            RpcRegistryGossip::Register { name, peer_id } => {
                self.executors.entry(name).or_default().insert(peer_id);
            }
            RpcRegistryGossip::Unregister { name, peer_id } => self.remove_executor(&name, &peer_id),
        }
    }

    /// Retire `peer_id` de toutes les RPC qu'il exécutait — utilisé quand ce
    /// nœud perd sa propre connexion vers lui. Retourne les noms concernés,
    /// à gossiper en `Unregister` (seul le nœud ayant observé la déconnexion
    /// peut le faire).
    fn remove_peer(&mut self, peer_id: &PeerId) -> Vec<String> {
        let mut affected = Vec::new();
        self.executors.retain(|name, peers| {
            if peers.remove(peer_id) {
                affected.push(name.clone());
            }
            !peers.is_empty()
        });
        affected
    }

    /// Retire `peer_id` de la liste des exécuteurs de `name` uniquement
    /// (contrairement à [`Self::remove_peer`], ne touche pas ses autres
    /// enregistrements). Utilisé pour l'auto-guérison locale d'une entrée
    /// dont un relais vient d'échouer — volontairement non re-gossipé, voir
    /// `execute_rpc`.
    fn remove_executor(&mut self, name: &str, peer_id: &PeerId) {
        if let Some(peers) = self.executors.get_mut(name) {
            peers.remove(peer_id);
            if peers.is_empty() {
                self.executors.remove(name);
            }
        }
    }

    /// Exécuteurs actuellement enregistrés pour `name`, s'il y en a au moins un.
    fn executors_for(&self, name: &str) -> Option<&HashSet<PeerId>> {
        self.executors.get(name).filter(|peers| !peers.is_empty())
    }
}

/// Relaie `call` vers tous les `executors` en parallèle et retourne la
/// première réponse positive — "le premier qui répond l'emporte". Les autres
/// requêtes en vol sont abandonnées (leur future est simplement annulée par
/// `select_ok` en étant droppée).
async fn forward_race(
    client: &NetworkClient,
    executors: &HashSet<PeerId>,
    call: RpcCall,
) -> Result<serde_json::Value, anyhow::Error> {
    type Attempt = Pin<Box<dyn Future<Output = Result<serde_json::Value, anyhow::Error>> + Send>>;

    let attempts: Vec<Attempt> = executors
        .iter()
        .map(|&peer_id| {
            let client = client.clone();
            let call = call.clone();
            Box::pin(async move { client.rpc_to::<serde_json::Value>(call, peer_id).await }) as Attempt
        })
        .collect();

    let (value, _still_pending) = futures::future::select_ok(attempts).await?;
    Ok(value)
}

/// Reconstitue le catalogue de modèles depuis le stockage chiffré local (voir
/// `model::catalog::store`) — utilisé pour la récupération à froid au
/// démarrage (voir [`start_control_plane`]). Best-effort : une entrée
/// illisible (déchiffrement échoué) ou une lecture du stockage en échec sont
/// journalisées puis ignorées plutôt que de bloquer le démarrage — dans le
/// pire cas, le catalogue démarre incomplet ou vide et se repeuple via Raft
/// en rejoignant le cluster.
async fn load_catalog_from_store(model_store: &Arc<dyn ModelStore>, secret: &SecretManager) -> ModelCatalog {
    let mut catalog = ModelCatalog::default();

    let stored_models = match model_store.list().await {
        Ok(stored_models) => stored_models,
        Err(error) => {
            warn!(%error, "lecture du catalogue de modèles local impossible, catalogue vide au démarrage (récupération attendue depuis Raft)");
            return catalog;
        }
    };

    for stored in stored_models {
        match decrypt_from_storage(&stored.declaration, secret) {
            Ok(declaration) => {
                catalog.insert(stored.id, declaration);
            }
            Err(error) => warn!(%error, id = %stored.id, "déchiffrement d'un modèle stocké localement impossible, ignoré"),
        }
    }

    catalog
}

/// Reconstitue le catalogue de tools depuis le stockage local (voir
/// `tools::catalog::store`) — utilisé pour la récupération à froid au
/// démarrage (voir [`start_control_plane`]), sur le même modèle que
/// [`load_catalog_from_store`] mais sans déchiffrement (voir
/// [`crate::tools::declaration::ToolDeclaration`]).
async fn load_tool_catalog_from_store(tool_store: &Arc<dyn ToolStore>) -> ToolCatalog {
    let mut catalog = ToolCatalog::default();

    let stored_tools = match tool_store.list().await {
        Ok(stored_tools) => stored_tools,
        Err(error) => {
            warn!(%error, "lecture du catalogue de tools local impossible, catalogue vide au démarrage (récupération attendue depuis Raft)");
            return catalog;
        }
    };

    for stored in stored_tools {
        catalog.insert(stored.id, stored.declaration);
    }

    catalog
}

/// Reconstitue le catalogue d'experts depuis le stockage local (voir
/// `expert::catalog::store`) — utilisé pour la récupération à froid au
/// démarrage (voir [`start_control_plane`]), sur le même modèle que
/// [`load_tool_catalog_from_store`], sans déchiffrement (voir
/// [`crate::expert::declaration::ExpertDeclaration`]).
async fn load_expert_catalog_from_store(expert_store: &Arc<dyn ExpertStore>) -> ExpertCatalog {
    let mut catalog = ExpertCatalog::default();

    let stored_experts = match expert_store.list().await {
        Ok(stored_experts) => stored_experts,
        Err(error) => {
            warn!(%error, "lecture du catalogue d'experts local impossible, catalogue vide au démarrage (récupération attendue depuis Raft)");
            return catalog;
        }
    };

    for stored in stored_experts {
        catalog.insert(stored.id, stored.declaration);
    }

    catalog
}

/// Reconstitue le catalogue de graphes d'états depuis le stockage local (voir
/// `mode::state_graph::catalog::store`) — utilisé pour la récupération à
/// froid au démarrage (voir [`start_control_plane`]), sur le même modèle que
/// [`load_expert_catalog_from_store`], sans déchiffrement (voir
/// [`crate::mode::state_graph::declaration::StateGraphDeclaration`]).
async fn load_state_graph_catalog_from_store(state_graph_store: &Arc<dyn StateGraphStore>) -> StateGraphCatalog {
    let mut catalog = StateGraphCatalog::default();

    let stored_state_graphs = match state_graph_store.list().await {
        Ok(stored_state_graphs) => stored_state_graphs,
        Err(error) => {
            warn!(%error, "lecture du catalogue de graphes d'états local impossible, catalogue vide au démarrage (récupération attendue depuis Raft)");
            return catalog;
        }
    };

    for stored in stored_state_graphs {
        catalog.insert(stored.id, stored.declaration);
    }

    catalog
}

/// Dérive un identifiant Raft numérique stable à partir du `PeerId` libp2p local.
///
/// Note : le `PeerId` change à chaque démarrage (identité générée via
/// `with_new_identity()`), donc ce `node_id` ne survit pas à un redémarrage.
/// Suffisant pour l'instant en l'absence de persistance d'identité.
fn derive_node_id(peer_id: &PeerId) -> RaftNodeId {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    peer_id.hash(&mut hasher);
    hasher.finish()
}

/// Intervalle du cycle de contrôle périodique (healthcheck + ordonnancement +
/// réassignation) — voir [`reconcile`].
const RECONCILE_INTERVAL: Duration = Duration::from_secs(4);

/// Nombre de tentatives (essai initial compris) avant d'abandonner un relais
/// RPC (vers le leader raft ou vers un exécuteur enregistré dynamiquement)
/// dont la cible s'avère injoignable — voir [`propose_or_forward`] et le
/// relais dynamique dans [`execute_rpc`].
const FORWARD_RETRY_ATTEMPTS: u32 = 3;
/// Délai entre deux tentatives de relais — laisse le temps à une élection
/// raft de converger, ou à un exécuteur de repli de se signaler.
const FORWARD_RETRY_DELAY: Duration = Duration::from_millis(300);

/// `secret` : secret partagé par le cluster, utilisé pour prouver
/// automatiquement l'appartenance de ce nœud aux autres control planes lors de
/// la découverte réseau (voir `secret::SecretManager::prove_membership` et
/// `network::actor::NetworkActor`) — sans lui, aucun pair ne reconnaîtrait ce
/// nœud comme control plane, et il ne rejoindrait jamais le cluster Raft.
///
/// `raft_log_backend` : stockage durable du log Raft (voir
/// [`log_store::RaftLogBackend`]) — technologie au choix de l'appelant
/// (`log_store::redb_backend::RedbLogBackend` par défaut, ou une
/// implémentation maison, ex. Postgres). Sans lui, tout `ControlPlaneState`
/// (jobs, registre des workers, etc.) ne survit qu'à la panne d'un nœud
/// isolé (tolérée par la réplication Raft elle-même tant qu'une majorité
/// reste debout), pas à un redémarrage complet du cluster : le log étant la
/// seule source à partir de laquelle `ControlPlaneStateMachineStore` peut
/// reconstruire cet état (voir `RaftStateMachine::apply`, rejoué par
/// openraft au démarrage depuis ce qu'il retrouve ici).
///
/// `model_store` : stockage chiffré local du catalogue de modèles (voir
/// `model::catalog::store`). Au démarrage, sert de première source pour
/// peupler `ControlPlaneState::models` (voir [`load_catalog_from_store`]) —
/// une récupération à froid immédiate, sans dépendre du reste du cluster. Si
/// ce nœud n'a jamais rien persisté (premier démarrage, ou stockage vide),
/// le catalogue démarre vide et se peuple normalement via Raft en rejoignant
/// le cluster (réplication des entrées de log, ou snapshot complet si ce
/// nœud a trop de retard — voir `ControlPlaneStateMachineStore::install_snapshot`).
///
/// `tool_store` : équivalent de `model_store` pour `ControlPlaneState::tools`
/// (voir [`load_tool_catalog_from_store`]) — pas de chiffrement, une
/// déclaration de tool ne porte aucun secret.
///
/// `expert_store` : équivalent de `model_store` pour
/// `ControlPlaneState::experts` (voir [`load_expert_catalog_from_store`]) —
/// pas de chiffrement, une déclaration d'expert ne porte aucun secret.
///
/// `state_graph_store` : équivalent de `model_store` pour
/// `ControlPlaneState::state_graphs` (voir
/// [`load_state_graph_catalog_from_store`]) — pas de chiffrement, une
/// déclaration de graphe d'états ne porte aucun secret.
///
/// `shutdown` : demande d'arrêt propre (voir `node::MarieHandle::shutdown`)
/// — contrairement à `network::worker::start_worker`, rien à drainer ici
/// (ce nœud ne détient pas de job en vol, seulement l'état répliqué) : la
/// boucle sort dès le signal, coupe la connexion réseau et rend la main.
/// Aucune tentative de transfert de leadership Raft avant de partir (openraft
/// 0.9 n'expose pas cette primitive) — un leader qui s'arrête ainsi laisse
/// le cluster détecter sa disparition par timeout d'élection, comme pour
/// n'importe quel arrêt non gracieux.
///
/// `ready` : signalé avec le [`NetworkClient`] de ce nœud dès la connexion
/// établie, avant que la boucle ci-dessous ne démarre — permet à l'appelant
/// (voir `node::Marie::start`) de le récupérer sans attendre l'arrêt du
/// nœud, qui ne survient normalement jamais.
pub async fn start_control_plane(
    secret: Arc<SecretManager>,
    raft_log_backend: Arc<dyn RaftLogBackend>,
    model_store: Arc<dyn ModelStore>,
    tool_store: Arc<dyn ToolStore>,
    expert_store: Arc<dyn ExpertStore>,
    state_graph_store: Arc<dyn StateGraphStore>,
    mut shutdown: watch::Receiver<bool>,
    ready: oneshot::Sender<NetworkClient>,
) -> Result<(), anyhow::Error> {
    use NodeKind::ControlPlane;

    let log_store = LogStore::new(raft_log_backend); // stocke le log, durablement (voir `RaftLogBackend`)
    let initial_models = load_catalog_from_store(&model_store, &secret).await;
    let initial_tools = load_tool_catalog_from_store(&tool_store).await;
    let initial_experts = load_expert_catalog_from_store(&expert_store).await;
    let initial_state_graphs = load_state_graph_catalog_from_store(&state_graph_store).await;
    let initial_state = ControlPlaneState {
        models: initial_models,
        tools: initial_tools,
        experts: initial_experts,
        state_graphs: initial_state_graphs,
        ..Default::default()
    };
    let state_machine = ControlPlaneStateMachineStore::new(
        initial_state,
        model_store,
        tool_store,
        expert_store,
        state_graph_store,
        secret.clone(),
    ); // applique le log

    let mut reconcile_timer = interval(RECONCILE_INTERVAL);

    let swarm = start_swarm(ControlPlane, |_| {}).await?;
    let local_peer_id = *swarm.local_peer_id();
    let node_id = derive_node_id(&local_peer_id);
    let (actor, client) = NetworkActor::new(swarm, secret.clone());
    let _ = ready.send(client.clone());
    let mut events = client.subscribe_events();

    let network_factory = NetworkFactory::new(client.clone());
    let config = Arc::new(Config::default().validate()?);

    let raft = Raft::new(node_id, config, network_factory, log_store, state_machine.clone()).await?;

    let actor_task = tokio::spawn(actor.run());
    client.subscribe(RPC_REGISTRY_TOPIC);
    // Pour reprendre un agent dont le dernier run a yieldé en attendant une
    // réponse HITL (voir `resume_after_hitl_answer`) dès qu'elle est
    // gossipée, sans attendre le prochain tick de `reconcile`.
    client.subscribe(HITL_TOPIC);

    // Membres connus pour le bootstrap initial du cluster (voir `BOOTSTRAP_DELAY`).
    let mut known_members: BTreeMap<RaftNodeId, RaftNode> = BTreeMap::new();
    known_members.insert(node_id, RaftNode { peer_id: Some(local_peer_id), addr: String::new() });
    let mut bootstrapped = false;
    let bootstrap_delay = tokio::time::sleep(BOOTSTRAP_DELAY);
    tokio::pin!(bootstrap_delay);

    // État local (non répliqué) du scheduler : santé des workers connus et
    // job actuellement assigné à chacun. Reconstruit au fil des healthchecks
    // et de l'état Raft — perdu à chaque redémarrage, sans conséquence sur la
    // correction puisqu'il n'est qu'un cache d'ordonnancement, pas une source
    // de vérité (celle-ci reste `ControlPlaneState`, répliquée par Raft).
    let mut health: HashMap<PeerId, NodeHealth> = HashMap::new();
    let mut assignments: HashMap<JobId, PeerId> = HashMap::new();
    // Watchdog des jobs `Yielded` orphelins (voir [`watch_stuck_yields`]) —
    // même statut que `health`/`assignments` ci-dessus : cache local, perdu
    // sans conséquence sur la correction en cas de redémarrage/changement
    // de leader.
    let mut yield_watch: HashMap<GlobalAgentId, YieldWatch> = HashMap::new();
    let mut rpc_registry = DynamicRpcRegistry::default();

    // `true` une fois `shutdown` fermé sans arrêt explicite demandé (voir
    // `node::MarieHandle`, qui documente qu'abandonner la poignée n'arrête
    // *pas* le nœud) — désactive alors la branche `shutdown.changed()`
    // ci-dessous plutôt que de la laisser se redéclencher en boucle serrée.
    let mut shutdown_gone = false;

    loop {
        tokio::select! {
            Some(event) = events.next() => {
                use crate::network::actor::NetworkEvent::*;
                match event {
                    RequestRemoteProcedureExecution { tx, call, peer } => {

                        let res = execute_rpc(call, &state_machine, &client, &raft, &secret, local_peer_id, &mut rpc_registry, peer).await;
                        let res = match res {
                            Ok(value) => RpcResult::RpcOk(value),
                            Err(error) => RpcResult::RpcErr(error.to_string()),
                        };
                        // `tx` est partagé (voir `RpcReplySlot`) : un seul abonné à
                        // `NetworkEvent` doit effectivement répondre, celui qui réussit
                        // `.take()` en premier (ici, toujours nous — ce nœud est seul à
                        // vouloir répondre aux RPC entrantes).
                        if let Ok(mut tx) = tx.lock() {
                            if let Some(tx) = tx.take() {
                                let _ = tx.send(res);
                            }
                        }
                    },
                    ControlPlanePeerDiscovered { peer_id, addr } => {
                        let peer_node_id = derive_node_id(&peer_id);
                        let peer_node = RaftNode { peer_id: Some(peer_id), addr: addr.map(|a| a.to_string()).unwrap_or_default() };

                        let is_new = known_members.insert(peer_node_id, peer_node.clone()).is_none();

                        // Le bootstrap initial (ci-dessous) se charge déjà des pairs connus
                        // avant `BOOTSTRAP_DELAY`. Une fois le cluster démarré, tout nouveau
                        // pair doit être rattaché dynamiquement.
                        if is_new && bootstrapped {
                            sync_new_peer(&raft, peer_node_id, peer_node).await;
                        }
                    },
                    WorkerPeerDiscovered { peer_id, .. } => {
                        health.entry(peer_id).or_insert_with(|| NodeHealth {
                            last_seen: Instant::now(),
                            rtt: None,
                            status: NodeStatus::Alive,
                        });

                        // Répliqué via Raft : ignoré silencieusement si ce nœud n'est pas
                        // leader, le leader effectif le fera à sa propre découverte du pair.
                        propose_best_effort(&raft, ControlPlaneRequest::RegisterWorker {
                            worker: WorkerInfo { peer_id },
                        }).await;
                    },
                    PersistencyPeerDiscovered { peer_id, .. } => {
                        // Répliqué via Raft, comme `RegisterWorker` : ignoré silencieusement
                        // si ce nœud n'est pas leader, le leader effectif le fera à sa propre
                        // découverte du pair. Voir `ControlPlaneState::persistency_nodes`.
                        propose_best_effort(&raft, ControlPlaneRequest::RegisterPersistency { peer_id }).await;
                    },
                    PeerDisconnected { peer_id } => {
                        // "Si tous les nœuds se déconnectent, cela retire le RPC" : `remove_peer`
                        // ne laisse subsister que les RPC ayant encore au moins un exécuteur.
                        // On gossipe le retrait pour que les autres control planes (qui
                        // n'observent pas forcément la même déconnexion) se mettent à jour aussi.
                        for name in rpc_registry.remove_peer(&peer_id) {
                            let msg = RpcRegistryGossip::Unregister { name, peer_id };
                            let _ = client.publish(RPC_REGISTRY_TOPIC, msg);
                        }

                        // Réaction immédiate si `peer_id` est un worker connu : pas la peine
                        // d'attendre le prochain tick de `reconcile` pour réassigner ses jobs
                        // (voir `reassign_and_unregister_dead_worker`) — notamment utile juste
                        // après l'arrêt propre d'un worker (voir `network::worker::mod`), qui
                        // ferme sa connexion en tout dernier, après avoir déjà rapporté
                        // l'issue de ses jobs (donc généralement un no-op ici, sauf jobs
                        // abandonnés après expiration de son délai de grâce).
                        if raft.current_leader().await == Some(node_id) {
                            let state = state_machine.read_state().await;
                            if state.workers.contains_key(&peer_id) {
                                reassign_and_unregister_dead_worker(&raft, &state, &mut assignments, &mut health, peer_id).await;
                            }
                        }
                    },
                    GossipMessageReceived { topic, data, .. } => {
                        if topic == RPC_REGISTRY_TOPIC {
                            if let Ok(msg) = serde_json::from_slice::<RpcRegistryGossip>(&data) {
                                rpc_registry.apply_gossip(msg);
                            }
                        } else if topic == HITL_TOPIC {
                            // Gossipé à tous les control planes abonnés : sans ce garde, chacun
                            // resoumettrait indépendamment un job pour le même agent (voir
                            // `submit_resume_job`). Seul le leader agit ; les autres l'ignorent,
                            // le leader effectif traitera le même message de son côté.
                            if raft.current_leader().await == Some(node_id) {
                                if let Ok(HitlGossipMessage::Answer(answer)) = serde_json::from_slice(&data) {
                                    resume_after_hitl_answer(&raft, &client, &state_machine, answer).await;
                                }
                            }
                        }
                    },
                }
            }
            () = &mut bootstrap_delay, if !bootstrapped => {
                bootstrapped = true;

                if elect_bootstrap_leader(node_id, &known_members) == node_id {
                    info!(
                        node_id,
                        pairs = known_members.len(),
                        "élu nœud bootstrap Raft (node_id le plus faible parmi les pairs connus) — initialisation du cluster"
                    );
                    if let Err(error) = raft.initialize(known_members.clone()).await {
                        debug!(%error, "initialisation raft ignorée (cluster déjà démarré entre-temps)");
                    }
                } else {
                    info!(
                        node_id,
                        "non élu nœud bootstrap — en attente d'être rattaché au cluster par le nœud élu"
                    );
                }
            }
            _ = reconcile_timer.tick() => {
                reconcile(&raft, &client, &state_machine, &mut health, &mut assignments, &mut yield_watch, node_id).await;
            }
            result = shutdown.changed(), if !shutdown_gone => {
                match result {
                    Ok(()) if *shutdown.borrow() => {
                        info!("arrêt propre du control plane demandé");
                        break;
                    }
                    Ok(()) => {}
                    Err(_) => shutdown_gone = true,
                }
            }
        }
    }

    client.shutdown();
    let _ = actor_task.await;
    Ok(())
}

/// Cycle de contrôle périodique du control plane :
///
/// 1. Healthcheck de tous les workers enregistrés (connectivité libp2p —
///    aucun handler applicatif requis côté worker). Un worker jamais revu
///    depuis le démarrage de *ce* nœud (ex: juste après son propre
///    redémarrage, `health` reparti à zéro) est traité comme "vivant puis
///    mort" dès ce premier tick — voir `NodeHealth`/`is_none_or` plus bas —
///    ce qui couvre aussi bien un worker mort en cours de route qu'un
///    worker resté enregistré sous un `PeerId` d'avant un redémarrage
///    complet du cluster (identité régénérée à chaque démarrage, voir
///    `network::start_swarm`).
/// 2. Si ce nœud est actuellement leader : remise en attente des jobs dont le
///    worker vient d'être détecté injoignable (réassignation, puisée dans
///    `ControlPlaneState::jobs` — pas dans le cache local `assignments`,
///    voir la note dans la boucle correspondante) et retrait de ce worker du
///    registre (voir `ControlPlaneRequest::UnregisterWorker`).
/// 3. Assignation des jobs `Pending` aux workers vivants et disponibles, avec
///    notification best-effort du worker via [`RpcCall::RUN_JOB`].
/// 4. Watchdog des jobs `Yielded` orphelins (voir [`watch_stuck_yields`]).
///
/// Les étapes 2 à 4 écrivent dans l'état répliqué via [`propose_best_effort`]/
/// [`propose_or_forward`], qui n'aboutissent que sur le leader — c'est
/// pourquoi elles sont sautées explicitement plus tôt : inutile de calculer
/// des décisions qui seront de toute façon rejetées ou de faire grossir
/// [`YieldWatch`] sur un nœud qui n'agira jamais dessus.
async fn reconcile(
    raft: &Raft<TypeConfig>,
    client: &NetworkClient,
    state_machine: &ControlPlaneStateMachineStore,
    health: &mut HashMap<PeerId, NodeHealth>,
    assignments: &mut HashMap<JobId, PeerId>,
    yield_watch: &mut HashMap<GlobalAgentId, YieldWatch>,
    node_id: RaftNodeId,
) {
    let state = state_machine.read_state().await;

    let mut newly_dead = Vec::new();
    for peer_id in state.workers.keys().copied() {
        let alive = client.is_connected(peer_id).await.unwrap_or(false);
        let was_alive = health.get(&peer_id).is_none_or(|h| matches!(h.status, NodeStatus::Alive));

        let entry = health.entry(peer_id).or_insert_with(|| NodeHealth {
            last_seen: Instant::now(),
            rtt: None,
            status: NodeStatus::Alive,
        });
        entry.status = if alive { NodeStatus::Alive } else { NodeStatus::Dead };
        if alive {
            entry.last_seen = Instant::now();
        } else if was_alive {
            newly_dead.push(peer_id);
        }
    }

    if raft.current_leader().await != Some(node_id) {
        return;
    }

    for dead_peer in newly_dead {
        reassign_and_unregister_dead_worker(raft, &state, assignments, health, dead_peer).await;
    }

    let busy: HashSet<PeerId> = assignments.values().copied().collect();
    let mut available_workers = state.workers.keys().copied().filter(|peer_id| {
        !busy.contains(peer_id) && matches!(health.get(peer_id).map(|h| &h.status), Some(NodeStatus::Alive))
    });

    for (job_id, record) in state.jobs.iter().filter(|(_, record)| matches!(record.state, JobState::Pending)) {
        let Some(worker) = available_workers.next() else { break };

        assignments.insert(*job_id, worker);
        propose_best_effort(raft, ControlPlaneRequest::AssignJob { job_id: *job_id, worker }).await;

        // Le worker assigné n'est pas garanti d'être celui qui exécutait déjà
        // cette session (réassignation après un healthcheck manqué, ou
        // simplement un nouveau frame de la même session parti sur un autre
        // worker) : il retrouve seul les détenteurs actuels de son état CRDT
        // via [`RpcCall::SESSION_HOLDERS`] avant de reprendre — voir
        // `session::client::SessionClient::acquire`.
        let request = RunJobRequest { job: record.job.clone() };
        let call = RpcCall::new(RpcCall::RUN_JOB, request);
        if let Err(error) = client.rpc_to::<serde_json::Value>(call, worker).await {
            debug!(%error, %job_id, %worker, "notification 'run-job' échouée (le worker n'a peut-être pas encore le handler)");
        }
    }

    watch_stuck_yields(raft, client, &state, yield_watch).await;
}

/// Réassigne les jobs `Scheduled`/`Running` de `dead_peer` (remise en
/// `Pending`) et retire ce worker du registre (voir
/// `ControlPlaneRequest::UnregisterWorker`) — factorisé entre [`reconcile`]
/// (détection périodique, healthcheck) et la réaction immédiate à
/// `NetworkEvent::PeerDisconnected` dans [`start_control_plane`] (détection
/// événementielle, plus rapide qu'attendre le prochain tick — notamment
/// utile juste après l'arrêt propre d'un worker, voir `network::worker::mod`,
/// qui ferme sa connexion réseau en tout dernier, après avoir déjà rapporté
/// l'issue de ses jobs).
///
/// Puisé dans `state.jobs` (répliqué, source de vérité), pas dans le cache
/// local `assignments` : ce dernier ne connaît que les jobs que *ce* nœud a
/// lui-même assignés depuis son propre démarrage — vide juste après un
/// redémarrage du control plane, alors que `state.jobs` (rejoué depuis le
/// log Raft durable, voir `log_store::RaftLogBackend`) porte encore les
/// affectations d'avant l'arrêt. Sans ça, un job resté `Scheduled`/`Running`
/// pour un worker mort avant le redémarrage ne serait jamais détecté : son
/// `PeerId` ne revivra jamais (identité régénérée à chaque démarrage, voir
/// `network::start_swarm`), donc `dead_peer` ne repasserait plus jamais par
/// la transition "vivant puis mort" qui peuple `assignments` en
/// fonctionnement normal.
///
/// N'agit que si ce nœud est actuellement leader (voir
/// [`propose_best_effort`], qui échoue silencieusement sinon) — appelable
/// sans garde préalable par l'appelant.
async fn reassign_and_unregister_dead_worker(
    raft: &Raft<TypeConfig>,
    state: &ControlPlaneState,
    assignments: &mut HashMap<JobId, PeerId>,
    health: &mut HashMap<PeerId, NodeHealth>,
    dead_peer: PeerId,
) {
    let orphaned: Vec<JobId> = state
        .jobs
        .iter()
        .filter(|(_, record)| {
            matches!(&record.state, JobState::Scheduled { worker } | JobState::Running { worker } if *worker == dead_peer)
        })
        .map(|(job_id, _)| *job_id)
        .collect();

    for job_id in orphaned {
        assignments.remove(&job_id);
        debug!(%job_id, %dead_peer, "worker injoignable, remise en attente du job pour réassignation");
        propose_best_effort(raft, ControlPlaneRequest::CommitState { job_id, new_state: JobState::Pending }).await;
    }

    // Le worker ne reviendra jamais sous ce `PeerId` (voir plus haut) : le
    // retirer du registre évite qu'il grossisse indéfiniment au fil des
    // redémarrages du cluster, et qu'on continue à le healthchecker pour
    // rien à chaque tick (voir [`reconcile`]).
    debug!(%dead_peer, "worker injoignable, retrait du registre");
    propose_best_effort(raft, ControlPlaneRequest::UnregisterWorker { peer_id: dead_peer }).await;
    health.remove(&dead_peer);
}

/// Un job `Pending`/`Scheduled`/`Running` existe-t-il déjà pour `agent_id` ?
/// Signale qu'une reprise est déjà en vol (déclenchée par un des chemins
/// événementiels — [`on_job_terminated`]/[`resume_after_hitl_answer`] — ou
/// soumise manuellement) : le watchdog (voir [`watch_stuck_yields`]) doit
/// alors s'effacer plutôt que d'en soumettre une seconde en parallèle.
fn agent_has_active_job(state: &ControlPlaneState, agent_id: GlobalAgentId) -> bool {
    state.jobs.values().any(|record| {
        let JobKind::RunAgent(candidate) = &record.job.kind;
        *candidate == agent_id && matches!(record.state, JobState::Pending | JobState::Scheduled { .. } | JobState::Running { .. })
    })
}

/// Filet de sécurité pour les agents dont le dernier job connu est `Yielded`
/// sans qu'aucune reprise ne soit déjà en vol (voir [`agent_has_active_job`])
/// — couvre les cas où [`on_job_terminated`]/[`resume_after_hitl_answer`]
/// n'ont jamais été déclenchés (message perdu, control plane redémarré
/// avant d'avoir réagi, etc.), pas seulement l'absence de leader qui est
/// déjà gérée par l'appelant (voir [`reconcile`]).
///
/// Volontairement plus prudent pour `WaitingToolReply` (voir
/// [`YieldStatus`]) : reprendre pendant qu'une réponse HITL est encore
/// susceptible d'arriver ferait potentiellement tourner deux fois la même
/// question posée à l'humain — un avertissement est émis, jamais de reprise
/// automatique pour cette raison précise.
async fn watch_stuck_yields(
    raft: &Raft<TypeConfig>,
    client: &NetworkClient,
    state: &ControlPlaneState,
    watch: &mut HashMap<GlobalAgentId, YieldWatch>,
) {
    let mut still_watched = HashSet::new();

    for record in state.jobs.values() {
        let JobKind::RunAgent(agent_id) = &record.job.kind;
        let agent_id = *agent_id;
        let JobState::Yielded { reason } = &record.state else { continue };

        if agent_has_active_job(state, agent_id) {
            continue;
        }

        still_watched.insert(agent_id);
        let entry = watch.entry(agent_id).or_insert_with(|| YieldWatch { first_seen: Instant::now(), attempts: 0 });

        if entry.attempts >= MAX_WATCHDOG_RESUME_ATTEMPTS || entry.first_seen.elapsed() < YIELD_WATCHDOG_TIMEOUT {
            continue;
        }

        entry.attempts += 1;
        entry.first_seen = Instant::now();
        let exhausted = entry.attempts >= MAX_WATCHDOG_RESUME_ATTEMPTS;

        if matches!(reason, YieldStatus::WaitingToolReply { .. }) {
            warn!(?agent_id, ?reason, attempts = entry.attempts, "agent bloqué en attente d'une réponse HITL depuis longtemps — vérifier la passerelle humaine (pas de reprise automatique pour cette raison)");
            continue;
        }

        if exhausted {
            warn!(?agent_id, ?reason, attempts = entry.attempts, "agent probablement bloqué : dernière reprise automatique tentée par le watchdog");
        }
        submit_resume_job(raft, client, agent_id).await;
    }

    // Oublie les agents qui ne sont plus bloqués (repris entre-temps, ou
    // dont le job yieldé a été superseded) — sans quoi `watch` grossirait
    // indéfiniment au fil de la vie du cluster.
    watch.retain(|agent_id, _| still_watched.contains(agent_id));
}

/// Session ciblée par `job`, le cas échéant (dépend de `JobKind`).
fn session_id_of(job: &Job) -> Option<SessionId> {
    match &job.kind {
        JobKind::RunAgent(global_agent_id) => Some(global_agent_id.session_id()),
    }
}

/// Combine les détenteurs connus via l'état Raft
/// (`ControlPlaneState::session_holders`) et ceux tout juste assignés plus
/// tôt dans le même passage de `reconcile`, via le cache local `assignments`
/// — pas encore visibles dans `state`, puisque la proposition Raft
/// correspondante (voir `propose_best_effort`) est asynchrone. Sans cette
/// combinaison, deux frames d'une même session assignés au même tick à des
/// workers différents ne se verraient pas l'un l'autre et créeraient chacun
/// une session CRDT vierge et divergente. `assignments` peut être une map
/// vide en dehors de `reconcile` (voir [`RpcCall::SESSION_HOLDERS`], servi
/// sur demande plutôt que pré-calculé) : cette combinaison n'a alors
/// simplement rien à ajouter à `state.session_holders`.
///
/// Les nœuds `Persistency` connus (voir `ControlPlaneState::persistency_nodes`)
/// sont ajoutés en fin de liste : `SessionClient::acquire` les essaie dans
/// l'ordre, donc les workers vivants (état le plus frais) passent avant ce
/// détenteur de secours, consulté seulement si aucun d'eux ne répond (ou
/// qu'aucun n'est actif — ex: reprise d'une session entre deux jobs).
fn session_holders_for(state: &ControlPlaneState, assignments: &HashMap<JobId, PeerId>, session_id: SessionId) -> Vec<PeerId> {
    let mut holders = state.session_holders(session_id);
    for (job_id, worker) in assignments {
        if state.jobs.get(job_id).and_then(|record| session_id_of(&record.job)) == Some(session_id) {
            holders.insert(*worker);
        }
    }

    let mut ordered: Vec<PeerId> = holders.iter().copied().collect();
    for &peer_id in &state.persistency_nodes {
        if !holders.contains(&peer_id) {
            ordered.push(peer_id);
        }
    }
    ordered
}

/// Équivalent de [`session_holders_for`] pour un workspace : n'a pas de
/// notion propre de détenteur (un workspace n'est jamais directement exécuté
/// par un job, voir `JobKind::RunAgent`), donc dérivé des détenteurs de
/// chacune de ses sessions membres (voir `ControlPlaneState::session_workspaces`)
/// — un worker qui détient une session détient, par construction, le
/// workspace auquel elle appartient (voir
/// `workspace::client::WorkspaceClient::acquire`, appelé juste après
/// `SessionClient::acquire` par le worker qui prend en charge un job). Les
/// nœuds `Persistency` connus sont ajoutés en fin de liste, une seule fois,
/// sur le même principe que [`session_holders_for`].
fn workspace_holders_for(state: &ControlPlaneState, assignments: &HashMap<JobId, PeerId>, workspace_id: WorkspaceId) -> Vec<PeerId> {
    let mut holders = HashSet::new();

    for (&session_id, &session_workspace) in &state.session_workspaces {
        if session_workspace != workspace_id {
            continue;
        }

        holders.extend(state.session_holders(session_id));
        for (job_id, worker) in assignments {
            if state.jobs.get(job_id).and_then(|record| session_id_of(&record.job)) == Some(session_id) {
                holders.insert(*worker);
            }
        }
    }

    let mut ordered: Vec<PeerId> = holders.iter().copied().collect();
    for &peer_id in &state.persistency_nodes {
        if !holders.contains(&peer_id) {
            ordered.push(peer_id);
        }
    }
    ordered
}

/// Élit, de façon déterministe et sans message d'élection, le nœud bootstrap
/// parmi un ensemble de membres connus : celui dont le `node_id` est le plus
/// faible.
///
/// Cette règle ne fonctionne que si tous les nœuds `ControlPlane` convergent
/// vers (approximativement) le même ensemble de pairs pendant `BOOTSTRAP_DELAY`
/// (vrai en pratique sur un même LAN via mDNS, où la découverte est
/// symétrique). Un pair que le nœud élu n'aurait pas encore découvert à ce
/// moment-là rejoint quand même le cluster dès que l'élu le découvre à son
/// tour, via [`sync_new_peer`].
fn elect_bootstrap_leader(local_node_id: RaftNodeId, known_members: &BTreeMap<RaftNodeId, RaftNode>) -> RaftNodeId {
    known_members.keys().copied().min().unwrap_or(local_node_id)
}

/// Rattache un pair `ControlPlane` découvert après le bootstrap initial : d'abord
/// comme learner (réplication du log), puis promu voter. Échoue silencieusement
/// si ce nœud n'est pas (ou plus) leader — c'est alors au leader courant de le faire
/// lorsqu'il recevra le même événement de découverte.
async fn sync_new_peer(raft: &Raft<TypeConfig>, node_id: RaftNodeId, node: RaftNode) {
    if let Err(error) = raft.add_learner(node_id, node, true).await {
        debug!(%error, node_id, "impossible d'ajouter le pair comme learner (probablement pas leader)");
        return;
    }

    if let Err(error) = raft.change_membership(ChangeMembers::AddVoterIds(BTreeSet::from([node_id])), true).await {
        debug!(%error, node_id, "impossible de promouvoir le pair en voter");
    }
}

/// Propose une commande au state machine via Raft, sans garantir qu'elle
/// aboutisse : échoue silencieusement (loggé en `debug`) si ce nœud n'est pas
/// leader. Réservé aux écritures déclenchées en interne (découverte de pair,
/// ordonnancement, réassignation) où aucun appelant RPC n'attend de réponse
/// définitive — le leader effectif, recevant le même déclencheur, retentera
/// l'opération de son côté.
async fn propose_best_effort(raft: &Raft<TypeConfig>, request: ControlPlaneRequest) {
    if let Err(error) = raft.client_write(request).await {
        debug!(%error, "écriture raft ignorée (probablement pas leader)");
    }
}

/// Propose une commande au state machine via Raft en réponse à un appel RPC
/// entrant. Contrairement à [`propose_best_effort`], un appelant attend une
/// réponse définitive : si ce nœud n'est pas leader, l'appel original est
/// transféré au leader connu.
///
/// Si ce leader s'avère injoignable (déconnecté — voir la gestion de
/// `OutboundFailure` dans `NetworkActor`), la RPC doit échouer côté transport
/// avant que l'on décide de retenter : jusqu'à [`FORWARD_RETRY_ATTEMPTS`]
/// essais, chacun réinterrogeant `raft.client_write` (donc le leader courant,
/// qui peut avoir changé entre deux essais — élection en cours, ou ce nœud
/// lui-même vient de le devenir).
async fn propose_or_forward(
    raft: &Raft<TypeConfig>,
    client: &NetworkClient,
    call: RpcCall,
    request: ControlPlaneRequest,
) -> Result<serde_json::Value, anyhow::Error> {
    let mut last_error = None;

    for attempt in 0..FORWARD_RETRY_ATTEMPTS {
        if attempt > 0 {
            sleep(FORWARD_RETRY_DELAY).await;
        }

        match raft.client_write(request.clone()).await {
            Ok(resp) => return Ok(serde_json::to_value(resp.data)?),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(ForwardToLeader { leader_node: Some(leader), .. }))) => {
                let Some(peer_id) = leader.peer_id else {
                    bail!("le leader raft connu n'a pas de peer_id libp2p");
                };
                match client.rpc_to(call.clone(), peer_id).await {
                    Ok(value) => return Ok(value),
                    Err(error) => {
                        debug!(%error, attempt, "relais vers le leader raft échoué, nouvel essai");
                        last_error = Some(error);
                    }
                }
            }
            Err(error) => bail!(error.to_string()),
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("aucun leader raft joignable après {FORWARD_RETRY_ATTEMPTS} tentatives")))
}

/// Soumet un nouveau job pour reprendre `agent_id` après résolution d'une
/// condition d'attente (voir [`resume_after_hitl_answer`]/
/// [`on_job_terminated`]) — jamais une mutation du job yieldé (voir
/// `job::JobState::Yielded`), toujours un nouveau [`Job`] : c'est le
/// découplage voulu entre le cycle de vie d'un job (un run borné, terminal)
/// et celui de l'agent qu'il exécute (voir `job::JobState`).
///
/// Utilise [`propose_or_forward`] plutôt que [`propose_best_effort`] : ce
/// déclenchement n'est pas rejoué périodiquement par tous les nœuds control
/// plane comme [`reconcile`] (où seul le leader doit réussir, les autres
/// s'effacent silencieusement) — c'est un événement ponctuel traité par un
/// seul nœud (celui qui a reçu l'appel RPC ou, pour le gossip HITL, celui
/// élu leader — voir la boucle événementielle dans [`start_control_plane`]).
/// S'il n'est pas leader lui-même, la resoumission doit donc être relayée,
/// pas silencieusement abandonnée.
async fn submit_resume_job(raft: &Raft<TypeConfig>, client: &NetworkClient, agent_id: GlobalAgentId) {
    let job = Job { id: crate::id::generate_id(), kind: JobKind::RunAgent(agent_id) };
    let call = RpcCall::new(RpcCall::SUBMIT_JOB, job.clone());
    if let Err(error) = propose_or_forward(raft, client, call, ControlPlaneRequest::SubmitJob(job)).await {
        debug!(%error, ?agent_id, "resoumission de job échouée");
    }
}

/// Une [`HumanInputAnswer`] vient d'être gossipée (voir [`crate::hitl`]) :
/// reprend tout agent dont le dernier run a yieldé en l'attendant.
/// `YieldStatus::WaitingToolReply::tool_call_id` est réutilisé comme
/// `HumanInputRequest::id` par l'appelant du tool `system/ask-human` (voir
/// `crate::hitl::client::HitlClient::ask`), donc `answer.request_id` suffit
/// à retrouver l'agent concerné directement dans `ControlPlaneState::jobs`,
/// sans registre séparé.
async fn resume_after_hitl_answer(
    raft: &Raft<TypeConfig>,
    client: &NetworkClient,
    state_machine: &ControlPlaneStateMachineStore,
    answer: HumanInputAnswer,
) {
    let state = state_machine.read_state().await;

    let waiting_agents: Vec<GlobalAgentId> = state
        .jobs
        .values()
        .filter_map(|record| {
            let JobKind::RunAgent(agent_id) = &record.job.kind;
            match &record.state {
                JobState::Yielded { reason: YieldStatus::WaitingToolReply { tool_call_id } } if *tool_call_id == answer.request_id => {
                    Some(*agent_id)
                }
                _ => None,
            }
        })
        .collect();

    for agent_id in waiting_agents {
        submit_resume_job(raft, client, agent_id).await;
    }
}

/// `agent_id` a-t-il au moins un job connu ayant abouti à `Completed` ? Voir
/// [`resume_orchestration_parents`] — best effort : plusieurs jobs peuvent
/// exister pour le même agent au fil de ses reprises successives, n'importe
/// lequel à `Completed` suffit à le considérer fini (aucun job terminal ne
/// redevient jamais autre chose, voir `job::JobState`).
fn is_agent_completed(state: &ControlPlaneState, agent_id: GlobalAgentId) -> bool {
    state.jobs.values().any(|record| {
        let JobKind::RunAgent(candidate) = &record.job.kind;
        *candidate == agent_id && matches!(record.state, JobState::Completed { .. })
    })
}

/// `completed_agent_id` vient de conclure avec succès : reprend tout agent
/// orchestrateur qui l'attendait spécifiquement (voir
/// `agent::status::YieldStatus::WaitingChildren`) et dont **tous** les
/// enfants sont désormais `Completed` — pas seulement celui-ci.
async fn resume_orchestration_parents(
    raft: &Raft<TypeConfig>,
    client: &NetworkClient,
    state: &ControlPlaneState,
    completed_agent_id: GlobalAgentId,
) {
    let ready_parents: Vec<GlobalAgentId> = state
        .jobs
        .values()
        .filter_map(|record| {
            let JobKind::RunAgent(parent_id) = &record.job.kind;
            match &record.state {
                JobState::Yielded { reason: YieldStatus::WaitingChildren { children } }
                    if children.contains(&completed_agent_id) && children.iter().all(|child| is_agent_completed(state, *child)) =>
                {
                    Some(*parent_id)
                }
                _ => None,
            }
        })
        .collect();

    for parent_id in ready_parents {
        submit_resume_job(raft, client, parent_id).await;
    }
}

/// Réagit à la conclusion d'un job (voir [`RpcCall::REPORT_JOB_STATE`]) en
/// resoumettant, si besoin, un nouveau job pour l'agent concerné :
///
/// - `Yielded { reason: RunExhausted }` : rien n'est attendu de l'extérieur
///   (voir `agent::status::YieldStatus::RunExhausted`), reprise immédiate.
/// - `Completed` : peut débloquer un agent orchestrateur qui attendait cet
///   agent précisément (voir [`resume_orchestration_parents`]).
///
/// `Yielded { reason: WaitingToolReply }` n'est volontairement pas traité
/// ici : sa résolution dépend d'un événement externe et potentiellement
/// tardif (réponse HITL, voir [`resume_after_hitl_answer`]), pas de la
/// conclusion du job lui-même — rien ne garantit qu'il ait déjà eu lieu au
/// moment où ce job se termine.
async fn on_job_terminated(
    raft: &Raft<TypeConfig>,
    client: &NetworkClient,
    state_machine: &ControlPlaneStateMachineStore,
    job_id: JobId,
    new_state: &JobState,
) {
    let state = state_machine.read_state().await;
    let Some(record) = state.jobs.get(&job_id) else {
        return;
    };
    let JobKind::RunAgent(agent_id) = &record.job.kind;
    let agent_id = *agent_id;

    match new_state {
        JobState::Yielded { reason: YieldStatus::RunExhausted } => {
            submit_resume_job(raft, client, agent_id).await;
        }
        JobState::Completed { .. } => {
            resume_orchestration_parents(raft, client, &state, agent_id).await;
        }
        _ => {}
    }
}

async fn execute_rpc(
    call: RpcCall,
    state_machine: &ControlPlaneStateMachineStore,
    client: &NetworkClient,
    raft: &Raft<TypeConfig>,
    secret: &SecretManager,
    local_peer_id: PeerId,
    rpc_registry: &mut DynamicRpcRegistry,
    peer: PeerId,
) -> Result<serde_json::Value, anyhow::Error> {
    match call.name.as_str() {
        RpcCall::REGISTER_RPC => {
            let name: String = serde_json::from_value(call.args)?;
            info!(%peer, rpc = %name, "RPC enregistrée dynamiquement");
            if rpc_registry.register(name.clone(), peer) {
                let msg = RpcRegistryGossip::Register { name, peer_id: peer };
                let _ = client.publish(RPC_REGISTRY_TOPIC, msg);
            }
            Ok(serde_json::Value::Null)
        }
        RpcCall::AUTH_CHALLENGE => {
            let nonce: [u8; 32] = serde_json::from_value(call.args)?;
            let proof = secret.prove_membership(&local_peer_id, &nonce)?;
            Ok(serde_json::to_value(proof)?)
        }
        RpcCall::GET_MODEL => {
            let model_id: ModelId = serde_json::from_value(call.args)?;
            let decl = state_machine.read_state().await.models.get(&model_id).cloned();

            // La clé API ne doit jamais transiter en clair : chiffrée
            // spécifiquement pour `peer` (voir `SecretManager::derive_node_key`),
            // seul ce nœud pourra la déchiffrer (voir
            // `NetworkClient::decrypt_secret`).
            let encrypted = decl.map(|decl| {
                let node_key = secret.derive_node_key(&peer)?;
                let api_key = secret.encrypt_api_key(decl.api_key(), &node_key)?;
                Ok::<_, SecretError>(decl.encrypt(api_key))
            }).transpose()?;

            Ok(serde_json::to_value(encrypted)?)
        }
        RpcCall::LIST_MODELS => {
            let state = state_machine.read_state().await;
            let node_key = secret.derive_node_key(&peer)?;

            let models = state.models.iter().map(|(id, decl)| {
                let api_key = secret.encrypt_api_key(decl.api_key(), &node_key)?;
                Ok::<_, SecretError>((id.clone(), decl.encrypt(api_key)))
            }).collect::<Result<Vec<(ModelId, EncryptedModel)>, _>>()?;

            Ok(serde_json::to_value(models)?)
        }
        RpcCall::SET_MODEL => {
            let request: SetModelRequest = serde_json::from_value(call.args.clone())?;
            let cp_request = ControlPlaneRequest::SetModel { id: request.id, declaration: request.declaration };
            propose_or_forward(raft, client, call, cp_request).await
        }
        RpcCall::REMOVE_MODEL => {
            let id: ModelId = serde_json::from_value(call.args.clone())?;
            propose_or_forward(raft, client, call, ControlPlaneRequest::RemoveModel { id }).await
        }
        RpcCall::GET_TOOL => {
            let tool_id: ToolId = serde_json::from_value(call.args)?;
            let decl = state_machine.read_state().await.tools.get(&tool_id).cloned();
            Ok(serde_json::to_value(decl)?)
        }
        RpcCall::LIST_TOOLS => {
            let state = state_machine.read_state().await;
            Ok(serde_json::to_value(&*state.tools)?)
        }
        RpcCall::SET_TOOL => {
            let request: SetToolRequest = serde_json::from_value(call.args.clone())?;
            let cp_request = ControlPlaneRequest::SetTool { id: request.id, declaration: request.declaration };
            propose_or_forward(raft, client, call, cp_request).await
        }
        RpcCall::REMOVE_TOOL => {
            let id: ToolId = serde_json::from_value(call.args.clone())?;
            propose_or_forward(raft, client, call, ControlPlaneRequest::RemoveTool { id }).await
        }
        RpcCall::GET_EXPERT => {
            let expert_id: ExpertId = serde_json::from_value(call.args)?;
            let decl = state_machine.read_state().await.experts.get(&expert_id).cloned();
            Ok(serde_json::to_value(decl)?)
        }
        RpcCall::LIST_EXPERTS => {
            let state = state_machine.read_state().await;
            Ok(serde_json::to_value(&*state.experts)?)
        }
        RpcCall::SET_EXPERT => {
            let request: SetExpertRequest = serde_json::from_value(call.args.clone())?;
            let cp_request = ControlPlaneRequest::SetExpert { id: request.id, declaration: request.declaration };
            propose_or_forward(raft, client, call, cp_request).await
        }
        RpcCall::REMOVE_EXPERT => {
            let id: ExpertId = serde_json::from_value(call.args.clone())?;
            propose_or_forward(raft, client, call, ControlPlaneRequest::RemoveExpert { id }).await
        }
        RpcCall::GET_STATE_GRAPH => {
            let state_graph_id: StateGraphId = serde_json::from_value(call.args)?;
            let decl = state_machine.read_state().await.state_graphs.get(&state_graph_id).cloned();
            Ok(serde_json::to_value(decl)?)
        }
        RpcCall::LIST_STATE_GRAPHS => {
            let state = state_machine.read_state().await;
            Ok(serde_json::to_value(&*state.state_graphs)?)
        }
        RpcCall::SET_STATE_GRAPH => {
            let request: SetStateGraphRequest = serde_json::from_value(call.args.clone())?;
            let cp_request = ControlPlaneRequest::SetStateGraph { id: request.id, declaration: request.declaration };
            propose_or_forward(raft, client, call, cp_request).await
        }
        RpcCall::REMOVE_STATE_GRAPH => {
            let id: StateGraphId = serde_json::from_value(call.args.clone())?;
            propose_or_forward(raft, client, call, ControlPlaneRequest::RemoveStateGraph { id }).await
        }
        RpcCall::APPEND_ENTRIES => {
            let rpc: AppendEntriesRequest<TypeConfig> = serde_json::from_value(call.args)?;
            let resp = raft.append_entries(rpc).await.map_err(|error| anyhow::anyhow!(error.to_string()))?;
            Ok(serde_json::to_value(resp)?)
        }
        RpcCall::INSTALL_SNAPSHOT => {
            let rpc: InstallSnapshotRequest<TypeConfig> = serde_json::from_value(call.args)?;
            let resp = raft.install_snapshot(rpc).await.map_err(|error| anyhow::anyhow!(error.to_string()))?;
            Ok(serde_json::to_value(resp)?)
        }
        RpcCall::VOTE => {
            let rpc: VoteRequest<RaftNodeId> = serde_json::from_value(call.args)?;
            let resp = raft.vote(rpc).await.map_err(|error| anyhow::anyhow!(error.to_string()))?;
            Ok(serde_json::to_value(resp)?)
        }
        RpcCall::SUBMIT_JOB => {
            let job: Job = serde_json::from_value(call.args.clone())?;
            propose_or_forward(raft, client, call, ControlPlaneRequest::SubmitJob(job)).await
        }
        RpcCall::SESSION_HOLDERS => {
            let session_id: SessionId = serde_json::from_value(call.args)?;
            let state = state_machine.read_state().await;
            // Lecture seule, servie depuis l'état Raft local : pas besoin
            // d'être le leader (comme `GET_MODEL`/`LIST_TOOLS`, ...), un
            // léger retard de réplication sur un suiveur est sans
            // conséquence ici. `assignments` (le cache des affectations pas
            // encore visibles dans `state`, propre à la passe courante de
            // `reconcile`) n'a pas de sens en dehors de `reconcile` : une
            // map vide suffit, `session_holders_for` retombe alors sur
            // `ControlPlaneState::session_holders` seul.
            let holders = session_holders_for(&state, &HashMap::new(), session_id);
            Ok(serde_json::to_value(holders)?)
        }
        RpcCall::WORKSPACE_HOLDERS => {
            let workspace_id: WorkspaceId = serde_json::from_value(call.args)?;
            let state = state_machine.read_state().await;
            // Lecture seule, même raisonnement que `SESSION_HOLDERS` ci-dessus.
            let holders = workspace_holders_for(&state, &HashMap::new(), workspace_id);
            Ok(serde_json::to_value(holders)?)
        }
        RpcCall::SET_SESSION_WORKSPACE => {
            let request: SetSessionWorkspaceRequest = serde_json::from_value(call.args.clone())?;
            let cp_request = ControlPlaneRequest::SetSessionWorkspace { session_id: request.session_id, workspace_id: request.workspace_id };
            propose_or_forward(raft, client, call, cp_request).await
        }
        RpcCall::SESSION_WORKSPACE => {
            let session_id: SessionId = serde_json::from_value(call.args)?;
            let state = state_machine.read_state().await;
            // Lecture seule, même raisonnement que `SESSION_HOLDERS` ci-dessus.
            let workspace_id = state.session_workspaces.get(&session_id).copied();
            Ok(serde_json::to_value(workspace_id)?)
        }
        RpcCall::REPORT_JOB_STATE => {
            let report: JobStateReport = serde_json::from_value(call.args.clone())?;
            let job_id = report.job_id;
            let new_state = report.state.clone();
            let request = ControlPlaneRequest::CommitState { job_id, new_state: report.state };
            let result = propose_or_forward(raft, client, call, request).await;

            // Contrairement au gossip HITL (voir la note dans la boucle
            // événementielle plus haut), ce déclenchement est déjà à flux
            // unique : un seul nœud control plane reçoit cette RPC d'un
            // worker donné, jamais tous — pas besoin d'un garde "leader
            // seulement" ici, `submit_resume_job` (appelé transitivement)
            // relaie déjà lui-même vers le leader si ce nœud ne l'est pas.
            if result.is_ok() {
                on_job_terminated(raft, client, state_machine, job_id, &new_state).await;
            }

            result
        }
        name => {
            // Pas une RPC connue nativement : peut-être une RPC enregistrée
            // dynamiquement par un pair (voir `RpcCall::REGISTER_RPC`).
            let name = name.to_string();
            let mut last_error = None;

            for attempt in 0..FORWARD_RETRY_ATTEMPTS {
                if attempt > 0 {
                    sleep(FORWARD_RETRY_DELAY).await;
                }

                // Requêté à chaque essai : reflète la purge de l'essai précédent
                // (ci-dessous) ainsi que tout nouvel exécuteur apparu entre-temps
                // (enregistrement direct ou gossip d'un autre control plane).
                let Some(executors) = rpc_registry.executors_for(&name).cloned() else {
                    bail!("unmanaged remote procedure {name}");
                };

                match forward_race(client, &executors, call.clone()).await {
                    Ok(value) => return Ok(value),
                    Err(error) => {
                        // Aucun exécuteur n'a répondu : probablement des entrées
                        // périmées (apprises par gossip d'un nœud control plane
                        // depuis disparu sans avoir pu gossiper leur retrait — voir
                        // `RpcRegistryGossip`). On les purge localement plutôt que de
                        // rester bloqué dessus. Pas re-gossipé : un échec de relais
                        // isolé n'est pas une preuve aussi définitive qu'une
                        // déconnexion observée directement.
                        debug!(%error, %name, attempt, "relais RPC dynamique échoué, nouvel essai");
                        for peer_id in &executors {
                            rpc_registry.remove_executor(&name, peer_id);
                        }
                        last_error = Some(error);
                    }
                }
            }

            Err(last_error.unwrap_or_else(|| anyhow::anyhow!("unmanaged remote procedure {name}")))
        }
    }
}
