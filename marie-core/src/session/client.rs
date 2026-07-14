use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::bail;
use futures::{Stream, StreamExt as _};
use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use crate::id::ID;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::{RwLock, broadcast};
use tokio_stream::wrappers::{BroadcastStream, errors::BroadcastStreamRecvError};
use tracing::debug;
use yrs::{StateVector, updates::{decoder::Decode, encoder::Encode}};

use crate::{
    agent::{frame::AgentFrame, status::AgentStatus},
    mode::SessionMode,
    network::{actor::{NetworkService, NetworkEvent, NetworkEventHandler}, cp::rpc::{RpcCall, SessionFetchRequest}},
    persistency::{
        filesystem::{FileSystem as _, OpenOptions, VFS},
        var::{SessionVarStore, VarStore},
        vfs::WorkspaceVfs,
    },
    session::{SessionApi, SessionId, SessionLog, crdt::YrsSession, sync::{SESSION_SYNC_TOPIC, SessionSyncMessage}},
    workspace::WorkspaceId,
};

/// Capacité du canal de diffusion locale des [`SessionEvent`] — des
/// événements de cycle de vie, pas un flux de contenu streamé (voir
/// `agent_events` côté serveur d'édition, bien plus verbeux) : une capacité
/// modeste suffit à laisser un abonnant temporairement en retard rattraper
/// son retard sans perdre d'événement.
const SESSION_EVENTS_CAPACITY: usize = 256;

/// Topic gossipsub (`node_gossip`) sur lequel les événements de session sont
/// diffusés à tout pair intéressé (autre worker préparant une reprise,
/// control plane, outillage d'observation) — voir [`SessionClient::emit`] et
/// [`SessionClient::new`]. Ne transporte que des événements de cycle de vie
/// (petits, peu fréquents), jamais le contenu de la session elle-même (voir
/// `session::sync::SESSION_SYNC_TOPIC` pour ça).
const SESSION_EVENTS_TOPIC: &str = "marie/worker/session-events/1.0.0";

/// Événement de cycle de vie d'une session, diffusé localement (voir
/// [`SessionClient::subscribe`]) et gossipé au reste du cluster (voir
/// [`SESSION_EVENTS_TOPIC`]). Permet de suivre l'avancement d'un agent ou la
/// vie d'une session sans avoir à ré-interroger le CRDT à chaque tick.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SessionEvent {
    /// La session est désormais connue localement — créée vierge ou
    /// synchronisée depuis un détenteur précédent (voir
    /// [`SessionClient::acquire`]).
    Created { session_id: SessionId },
    /// Le statut d'un frame de la session vient de changer (voir
    /// [`SessionClient::set_frame_status`]).
    FrameStatusChanged { session_id: SessionId, local_id: ID, status: AgentStatus },
    /// Une entrée a été ajoutée au journal de la session (voir
    /// [`SessionClient::push_log`]).
    LogAppended { session_id: SessionId, log: SessionLog },
    /// La pile de modes de la session vient de changer, par empilage ou
    /// dépilage (voir [`SessionClient::push_mode`]/[`SessionClient::pop_mode`])
    /// — `mode` est le nouveau sommet de pile, [`SessionMode::Simple`] si
    /// elle est désormais vide.
    ModeChanged { session_id: SessionId, mode: SessionMode },
    /// La session n'est plus détenue localement par ce worker (voir
    /// [`SessionClient::remove`]).
    Removed { session_id: SessionId },
    /// Une valeur du store clé-valeur libre de la session a été définie
    /// (créée ou remplacée, voir [`SessionClient::set_value`]) — backend de
    /// `/session/var` dans le VFS (voir `persistency::var::VarFileSystem`).
    ValueChanged { session_id: SessionId, key: String, value: Value },
    /// Une valeur du store clé-valeur libre de la session a été retirée
    /// (voir [`SessionClient::remove_value`]).
    ValueRemoved { session_id: SessionId, key: String },
}

/// Flux de [`SessionEvent`] retourné par [`SessionClient::subscribe`] —
/// encapsule le `broadcast::Receiver` sous-jacent (même motif que
/// `network::actor::NetworkEventHandler` pour `NetworkEvent`) : un abonné en
/// retard perd les événements les plus anciens (`Lagged`), absorbé
/// silencieusement ici plutôt que remonté comme une erreur — un événement de
/// cycle de vie manqué n'est jamais fatal (voir [`SESSION_SYNC_TOPIC`] pour
/// la synchronisation du contenu, qui elle ne dépend pas de ce flux).
pub struct SessionEventHandler(BroadcastStream<SessionEvent>);

impl Stream for SessionEventHandler {
    type Item = SessionEvent;

    fn poll_next(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        loop {
            return match std::pin::Pin::new(&mut self.0).poll_next(cx) {
                std::task::Poll::Ready(Some(Ok(event))) => std::task::Poll::Ready(Some(event)),
                std::task::Poll::Ready(Some(Err(BroadcastStreamRecvError::Lagged(skipped)))) => {
                    debug!(skipped, "abonné SessionEvent en retard, événements perdus");
                    continue;
                }
                std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
                std::task::Poll::Pending => std::task::Poll::Pending,
            };
        }
    }
}

/// Session détenue localement, avec le curseur nécessaire pour ne publier
/// que les deltas (voir [`SessionClient::diff_and_bump`]) plutôt que tout le
/// document à chaque mutation.
struct SessionEntry {
    session: YrsSession,
    /// Vecteur d'état au dernier envoi (diffusion locale ou réception d'un
    /// diff distant) — la prochaine publication n'envoie que ce qui a changé
    /// depuis.
    last_synced: StateVector,
}

impl SessionEntry {
    fn new(session: YrsSession) -> Self {
        let last_synced = session.state_vector();
        Self { session, last_synced }
    }
}

/// Pont entre le stockage local des sessions CRDT (voir
/// `session::crdt::YrsSession`) et le réseau : centralise la prise en charge
/// d'une session (création, ou synchronisation depuis un détenteur déjà
/// actif) et son maintien à jour en continu — une fois acquise, une session
/// reste synchronisée avec tous ses détenteurs actifs (potentiellement
/// plusieurs, si plusieurs frames de la même session tournent en parallèle
/// sur des workers différents) via [`SESSION_SYNC_TOPIC`], pas seulement au
/// moment de l'acquisition. Sert aussi les demandes
/// [`RpcCall::FETCH_SESSION`] d'un pair qui démarre.
///
/// Bon marché à cloner (comme [`NetworkClient`]) : pensé pour être threadé
/// dans les tâches de fond au même titre que lui, plutôt que de faire
/// transiter chaque accès par la boucle mono-thread de `NetworkActor`.
#[derive(Clone)]
pub struct SessionClient {
    network: NetworkService,
    sessions: Arc<RwLock<HashMap<SessionId, SessionEntry>>>,
    events: broadcast::Sender<SessionEvent>,
    /// Composition du VFS d'une session (voir [`Self::vfs`]) — `/var`/`/files`
    /// portée workspace viennent de là, `/session` est monté par-dessus par
    /// [`Self::vfs`] lui-même (voir la doc de [`WorkspaceVfs`] pour pourquoi
    /// ce type ne connaît pas `SessionClient` en retour).
    workspace_vfs: WorkspaceVfs,
    /// Nœuds `Persistency` découverts directement par ce nœud (voir
    /// `NetworkEvent::PersistencyPeerDiscovered`) — indépendant de ce que
    /// sait le control plane, pour amorcer une session même si celui-ci est
    /// injoignable (voir [`Self::acquire`]).
    known_persistency_peers: Arc<RwLock<HashSet<PeerId>>>,
}

impl SessionClient {
    /// S'abonne lui-même au flux d'événements réseau de `network` (voir
    /// `NetworkClient::subscribe_events`) et démarre sa propre tâche de fond
    /// pour traiter les messages gossipés sur [`SESSION_EVENTS_TOPIC`] (réémis
    /// aux abonnés locaux, voir [`Self::subscribe`]) et [`SESSION_SYNC_TOPIC`]
    /// (fusionnés dans les sessions détenues localement) — l'appelant n'a donc
    /// pas besoin de savoir filtrer ni forwarder quoi que ce soit lui-même.
    pub fn new(network: NetworkService, workspace_vfs: WorkspaceVfs) -> Self {
        let (events, _) = broadcast::channel(SESSION_EVENTS_CAPACITY);
        network.subscribe(SESSION_EVENTS_TOPIC);
        network.subscribe(SESSION_SYNC_TOPIC);

        let sessions = Arc::new(RwLock::new(HashMap::new()));
        let known_persistency_peers = Arc::new(RwLock::new(HashSet::new()));
        tokio::spawn(ingest_network_events(
            network.subscribe_events(),
            events.clone(),
            sessions.clone(),
            known_persistency_peers.clone(),
        ));

        Self { network, sessions, events, workspace_vfs, known_persistency_peers }
    }

    /// VFS complet d'une session : résout son workspace (voir
    /// [`RpcCall::SESSION_WORKSPACE`]) puis compose `/var`/`/files` (portée
    /// workspace, voir [`WorkspaceVfs::vfs`]) et `/session/var`/`/session/files`
    /// (portée session, voir [`WorkspaceVfs::mount_session`]) — le store
    /// `/session/var` est adossé à `self` (voir [`SessionVarStore`]), pas de
    /// pré-requis d'[`Self::acquire`] préalable (comme l'ancien
    /// `SessionFilesystem`, déjà un stockage partagé).
    ///
    /// Échoue si la session n'est rattachée à aucun workspace : depuis
    /// `workspace::client::WorkspaceClient::create_session` (seul point de
    /// création d'une session), c'est le signe d'un identifiant invalide ou
    /// jamais créé, pas d'une session légitime en attente de rattachement —
    /// voir la même vérification dans [`Self::acquire`].
    pub async fn vfs(&self, session_id: SessionId) -> anyhow::Result<Arc<VFS>> {
        let workspace_id = self.workspace_of(session_id).await?;
        let var: Arc<dyn VarStore> = Arc::new(SessionVarStore::new(self.clone(), session_id));
        self.workspace_vfs.mount_session(workspace_id, session_id, var).await
    }

    /// Workspace de `session_id` (voir [`RpcCall::SESSION_WORKSPACE`]) —
    /// échoue si elle n'en a aucun (voir [`Self::vfs`]/[`Self::acquire`]).
    /// Publique en plus de son usage interne : c'est aussi ce que
    /// `network::worker::mod::run_orchestration` interroge pour créer un
    /// enfant dans le même workspace que le frame qui délègue la tâche.
    pub async fn workspace_of(&self, session_id: SessionId) -> anyhow::Result<WorkspaceId> {
        let workspace_id: Option<WorkspaceId> = self.network.rpc(RpcCall::new(RpcCall::SESSION_WORKSPACE, session_id)).await.unwrap_or_default();
        workspace_id.ok_or_else(|| {
            anyhow::anyhow!("session {session_id} non rattachée à un workspace (voir WorkspaceClient::create_session)")
        })
    }

    /// S'abonne aux événements de cycle de vie des sessions — les siens
    /// comme ceux gossipés par d'autres pairs (voir [`SessionEvent`]).
    /// Chaque abonné reçoit sa propre copie ; les événements émis avant
    /// l'abonnement ne sont pas rejoués.
    pub fn subscribe(&self) -> SessionEventHandler {
        SessionEventHandler(BroadcastStream::new(self.events.subscribe()))
    }

    /// Diffuse `event` aux abonnés locaux (voir [`Self::subscribe`]) et au
    /// reste du cluster via gossipsub (voir [`SESSION_EVENTS_TOPIC`]) —
    /// best-effort dans les deux cas : ni l'absence d'abonné local, ni
    /// l'absence de pair dans le mesh gossipsub, ne fait échouer l'opération
    /// qui a produit l'événement.
    fn emit(&self, event: SessionEvent) {
        if let Err(error) = self.network.publish(SESSION_EVENTS_TOPIC, &event) {
            debug!(%error, ?event, "publication gossip de l'événement de session échouée");
        }
        let _ = self.events.send(event);
    }

    /// Diffuse `diff` aux autres détenteurs de `session_id` via
    /// [`SESSION_SYNC_TOPIC`] — best-effort, comme [`Self::emit`].
    fn publish_sync(&self, session_id: SessionId, diff: Vec<u8>) {
        let message = SessionSyncMessage { session_id, diff };
        if let Err(error) = self.network.publish(SESSION_SYNC_TOPIC, &message) {
            debug!(%error, %session_id, "publication du diff de session échouée");
        }
    }

    /// Prend en charge la session ciblée par le job en cours : localise une
    /// copie existante (voir [`Self::locate_session`]) et s'y synchronise,
    /// ou en crée une vierge si aucune n'est trouvée (ce worker est le
    /// premier à exécuter un frame de cette session). Ne fait rien si elle
    /// est déjà détenue localement (réexécution sur ce même worker) — dans ce
    /// cas [`SessionEvent::Created`] n'est pas réémis.
    ///
    /// Créer une session vierge exige qu'elle soit déjà rattachée à un
    /// workspace (voir [`Self::workspace_of`]) — une session ne naît que via
    /// `workspace::client::WorkspaceClient::create_session`, jamais
    /// implicitement ici : sans ce garde-fou, un `session_id` invalide ou
    /// mal orthographié produirait silencieusement une session fantôme sans
    /// workspace, avec `/session/files` durablement indisponible (voir
    /// [`Self::vfs`]). Ce garde-fou ne s'applique qu'à la création — une
    /// session déjà détenue par un pair (voir [`Self::locate_session`]) est
    /// acceptée telle quelle, son workspace ayant déjà été validé par qui
    /// l'a créée.
    ///
    /// Une fois acquise, la session reste à jour en continu via
    /// [`SESSION_SYNC_TOPIC`] (voir [`ingest_network_events`]) : inutile de
    /// fusionner toutes les copies trouvées, une seule suffit pour amorcer,
    /// les diffs des autres détenteurs actifs arriveront par ce flux.
    pub async fn acquire(&self, session_id: SessionId) -> anyhow::Result<()> {
        if self.sessions.read().await.contains_key(&session_id) {
            return Ok(());
        }

        let session = match self.locate_session(session_id).await {
            Some(session) => session,
            None => {
                self.workspace_of(session_id).await?;
                YrsSession::new(session_id)
            }
        };

        self.sessions.write().await.insert(session_id, SessionEntry::new(session));
        self.emit(SessionEvent::Created { session_id });
        Ok(())
    }

    /// Localise une copie existante de `session_id`, dans l'ordre : d'abord
    /// auprès du control plane (voir [`RpcCall::SESSION_HOLDERS`], qui
    /// connaît les workers l'exécutant actuellement ainsi que les nœuds
    /// `Persistency` déclarés, essayés dans l'ordre qu'il indique), puis, si
    /// le control plane est injoignable ou ne connaît aucun détenteur
    /// utilisable, auprès des nœuds `Persistency` découverts directement par
    /// ce nœud (voir [`Self::known_persistency_peers`] et
    /// `NetworkEvent::PersistencyPeerDiscovered`) — indépendant du control
    /// plane, pour permettre une reprise à froid même s'il est hors service.
    /// `None` si aucune de ces pistes n'aboutit.
    async fn locate_session(&self, session_id: SessionId) -> Option<YrsSession> {
        let cp_holders: Vec<PeerId> = self
            .network
            .rpc(RpcCall::new(RpcCall::SESSION_HOLDERS, session_id))
            .await
            .unwrap_or_else(|error| {
                debug!(%error, %session_id, "interrogation du control plane pour les détenteurs de session échouée");
                Vec::new()
            });

        if !cp_holders.is_empty() {
            match self.fetch_from_any(session_id, &cp_holders).await {
                Ok(session) => return Some(session),
                Err(error) => debug!(%error, %session_id, "aucun détenteur indiqué par le control plane n'a répondu"),
            }
        }

        let persistency_peers: Vec<PeerId> = self.known_persistency_peers.read().await.iter().copied().collect();
        if !persistency_peers.is_empty() {
            match self.fetch_from_any(session_id, &persistency_peers).await {
                Ok(session) => return Some(session),
                Err(error) => debug!(%error, %session_id, "aucun nœud persistency connu localement n'a répondu"),
            }
        }

        None
    }

    /// Reconstruit le [`AgentFrame`] `local_id` à partir de l'état synchronisé
    /// de la session — `None` si la session elle-même est inconnue de ce
    /// worker (voir [`Self::acquire`]) ou si elle ne contient pas encore ce
    /// frame (voir `session::crdt::YrsSession::put_frame`, qui doit avoir été
    /// appelé au moins une fois, sur ce worker ou un pair dont le diff a
    /// depuis été reçu, avant qu'un frame existe).
    pub async fn frame(&self, session_id: SessionId, local_id: ID) -> Option<AgentFrame> {
        self.sessions.read().await.get(&session_id)?.session.frame(local_id)
    }

    /// Écrit (crée) l'état intégral du frame `local_id` — seul point d'entrée
    /// pour faire naître un frame en mode [`SessionMode::Simple`] (voir
    /// `network::worker::mod::run_simple`, qui échoue tant qu'aucun frame
    /// n'existe sous ce `local_id`) ou pour amorcer l'enfant d'un cycle
    /// d'orchestration (voir `network::worker::mod::run_orchestration`).
    /// Diffuse [`SessionEvent::FrameStatusChanged`] avec le statut du frame
    /// tel qu'écrit (même événement qu'un changement de statut ordinaire :
    /// un frame qui vient de naître en a bien un, `AgentStatus::Initial`
    /// typiquement) et publie le delta CRDT résultant.
    pub async fn put_frame(&self, session_id: SessionId, local_id: ID, frame: &AgentFrame) -> anyhow::Result<()> {
        let status = frame.status.clone();
        let diff = {
            let mut sessions = self.sessions.write().await;
            let Some(entry) = sessions.get_mut(&session_id) else {
                bail!("session {session_id} inconnue de ce worker");
            };
            entry.session.put_frame(local_id, frame)?;
            self.diff_and_bump(entry)
        };

        self.publish_sync(session_id, diff);
        self.emit(SessionEvent::FrameStatusChanged { session_id, local_id, status });
        Ok(())
    }

    /// Change le statut d'un frame connu de la session (voir
    /// [`crate::agent::status::AgentStatus`]), diffuse
    /// [`SessionEvent::FrameStatusChanged`] et publie le delta CRDT résultant
    /// (voir [`SESSION_SYNC_TOPIC`]).
    pub async fn set_frame_status(&self, session_id: SessionId, local_id: ID, status: AgentStatus) -> anyhow::Result<()> {
        let diff = {
            let mut sessions = self.sessions.write().await;
            let Some(entry) = sessions.get_mut(&session_id) else {
                bail!("session {session_id} inconnue de ce worker");
            };
            entry.session.set_status(local_id, &status)?;
            self.diff_and_bump(entry)
        };

        self.publish_sync(session_id, diff);
        self.emit(SessionEvent::FrameStatusChanged { session_id, local_id, status });
        Ok(())
    }

    /// Ajoute une entrée au contexte d'un frame connu de la session (voir
    /// [`crate::agent::context::ContextEntry`], ex: un nouveau message
    /// modèle/tool produit par [`crate::agent::run`]) et publie le delta CRDT
    /// résultant. Contrairement à [`Self::set_frame_status`]/[`Self::push_log`],
    /// n'émet pas de [`SessionEvent`] : le contexte d'un frame n'est pas un
    /// événement de cycle de vie (peu fréquent, intéressant à suivre en soi),
    /// mais du contenu de session ordinaire — déjà diffusé aux autres
    /// détenteurs via le diff CRDT publié ci-dessous, sans avoir besoin d'un
    /// canal séparé.
    pub async fn push_context_entry(&self, session_id: SessionId, local_id: ID, entry: crate::agent::context::ContextEntry) -> anyhow::Result<()> {
        let diff = {
            let mut sessions = self.sessions.write().await;
            let Some(entry_slot) = sessions.get_mut(&session_id) else {
                bail!("session {session_id} inconnue de ce worker");
            };
            entry_slot.session.push_context_entry(local_id, &entry)?;
            self.diff_and_bump(entry_slot)
        };

        self.publish_sync(session_id, diff);
        Ok(())
    }

    /// Ajoute une entrée au journal de la session (voir [`SessionLog`]),
    /// diffuse [`SessionEvent::LogAppended`] et publie le delta CRDT résultant
    /// (voir [`SESSION_SYNC_TOPIC`]).
    pub async fn push_log(&self, session_id: SessionId, log: SessionLog) -> anyhow::Result<()> {
        let diff = {
            let mut sessions = self.sessions.write().await;
            let Some(entry) = sessions.get_mut(&session_id) else {
                bail!("session {session_id} inconnue de ce worker");
            };
            entry.session.push_log(&log)?;
            self.diff_and_bump(entry)
        };

        self.publish_sync(session_id, diff);
        self.emit(SessionEvent::LogAppended { session_id, log });
        Ok(())
    }

    /// Empile `mode` au sommet de la pile de modes de la session (voir
    /// `session::crdt::YrsSession::push_mode`, qui rejette
    /// [`SessionMode::Simple`]), diffuse [`SessionEvent::ModeChanged`] et
    /// publie le delta CRDT résultant. C'est ce que le point d'entrée d'un
    /// tool `system/push-mode` (voir [`crate::mode::PUSH_MODE_TOOL`])
    /// appellerait pour un agent qui souhaite entrer en orchestration ou
    /// suivre un graphe d'états ; un humain (via `node::Marie::join`) peut y
    /// appeler la même méthode directement, sans passer par un tool.
    pub async fn push_mode(&self, session_id: SessionId, mode: SessionMode) -> anyhow::Result<()> {
        let diff = {
            let mut sessions = self.sessions.write().await;
            let Some(entry) = sessions.get_mut(&session_id) else {
                bail!("session {session_id} inconnue de ce worker");
            };
            entry.session.push_mode(&mode)?;
            self.diff_and_bump(entry)
        };

        self.publish_sync(session_id, diff);
        self.emit(SessionEvent::ModeChanged { session_id, mode });
        Ok(())
    }

    /// Remplace le mode au sommet de la pile par `mode`, sans changer la
    /// profondeur de la pile (voir `session::crdt::YrsSession::update_current_mode`) —
    /// pour persister la *progression* d'un mode déjà empilé (ex:
    /// `mode::state_graph::StateGraph::current` après un `advance`, voir
    /// `network::worker::mod::drive_state_graph`), diffuse
    /// [`SessionEvent::ModeChanged`] et publie le delta CRDT résultant.
    /// Contrairement à [`Self::push_mode`], n'a pas vocation à être appelée
    /// via un tool : c'est un détail d'implémentation de la boucle de
    /// pilotage d'un mode, pas une action que l'agent ou un humain choisit.
    pub async fn update_current_mode(&self, session_id: SessionId, mode: SessionMode) -> anyhow::Result<()> {
        let diff = {
            let mut sessions = self.sessions.write().await;
            let Some(entry) = sessions.get_mut(&session_id) else {
                bail!("session {session_id} inconnue de ce worker");
            };
            entry.session.update_current_mode(&mode)?;
            self.diff_and_bump(entry)
        };

        self.publish_sync(session_id, diff);
        self.emit(SessionEvent::ModeChanged { session_id, mode });
        Ok(())
    }

    /// Dépile le mode courant de la session (voir
    /// `session::crdt::YrsSession::pop_mode`) et diffuse
    /// [`SessionEvent::ModeChanged`] avec le nouveau sommet de pile — ne
    /// fait rien (retourne `Ok(None)`) si la pile était déjà vide, aucun
    /// événement n'est alors émis.
    pub async fn pop_mode(&self, session_id: SessionId) -> anyhow::Result<Option<SessionMode>> {
        let (popped, diff, current) = {
            let mut sessions = self.sessions.write().await;
            let Some(entry) = sessions.get_mut(&session_id) else {
                bail!("session {session_id} inconnue de ce worker");
            };

            let Some(popped) = entry.session.pop_mode()? else {
                return Ok(None);
            };
            let diff = self.diff_and_bump(entry);
            let current = entry.session.current_mode();
            (popped, diff, current)
        };

        self.publish_sync(session_id, diff);
        self.emit(SessionEvent::ModeChanged { session_id, mode: current });
        Ok(Some(popped))
    }

    /// Mode courant de la session (sommet de la pile) — [`SessionMode::Simple`]
    /// si elle est vide, ou si cette session est inconnue de ce worker.
    pub async fn current_mode(&self, session_id: SessionId) -> SessionMode {
        match self.sessions.read().await.get(&session_id) {
            Some(entry) => entry.session.current_mode(),
            None => SessionMode::Simple,
        }
    }

    /// Définit une valeur du store clé-valeur libre de la session (voir
    /// [`SessionApi::set_value`]), diffuse [`SessionEvent::ValueChanged`] et
    /// publie le delta CRDT résultant.
    pub async fn set_value(&self, session_id: SessionId, key: String, value: Value) -> anyhow::Result<()> {
        let diff = {
            let mut sessions = self.sessions.write().await;
            let Some(entry) = sessions.get_mut(&session_id) else {
                bail!("session {session_id} inconnue de ce worker");
            };
            entry.session.set_value(&key, &value)?;
            self.diff_and_bump(entry)
        };

        self.publish_sync(session_id, diff);
        self.emit(SessionEvent::ValueChanged { session_id, key, value });
        Ok(())
    }

    /// Retire une clé du store clé-valeur libre de la session (voir
    /// [`SessionApi::remove_value`]), diffuse [`SessionEvent::ValueRemoved`]
    /// et publie le delta CRDT résultant.
    pub async fn remove_value(&self, session_id: SessionId, key: String) -> anyhow::Result<()> {
        let diff = {
            let mut sessions = self.sessions.write().await;
            let Some(entry) = sessions.get_mut(&session_id) else {
                bail!("session {session_id} inconnue de ce worker");
            };
            entry.session.remove_value(&key)?;
            self.diff_and_bump(entry)
        };

        self.publish_sync(session_id, diff);
        self.emit(SessionEvent::ValueRemoved { session_id, key });
        Ok(())
    }

    /// Valeur associée à `key`, ou `None` si absente ou si la session est
    /// inconnue de ce worker.
    pub async fn value(&self, session_id: SessionId, key: &str) -> Option<Value> {
        self.sessions.read().await.get(&session_id)?.session.value(key)
    }

    /// Snapshot complet du store clé-valeur libre, ou vide si la session est
    /// inconnue de ce worker.
    pub async fn values(&self, session_id: SessionId) -> HashMap<String, Value> {
        match self.sessions.read().await.get(&session_id) {
            Some(entry) => entry.session.values(),
            None => HashMap::new(),
        }
    }

    /// Chemin `/session/files/{path}` dans le VFS (voir [`Self::vfs`]) — les
    /// quatre méthodes fichier ci-dessous n'opèrent que sur ce sous-arbre.
    fn file_path(path: &str) -> String {
        format!("/session/files/{}", path.trim_start_matches('/'))
    }

    /// Contenu du fichier `path` de la session, ou `None` s'il n'existe pas
    /// — ou si la session n'a pas encore de workspace (voir [`Self::vfs`],
    /// `/session/files` n'est alors pas monté).
    pub async fn read_file(&self, session_id: SessionId, path: &str) -> anyhow::Result<Option<Vec<u8>>> {
        let vfs = self.vfs(session_id).await?;
        let Ok(mut descriptor) = vfs.open(&Self::file_path(path), OpenOptions::builder().read(true).build()).await else {
            return Ok(None);
        };

        let mut content = Vec::new();
        descriptor.read_to_end(&mut content).await?;
        Ok(Some(content))
    }

    /// Écrit (ou remplace) le fichier `path` de la session.
    pub async fn write_file(&self, session_id: SessionId, path: &str, data: Vec<u8>) -> anyhow::Result<()> {
        let vfs = self.vfs(session_id).await?;
        let mut descriptor = vfs.open(&Self::file_path(path), OpenOptions::builder().create(true).write(true).build()).await?;
        descriptor.write_all(&data).await?;
        descriptor.shutdown().await?;
        Ok(())
    }

    /// Supprime le fichier `path` de la session — sans effet s'il n'existe
    /// pas.
    pub async fn delete_file(&self, session_id: SessionId, path: &str) -> anyhow::Result<()> {
        let vfs = self.vfs(session_id).await?;
        vfs.remove(&Self::file_path(path)).await
    }

    /// Chemins de tous les fichiers connus de la session.
    pub async fn list_files(&self, session_id: SessionId) -> anyhow::Result<Vec<String>> {
        let vfs = self.vfs(session_id).await?;
        vfs.ls("/session/files").await
    }

    /// Retire la session du stockage local de ce worker (par exemple une
    /// fois le job terminé) et diffuse [`SessionEvent::Removed`]. Ne fait
    /// rien si elle n'était pas détenue. Purement local : les autres
    /// détenteurs actifs, s'il y en a, conservent leur copie — les fichiers
    /// de la session, stockage partagé, ne sont pas concernés (voir
    /// `RpcCall::DELETE_SESSION` pour la suppression définitive).
    pub async fn remove(&self, session_id: SessionId) {
        if self.sessions.write().await.remove(&session_id).is_some() {
            self.emit(SessionEvent::Removed { session_id });
        }
    }

    /// Calcule le diff depuis le dernier envoi/réception et avance le
    /// curseur — à appeler juste après toute mutation locale, avant de
    /// relâcher le verrou d'écriture (voir [`SessionEntry::last_synced`]).
    fn diff_and_bump(&self, entry: &mut SessionEntry) -> Vec<u8> {
        let diff = entry.session.diff_since(&entry.last_synced);
        entry.last_synced = entry.session.state_vector();
        diff
    }

    /// Récupère l'état CRDT complet d'une session en interrogeant `holders`
    /// dans l'ordre jusqu'à ce que l'un réponde.
    async fn fetch_from_any(&self, session_id: SessionId, holders: &[PeerId]) -> anyhow::Result<YrsSession> {
        let mut last_error = None;

        for &holder in holders {
            match self.fetch_from(session_id, holder).await {
                Ok(session) => return Ok(session),
                Err(error) => {
                    debug!(%error, %session_id, %holder, "récupération de session échouée, essai du détenteur suivant");
                    last_error = Some(error);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("aucun détenteur connu pour la session {session_id}")))
    }

    /// Récupère l'état CRDT complet d'une session auprès d'un détenteur
    /// connu (voir [`RpcCall::FETCH_SESSION`]) — on part d'un vecteur d'état
    /// vide : ce worker n'a par construction jamais vu cette session (sinon
    /// [`Self::acquire`] n'aurait pas appelé cette méthode).
    async fn fetch_from(&self, session_id: SessionId, holder: PeerId) -> anyhow::Result<YrsSession> {
        let request = SessionFetchRequest { session_id, state_vector: StateVector::default().encode_v1() };
        let diff: Vec<u8> = self.network.rpc_to(RpcCall::new(RpcCall::FETCH_SESSION, request), holder).await?;
        YrsSession::from_diff(&diff)
    }

    /// Répond à une demande [`RpcCall::FETCH_SESSION`] d'un pair : le diff
    /// depuis son vecteur d'état, si nous détenons encore la session.
    pub async fn serve_fetch(&self, request: SessionFetchRequest) -> anyhow::Result<Vec<u8>> {
        let remote_sv = StateVector::decode_v1(&request.state_vector).map_err(|error| anyhow::anyhow!(error))?;

        let sessions = self.sessions.read().await;
        let Some(entry) = sessions.get(&request.session_id) else {
            bail!("session {} inconnue de ce worker", request.session_id);
        };

        Ok(entry.session.diff_since(&remote_sv))
    }
}

/// Tâche de fond démarrée par [`SessionClient::new`] : consomme
/// `network_events` et traite les messages gossipés sur
/// [`SESSION_EVENTS_TOPIC`] (réémis sur `events`) et [`SESSION_SYNC_TOPIC`]
/// (diffs fusionnés dans `sessions`, s'ils concernent une session détenue
/// localement — ignorés sinon, voir la note sur la fenêtre de course
/// ci-dessous). Jamais re-gossipé (évite les boucles, à la manière de
/// `cp::RpcRegistryGossip`). Tout événement réseau qui n'est pas un
/// `GossipMessageReceived` sur l'un de ces deux topics est ignoré
/// silencieusement.
///
/// Fenêtre de course connue et acceptée : un diff reçu pendant qu'
/// [`SessionClient::acquire`] est en cours pour la même session (entre le
/// début du fetch et l'insertion dans `sessions`) est perdu, faute
/// d'endroit où le mettre en attente. Sans conséquence en pratique : le
/// fetch en cours récupère de toute façon l'état le plus récent connu du
/// détenteur interrogé, et les diffs suivants du même émetteur continueront
/// d'arriver normalement une fois la session insérée.
///
/// Alimente aussi `known_persistency_peers` (voir
/// [`SessionClient::locate_session`]) à partir de
/// [`NetworkEvent::PersistencyPeerDiscovered`], indépendamment de tout
/// topic gossipsub : ce sont les pairs que ce nœud a lui-même identifiés via
/// libp2p `identify`, pas ceux relayés par le control plane.
async fn ingest_network_events(
    mut network_events: NetworkEventHandler,
    events: broadcast::Sender<SessionEvent>,
    sessions: Arc<RwLock<HashMap<SessionId, SessionEntry>>>,
    known_persistency_peers: Arc<RwLock<HashSet<PeerId>>>,
) {
    while let Some(event) = network_events.next().await {
        let NetworkEvent::GossipMessageReceived { topic, data, .. } = event else {
            if let NetworkEvent::PersistencyPeerDiscovered { peer_id, .. } = event {
                known_persistency_peers.write().await.insert(peer_id);
            }
            continue;
        };

        if topic == SESSION_EVENTS_TOPIC {
            if let Ok(event) = serde_json::from_slice::<SessionEvent>(&data) {
                let _ = events.send(event);
            }
            continue;
        }

        if topic == SESSION_SYNC_TOPIC {
            let Ok(message) = serde_json::from_slice::<SessionSyncMessage>(&data) else {
                continue;
            };

            let mut sessions = sessions.write().await;
            let Some(entry) = sessions.get_mut(&message.session_id) else {
                continue;
            };

            if let Err(error) = entry.session.apply_diff(&message.diff) {
                debug!(%error, session_id = %message.session_id, "diff de session reçu illisible, ignoré");
                continue;
            }
            entry.last_synced = entry.session.state_vector();
        }
    }
}
