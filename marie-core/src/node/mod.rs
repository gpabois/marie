use std::future::Future;
use std::sync::{Arc, OnceLock};

use object_store::ObjectStore;
use sqlx::postgres::PgPool;
use thiserror::Error;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tracing::error;
use typed_builder::TypedBuilder;

use crate::layer::{IntoService as _, LayerExt};
use crate::network::mux::FrameLayer;
use crate::network::rpc::RpcMuxLayer;
use crate::rpc::router::{RpcRelayLayer, RpcRelayService};
use crate::rpc::{RpcClientActor, RpcClientService, RpcServerService};

use crate::{
    expert::{catalog::store::ExpertStore, client::ExpertClient},
    hitl::client::HitlClient,
    mode::{executable::RustRegistry, state_graph::{catalog::store::StateGraphStore, client::StateGraphClient}},
    model::{ModelClient, catalog::store::ModelStore},
    network::{
        actor::{NetworkActor, NetworkService},
        persistency as persistency_role,
        peer::NodeKind,
        start_swarm, worker,
    },
    secret::{SecretKey, SecretManager},
    session::client::SessionClient,
    tools::{catalog::store::ToolStore, client::ToolClient},
    workspace::client::WorkspaceClient,
};

/// Configuration d'un [`Marie`] : le secret maître du cluster (voir
/// [`SecretManager::new`]), à partager entre tous les nœuds destinés à
/// s'authentifier mutuellement. `master_key` doit être identique sur tous
/// les nœuds d'un même cluster — c'est ce secret, jamais l'identité libp2p
/// (régénérée à chaque démarrage, voir `network::start_swarm`), qui les
/// authentifie mutuellement.
#[derive(TypedBuilder)]
pub struct MarieConfig {
    master_key: SecretKey,
}

/// Rôle sous lequel un nœud rejoint le cluster (voir [`NodeKind`]) : chaque
/// variante correspond à une boucle de rôle existante (`network::cp`,
/// `network::worker`, `network::persistency`), démarrée par [`Marie::start`].
///
/// Un nœud tiers qui n'a besoin que de se brancher sur le réseau (sans
/// endosser de rôle de cluster) utilise [`Marie::join`] plutôt qu'une
/// variante de cette énumération.
pub enum NodeRole {
    /// `raft_log_backend` : stockage durable du log Raft (voir
    /// `network::cp::log_store::RaftLogBackend`) — technologie au choix de
    /// l'appelant (`network::cp::log_store::redb_backend::RedbLogBackend`
    /// par défaut, ou une implémentation maison, ex. Postgres). Sans lui, un
    /// redémarrage complet du cluster perd tout `ControlPlaneState` (jobs,
    /// registre des workers, etc.), pas seulement la panne d'un nœud isolé
    /// (déjà tolérée par la réplication Raft elle-même).
    ///
    /// `model_store` : stockage chiffré local du catalogue de modèles (voir
    /// `model::catalog::store` et `network::cp::start_control_plane`) —
    /// permet à ce nœud de récupérer son catalogue à froid sans dépendre du
    /// reste du cluster.
    ///
    /// `tool_store` : équivalent de `model_store` pour le catalogue de tools
    /// (voir `tools::catalog::store`).
    ///
    /// `expert_store` : équivalent de `model_store` pour le catalogue
    /// d'experts (voir `expert::catalog::store`).
    ///
    /// `state_graph_store` : équivalent de `model_store` pour le catalogue
    /// de graphes d'états (voir `mode::state_graph::catalog::store`).
    Structure {
        model_store: Arc<dyn ModelStore>,
        tool_store: Arc<dyn ToolStore>,
        expert_store: Arc<dyn ExpertStore>,
        state_graph_store: Arc<dyn StateGraphStore>,
    },
    /// `pool`/`store` : backends du VFS des sessions exécutées par ce worker
    /// (voir `session::client::SessionClient::vfs`/`read_file`/`write_file`,
    /// et `persistency::vfs::WorkspaceVfs`) — `pool` porte l'arborescence
    /// `/files` (catalogue d'inodes Postgres), `store` le contenu des
    /// fichiers (voir `persistency::FilesystemConfig` pour choisir son
    /// backend : mémoire, S3/compatible S3).
    ///
    /// `rust_registry` : fonctions Rust utilisables comme `Executable::Rust`
    /// par les nœuds/arêtes d'un `mode::state_graph::StateGraph` exécuté par
    /// ce worker (voir `mode::executable::RustRegistry`) — à peupler par
    /// l'appelant, qui garde la main dessus après `start` (bon marché à
    /// cloner, mutation intérieure) pour y enregistrer de nouvelles
    /// fonctions à tout moment.
    Worker { pool: PgPool, store: Arc<dyn ObjectStore>, rust_registry: RustRegistry },
}

/// Poignée de supervision d'un nœud démarré par [`Marie`]. L'abandonner
/// n'arrête pas le nœud sous-jacent (voir [`tokio::task::JoinHandle`]) —
/// utiliser [`Self::shutdown`] pour un arrêt propre (recommandé),
/// [`Self::abort`] pour un arrêt immédiat sans garantie, ou [`Self::wait`]
/// pour bloquer jusqu'à l'arrêt du nœud (erreur de démarrage, panique, ou
/// arrêt demandé par ailleurs).
pub struct MarieHandle {
    task: JoinHandle<()>,
    /// Signale la demande d'arrêt à la boucle de rôle (voir
    /// [`Self::shutdown`]) — `false` tant qu'aucun arrêt n'a été demandé,
    /// `true` une fois [`Self::shutdown`] appelé. Un `watch` plutôt qu'un
    /// `oneshot` : la boucle de rôle le consulte à chaque tour de
    /// `tokio::select!` sans le consommer (voir
    /// `network::cp::start_control_plane`/`network::worker::start_worker`/
    /// `network::persistency::start_persistency`).
    shutdown: watch::Sender<bool>,
}

impl MarieHandle {
    /// Arrêt immédiat, sans garantie : la tâche est annulée à son prochain
    /// point d'attente (voir [`tokio::task::JoinHandle::abort`]), sans
    /// laisser au nœud la moindre chance de terminer un travail en vol
    /// (job en cours d'exécution, diff pas encore publié) ni de fermer
    /// proprement ses connexions réseau. Préférer [`Self::shutdown`], sauf
    /// si le nœud ne répond déjà plus.
    pub fn abort(&self) {
        self.task.abort();
    }

    pub async fn wait(self) {
        let _ = self.task.await;
    }

    /// Arrêt propre du nœud : signale la demande d'arrêt (voir
    /// [`Self::shutdown`] sur le champ `shutdown`) puis attend que la
    /// boucle de rôle ait fini de se terminer — pour un worker, cela
    /// inclut de laisser les jobs déjà en vol se conclure (ou d'atteindre
    /// leur propre délai de grâce, voir
    /// `network::worker::mod::SHUTDOWN_GRACE_PERIOD`) et de rapporter leur
    /// issue avant de couper la connexion réseau sous-jacente (voir
    /// `network::actor::NetworkClient::shutdown`, appelé en tout dernier
    /// par la boucle de rôle). Peut donc prendre jusqu'à ce délai de grâce
    /// avant de rendre la main.
    pub async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        let _ = self.task.await;
    }
}

/// Point d'entrée unique pour configurer et démarrer un nœud du cluster Marie
/// (voir [`Self::start`]), ou pour simplement se brancher sur le réseau
/// depuis un nœud tiers développé par l'utilisateur (voir [`Self::join`]) —
/// par exemple une passerelle HTTP/WebSocket exposant du HITL (voir
/// [`Self::hitl_client`]), ou affichant les logs/statuts d'une session (voir
/// [`Self::session_client`]).
///
/// Tous les nœuds d'un même cluster doivent partager le même secret maître
/// (voir [`MarieConfig`]) : c'est lui, et non l'identité libp2p (régénérée à
/// chaque démarrage, voir `network::start_swarm`), qui permet
/// l'authentification mutuelle des control planes et le chiffrement des
/// secrets applicatifs transmis sur le réseau (voir
/// `NetworkClient::decrypt_secret`).
pub struct Marie {
    secret: Arc<SecretManager>,
    /// [`NetworkClient`] de ce nœud, rempli dès la connexion établie par
    /// [`Self::start`] ou [`Self::join`] — voir [`Self::model_client`]/
    /// [`Self::tool_client`]. `Arc` pour rester accessible depuis la tâche de
    /// fond qui le peuple (voir [`Self::start`]), indépendamment de la durée
    /// de vie d'un emprunt de `&self`.
    network: Arc<OnceLock<NetworkService>>,

    /// Servicé dédié à faire des appels à procédure distants (RPC)
    rpc_client: Arc<OnceLock<RpcClientService>>,
    /// Service dédié à servir des RPC
    rpc_server: Arc<OnceLock<RpcServerService>>,
    
    /// [`HitlClient`] de ce nœud, construit paresseusement au premier appel
    /// à [`Self::hitl_client`] — contrairement à [`ModelClient`]/
    /// [`ToolClient`]/[`ExpertClient`] (de simples enveloppes sans état
    /// local), un [`HitlClient`] démarre sa propre tâche de fond et détient
    /// les questions en attente de réponse (voir `hitl::client::HitlClient::new`) :
    /// il doit donc être construit une seule fois puis réutilisé, jamais
    /// recréé à chaque accès.
    hitl: Arc<OnceLock<HitlClient>>,
    /// [`SessionClient`] de ce nœud, construit paresseusement au premier
    /// appel à [`Self::session_client`] — sur le même modèle que
    /// [`Self::hitl`] : un [`SessionClient`] démarre lui aussi sa propre
    /// tâche de fond (voir `session::client::SessionClient::new`) et détient
    /// les sessions acquises localement, donc une seule instance doit être
    /// partagée plutôt que reconstruite à chaque accès.
    sessions: Arc<OnceLock<SessionClient>>,
    /// [`WorkspaceClient`] de ce nœud, construit paresseusement au premier
    /// appel à [`Self::workspace_client`] — même motif que [`Self::sessions`].
    workspaces: Arc<OnceLock<WorkspaceClient>>,
}

/// Retourné par [`Marie::model_client`]/[`Marie::tool_client`] tant que ce
/// nœud n'est pas encore connecté au réseau (voir [`Marie::start`]/
/// [`Marie::join`]) — la connexion est asynchrone, un appel juste après
/// [`Marie::start`] peut donc légitimement la précéder de peu.
#[derive(Debug, Error)]
#[error("nœud pas encore connecté au réseau (voir Marie::start / Marie::join)")]
pub struct NotConnected;

impl Marie {
    #[must_use]
    pub fn new(config: MarieConfig) -> Self {
        Self {
            secret: Arc::new(SecretManager::new(&config.master_key)),
            rpc_client: Arc::new(OnceLock::new()),
            rpc_server: Arc::new(OnceLock::new()),
            network: Arc::new(OnceLock::new()),
            hitl: Arc::new(OnceLock::new()),
            sessions: Arc::new(OnceLock::new()),
            workspaces: Arc::new(OnceLock::new()),
        }
    }

    /// Démarre un nœud endossant `role` en tâche de fond. La boucle de rôle
    /// tourne jusqu'à un arrêt demandé via [`MarieHandle::shutdown`]/
    /// [`MarieHandle::abort`], ou jusqu'à une erreur de démarrage (ex. port
    /// déjà occupé) — loggée puis mettant fin à la tâche, observable via
    /// [`MarieHandle::wait`].
    pub fn start(&self, role: NodeRole) -> MarieHandle {
        let secret = self.secret.clone();
        let (ready_tx, ready_rx) = oneshot::channel();
        let network = self.network.clone();
        tokio::spawn(async move {
            if let Ok(client) = ready_rx.await {
                let _ = network.set(client);
            }
        });

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let task = match role {
            NodeRole::ControlPlane { raft_log_backend, model_store, tool_store, expert_store, state_graph_store } => Self::spawn_role(
                "control-plane",
                cp::start_control_plane(
                    secret,
                    raft_log_backend,
                    model_store,
                    tool_store,
                    expert_store,
                    state_graph_store,
                    shutdown_rx,
                    ready_tx,
                ),
            ),
            NodeRole::Worker { pool, store, rust_registry } => Self::spawn_role(
                "worker",
                worker::start_worker(secret, pool, store, rust_registry, shutdown_rx, ready_tx),
            ),
            NodeRole::Persistency { store, workspace_store, pool, object_store } => Self::spawn_role(
                "persistency",
                persistency_role::start_persistency(secret, store, workspace_store, pool, object_store, shutdown_rx, ready_tx),
            ),
        };

        MarieHandle { task, shutdown: shutdown_tx }
    }

    /// Rejoint le réseau sans endosser de rôle de cluster (voir
    /// [`NodeKind::Client`]) : le point d'entrée pour un nœud développé par
    /// l'utilisateur qui a seulement besoin d'un [`NetworkClient`] pour
    /// émettre des RPC et observer les
    /// [`NetworkEvent`](crate::network::actor::NetworkEvent) du cluster (voir
    /// `NetworkClient::subscribe_events`), sans exécuter la logique d'un
    /// control plane, d'un worker ou d'un nœud de persistance.
    pub async fn join(&self) -> Result<(NetworkService, MarieHandle), anyhow::Error> {
        let swarm = start_swarm(NodeKind::Client, |_| {}).await?;
        let (actor, client) = NetworkActor::new(swarm, self.secret.clone());
    
        self.rpc_client.set(
            client.transport()
                .chain::<FrameLayer, _>(())
                .chain::<RpcMuxLayer, _>(())
                .into_service(())
        );

        self.rpc_server.set(
            client.transport()
                .chain::<FrameLayer, _>(())
                .chain::<RpcMuxLayer, _>(())
                .chain::<RpcRelayLayer, _>((
                    self.rpc_relay.clone(), 
                    client.transport()
                        .chain::<FrameLayer, _>(())
                        .chain::<RpcMuxLayer, _>(())
                ))
                .into_service(())
        );

        let _ = self.network.set(client.clone());

        

        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let shutdown_client = client.clone();
        let task = tokio::spawn(async move {
            let actor_task = tokio::spawn(actor.run());
            // Pas de boucle applicative ici (contrairement à un rôle de
            // cluster) : rien à drainer avant de couper le réseau, juste à
            // attendre la demande d'arrêt explicite puis relayer à l'actor
            // (voir `NetworkClient::shutdown`). Si `shutdown_tx` est
            // abandonné sans arrêt explicite (voir `MarieHandle`, qui
            // documente qu'abandonner la poignée n'arrête *pas* le nœud),
            // `changed()` échoue immédiatement : on attend alors simplement
            // la fin (normalement jamais) de l'actor lui-même, qui continue
            // de tourner en arrière-plan.
            if shutdown_rx.changed().await.is_ok() {
                shutdown_client.shutdown();
            }
            let _ = actor_task.await;
        });

        Ok((client, MarieHandle { task, shutdown: shutdown_tx }))
    }

    /// Client pour le catalogue de modèles (voir [`ModelClient`]), une fois
    /// ce nœud connecté au réseau (voir [`Self::start`]/[`Self::join`]) —
    /// évite à l'appelant de conserver lui-même le [`NetworkClient`] obtenu à
    /// la connexion.
    pub fn model_client(&self) -> Result<ModelClient, NotConnected> {
        self.network.get().cloned().map(ModelClient::new).ok_or(NotConnected)
    }

    /// Client pour le catalogue de tools (voir [`ToolClient`]), sur le même
    /// modèle que [`Self::model_client`].
    pub fn tool_client(&self) -> Result<ToolClient, NotConnected> {
        self.network.get().cloned().map(ToolClient::new).ok_or(NotConnected)
    }

    /// Client pour le catalogue d'experts (voir [`ExpertClient`]), sur le
    /// même modèle que [`Self::model_client`].
    pub fn expert_client(&self) -> Result<ExpertClient, NotConnected> {
        self.network.get().cloned().map(ExpertClient::new).ok_or(NotConnected)
    }

    /// Client pour le catalogue de graphes d'états (voir [`StateGraphClient`]),
    /// sur le même modèle que [`Self::model_client`].
    pub fn state_graph_client(&self) -> Result<StateGraphClient, NotConnected> {
        self.network.get().cloned().map(StateGraphClient::new).ok_or(NotConnected)
    }

    /// Client pour le tool `system/ask-human` (voir [`crate::hitl`] et
    /// [`HitlClient`]), une fois ce nœud connecté au réseau. Contrairement à
    /// [`Self::model_client`]/[`Self::tool_client`]/[`Self::expert_client`],
    /// la même instance est retournée à chaque appel (voir le champ
    /// [`Self::hitl`]) plutôt qu'une nouvelle enveloppe à chaque fois — bon
    /// marché à cloner, la valeur retournée peut être conservée par
    /// l'appelant sans repasser par ici.
    pub fn hitl_client(&self) -> Result<HitlClient, NotConnected> {
        let network = self.network.get().cloned().ok_or(NotConnected)?;
        Ok(self.hitl.get_or_init(|| HitlClient::new(network)).clone())
    }

    /// Client pour l'état CRDT des sessions (voir [`SessionClient`]), une
    /// fois ce nœud connecté au réseau — typiquement depuis un nœud tiers
    /// (voir [`Self::join`]) affichant les logs/statuts d'une session, ex.
    /// une passerelle HTTP/WebSocket ou un tableau de bord. Même motif que
    /// [`Self::hitl_client`] : la même instance est retournée à chaque appel
    /// (voir le champ [`Self::sessions`]), `pool`/`store` ne sont donc pris
    /// en compte qu'à la première construction — passer des valeurs
    /// différentes à un appel suivant n'a aucun effet.
    pub fn session_client(&self, pool: PgPool, store: Arc<dyn ObjectStore>) -> Result<SessionClient, NotConnected> {
        let network = self.network.get().cloned().ok_or(NotConnected)?;
        let workspace = self.workspace_client()?;
        let workspace_vfs = WorkspaceVfs::new(workspace, pool, store);
        Ok(self.sessions.get_or_init(|| SessionClient::new(network, workspace_vfs)).clone())
    }

    /// Client pour l'état CRDT des workspaces (voir [`WorkspaceClient`]),
    /// une fois ce nœud connecté au réseau — même motif que
    /// [`Self::session_client`] : la même instance est retournée à chaque
    /// appel (voir le champ [`Self::workspaces`]).
    pub fn workspace_client(&self) -> Result<WorkspaceClient, NotConnected> {
        let network = self.network.get().cloned().ok_or(NotConnected)?;
        Ok(self.workspaces.get_or_init(|| WorkspaceClient::new(network)).clone())
    }

    fn spawn_role(
        name: &'static str,
        role: impl Future<Output = Result<(), anyhow::Error>> + Send + 'static,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            if let Err(error) = role.await {
                error!(%error, node = name, "nœud arrêté suite à une erreur");
            }
        })
    }
}
