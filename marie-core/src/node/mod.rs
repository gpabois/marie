use thiserror::Error;
use tokio::{sync::watch, task::JoinHandle};
use typed_builder::TypedBuilder;

use crate::secret::{KeyEpoch, SecretError, SecretKey, SecretManager};

/// Une ou plusieurs master keys pour construire le [`SecretManager`] d'un
/// nœud (voir [`MarieConfig::master_key`]) : [`Self::Single`] pour un
/// cluster qui n'est pas en cours de rotation, [`Self::Multi`] pendant une
/// rotation (voir le runbook de rotation sur [`SecretManager`] — plusieurs
/// epochs coexistent le temps que chaque nœud du cluster ait basculé et que
/// les données existantes aient été re-chiffrées). `marie-core` ne dicte
/// volontairement aucun format CLI/fichier/env pour peupler [`Self::Multi`] :
/// à l'intégrateur (binaire consommateur, ex. `marie-web-example`,
/// `marie-axum`) de choisir comment il source/parse ses epochs dans son
/// propre système de configuration.
pub enum MasterKeys {
    Single(SecretKey),
    Multi { keys: Vec<(KeyEpoch, SecretKey)>, current_epoch: KeyEpoch },
}

/// Permet à tout appelant existant de continuer à passer une `SecretKey`
/// brute à [`MarieConfig::builder`] sans changement (voir
/// `#[builder(setter(into))]` sur [`MarieConfig::master_key`]).
impl From<SecretKey> for MasterKeys {
    fn from(key: SecretKey) -> Self {
        Self::Single(key)
    }
}

impl MasterKeys {
    /// Construit le [`SecretManager`] correspondant (voir
    /// [`SecretManager::new`]/[`SecretManager::with_epochs`]).
    pub fn into_secret_manager(self) -> Result<SecretManager, SecretError> {
        match self {
            Self::Single(key) => Ok(SecretManager::new(&key)),
            Self::Multi { keys, current_epoch } => SecretManager::with_epochs(keys, current_epoch),
        }
    }
}

/// Configuration d'un [`Marie`] : le secret maître du cluster (voir
/// [`MasterKeys`]/[`SecretManager::new`]), à partager entre tous les nœuds
/// destinés à s'authentifier mutuellement. `master_key` doit être identique
/// sur tous les nœuds d'un même cluster (hors fenêtre de rotation, voir
/// [`MasterKeys::Multi`]) — c'est ce secret, jamais l'identité libp2p
/// (régénérée à chaque démarrage, voir `network::start_swarm`), qui les
/// authentifie mutuellement.
#[derive(TypedBuilder)]
pub struct MarieConfig {
    #[builder(setter(into))]
    master_key: MasterKeys,
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
    /// de graphes d'états (voir `session::state::catalog::store`).
    Structure {
    },
    /// `pool`/`store` : backends du VFS des sessions exécutées par ce worker
    /// (voir `session::client::SessionClient::vfs`/`read_file`/`write_file`,
    /// et `persistency::vfs::WorkspaceVfs`) — `pool` porte l'arborescence
    /// `/files` (catalogue d'inodes Postgres), `store` le contenu des
    /// fichiers (voir `persistency::FilesystemConfig` pour choisir son
    /// backend : mémoire, S3/compatible S3).
    ///
    /// `rust_registry` : fonctions Rust utilisables comme `Executable::Rust`
    /// par les nœuds/arêtes d'un `session::state::StateGraph` exécuté par
    /// ce worker (voir `session::state::executable::RustRegistry`) — à peupler par
    /// l'appelant, qui garde la main dessus après `start` (bon marché à
    /// cloner, mutation intérieure) pour y enregistrer de nouvelles
    /// fonctions à tout moment.
    Worker { },
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
        }
    }

}
