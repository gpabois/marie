use std::sync::Arc;

use marie_core::{
    Marie, MarieConfig, MarieHandle,
    expert::client::ExpertClient,
    hitl::client::HitlClient,
    mode::state_graph::client::StateGraphClient,
    model::ModelClient,
    network::swarm::SwarmNetwork,
    node::NotConnected,
    secret::SecretKey,
    session::client::SessionClient,
    tools::client::ToolClient,
    workspace::client::WorkspaceClient,
};
use object_store::ObjectStore;
use sqlx::postgres::PgPool;

/// Nœud réseau d'une passerelle Marie : rejoint le cluster (voir
/// [`Marie::join`]) sans endosser de rôle — un `Client` au sens de
/// `marie_core::network::peer::NodeKind`. Ne détient rien de plus que ce
/// qu'il faut pour brancher `marie-core` sur un routeur `axum` : les clients
/// applicatifs ([`Self::model_client`], [`Self::hitl_client`], ...) et
/// l'accès réseau bas niveau ([`Self::network`]).
///
/// Bon marché à cloner (comme `NetworkClient`/`SessionClient`) : prévu pour
/// être placé tel quel dans l'état `axum` de l'appelant.
#[derive(Clone)]
pub struct MarieGateway {
    marie: Arc<Marie>,
    network: SwarmNetwork,
}

impl MarieGateway {
    /// Rejoint le cluster identifié par `master_key` (voir
    /// `secret::SecretManager::new` — doit être le même secret que celui
    /// des autres nœuds du cluster) et retourne la passerelle ainsi que le
    /// [`MarieHandle`] permettant d'arrêter proprement le nœud sous-jacent
    /// (voir [`MarieHandle::shutdown`]).
    pub async fn connect(master_key: SecretKey) -> anyhow::Result<(Self, MarieHandle)> {
        let marie = Arc::new(Marie::new(MarieConfig::builder().master_key(master_key).build()));
        let (network, handle) = marie.join().await?;
        Ok((Self { marie, network }, handle))
    }

    /// Accès bas niveau au réseau (RPC, gossip, événements) — pour tout
    /// besoin non couvert par les clients applicatifs ci-dessous, par
    /// exemple soumettre un job (voir `NetworkClient::spawn_job`) ou
    /// appeler un RPC personnalisé enregistré par l'appelant (voir
    /// `NetworkClient::register_rpc`).
    pub fn network(&self) -> &SwarmNetwork {
        &self.network
    }

    /// Client du catalogue de modèles (voir [`ModelClient`]).
    pub fn model_client(&self) -> ModelClient {
        self.marie.model_client().expect("MarieGateway est toujours connecté après Self::connect")
    }

    /// Client du catalogue de tools (voir [`ToolClient`]).
    pub fn tool_client(&self) -> ToolClient {
        self.marie.tool_client().expect("MarieGateway est toujours connecté après Self::connect")
    }

    /// Client du catalogue d'experts (voir [`ExpertClient`]).
    pub fn expert_client(&self) -> ExpertClient {
        self.marie.expert_client().expect("MarieGateway est toujours connecté après Self::connect")
    }

    /// Client du catalogue de graphes d'états (voir [`StateGraphClient`]).
    pub fn state_graph_client(&self) -> StateGraphClient {
        self.marie.state_graph_client().expect("MarieGateway est toujours connecté après Self::connect")
    }

    /// Client CRDT des workspaces (voir [`WorkspaceClient`]) — création de
    /// session, store clé-valeur de workspace, contexte partagé.
    pub fn workspace_client(&self) -> WorkspaceClient {
        self.marie.workspace_client().expect("MarieGateway est toujours connecté après Self::connect")
    }

    /// Client du tool `system/ask-human` (voir [`HitlClient`]) — point
    /// d'entrée pour présenter les formulaires HITL du cluster à un
    /// opérateur humain et y répondre (voir [`crate::ws`]).
    pub fn hitl_client(&self) -> HitlClient {
        self.marie.hitl_client().expect("MarieGateway est toujours connecté après Self::connect")
    }

    /// Client CRDT des sessions (voir [`SessionClient`]) — `pool`/`store`
    /// alimentent le VFS `/session/files` (voir
    /// `persistency::vfs::WorkspaceVfs`), nécessaires même pour une
    /// passerelle qui ne fait qu'afficher des sessions plutôt qu'exécuter
    /// des jobs. Comme `Marie::session_client`, ne construit qu'une seule
    /// fois : un appel suivant retourne la même instance quels que soient
    /// `pool`/`store` passés.
    pub fn session_client(&self, pool: PgPool, store: Arc<dyn ObjectStore>) -> Result<SessionClient, NotConnected> {
        self.marie.session_client(pool, store)
    }
}
