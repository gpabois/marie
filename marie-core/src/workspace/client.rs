use libp2p::PeerId;
use serde_json::Value;
use thiserror::Error;

use crate::{
    di::{Factory, Get, Resolve}, network::{LocalPeerId, bootstrap::BootstrapClient}, rpc::{RpcClient, RpcError, Void}, session::SessionId, workspace::{
        NS_WORKSPACE, Workspace, WorkspaceId, WorkspaceSessionRequest, WorkspaceVarsPatchRequest, WorkspaceVarsQueryRequest, WorkspaceVarsRemoveRequest, rpc::{AddSession, GetWorkspace, InsertWorkspace, ListWorkspace, PatchVars, QueryVars, RemoveSession, RemoveVars, RemoveWorkspace},
    },
};

#[derive(Debug, Error)]
pub enum WorkspaceError {
    #[error("aucun serveur de workspaces n'est disponible")]
    NoServerAvailable,
    #[error("workspace inconnu : {0}")]
    UnknownWorkspace(WorkspaceId),
    #[error("[Workspace] échec de l'appel distant : {0}")]
    RpcError(#[from] RpcError),
    #[error("échec côté serveur de workspaces : {0}")]
    Server(String),
}

/// Point d'entrée pour le CRUD des workspaces, sur le même modèle que
/// [`crate::session::client::SessionClient`]/[`crate::model::client::ModelClient`] :
/// chaque opération sélectionne de manière déterministe le pair propriétaire
/// du workspace (voir [`Self::select_owner`]) plutôt que de s'appuyer sur
/// une réplication. Pas de `SecretManager` ici, contrairement à
/// `ModelClient` : le contenu d'un workspace ne transporte pas de secret
/// (pas de clé d'API), le chiffrement du transport libp2p (noise) suffit,
/// comme pour les sessions.
#[derive(Clone)]
pub struct WorkspaceClient {
    local_peer_id: LocalPeerId,
    rpc: RpcClient,
    bootstrap: BootstrapClient,
}

impl<C> Factory<C> for WorkspaceClient where C: Get<RpcClient> + Get<BootstrapClient> + Get<LocalPeerId> {
    fn create(container: &C) -> Self {
       Self {
            local_peer_id: container.get(),
            rpc: container.get(),
            bootstrap: container.get()
       }
    }
}

impl WorkspaceClient {

    /// Crée le workspace `id`, vide — raccourci de [`Self::insert`].
    pub async fn create(&self, id: impl Into<WorkspaceId>) -> Result<(), WorkspaceError> {
        self.insert(Workspace::new(id.into())).await
    }

    /// Crée un workspace dans le catalogue.
    pub async fn insert(&self, workspace: Workspace) -> Result<(), WorkspaceError> {
        let owner = self.select_owner(&workspace.id)?;

        self.rpc.invoke::<InsertWorkspace>(workspace, [owner]).await?;

        Ok(())
    }

    /// Récupère un workspace auprès du pair qui le sert.
    pub async fn get(&self, id: impl Into<WorkspaceId>) -> Result<Workspace, WorkspaceError> {
        let id = id.into();
        let owner = self.select_owner(&id)?;

        self.rpc
            .invoke::<GetWorkspace>(id, [owner])
            .await?
            .ok_or(WorkspaceError::UnknownWorkspace(id))
    }

    /// Liste tout le catalogue de workspaces connu du nœud sélectionné.
    pub async fn list(&self) -> Result<Vec<Workspace>, WorkspaceError> {
        let owner = self.select_owner(self.local_peer_id.to_bytes())?;

        self.rpc.invoke::<ListWorkspace>(Void, [owner]).await.map_err(WorkspaceError::from)
    }

    /// Retire un workspace du catalogue.
    pub async fn remove(&self, id: impl Into<WorkspaceId>) -> Result<(), WorkspaceError> {
        let id = id.into();
        let owner = self.select_owner(&id)?;

        self.rpc.invoke::<RemoveWorkspace>(id, [owner]).await?;

        Ok(())
    }

    /// Rattache `session_id` au workspace `workspace_id` (existant — voir
    /// [`crate::workspace::rpc::AddSession`]).
    pub async fn add_session(&self, workspace_id: WorkspaceId, session_id: SessionId) -> Result<(), WorkspaceError> {
        let owner = self.select_owner(&workspace_id)?;
        let request = WorkspaceSessionRequest { workspace_id, session_id };

        self.rpc.invoke::<AddSession>(request, [owner]).await?.map_err(WorkspaceError::Server)
    }

    /// Détache `session_id` du workspace `workspace_id`.
    pub async fn remove_session(&self, workspace_id: WorkspaceId, session_id: SessionId) -> Result<(), WorkspaceError> {
        let owner = self.select_owner(&workspace_id)?;
        let request = WorkspaceSessionRequest { workspace_id, session_id };

        self.rpc.invoke::<RemoveSession>(request, [owner]).await?.map_err(WorkspaceError::Server)
    }

    /// Génère un identifiant de session frais et le rattache immédiatement à
    /// `workspace_id` — une session ne naît jamais hors workspace. Ne crée
    /// pas la [`Session`](crate::session::Session) elle-même dans son
    /// catalogue (voir [`crate::session::client::SessionClient::insert`],
    /// servie par un autre pair) : c'est à l'appelant d'enchaîner les deux,
    /// l'identifiant renvoyé faisant le lien.
    pub async fn create_session(&self, workspace_id: WorkspaceId) -> Result<SessionId, WorkspaceError> {
        let session_id = SessionId::new(crate::id::generate_id());
        self.add_session(workspace_id, session_id).await?;
        Ok(session_id)
    }

    /// Sessions rattachées à `workspace_id` — simple lecture de
    /// [`Workspace::sessions`], le workspace entier restant petit
    /// (identifiants et variables, pas le contenu des sessions).
    pub async fn sessions(&self, workspace_id: WorkspaceId) -> Result<Vec<SessionId>, WorkspaceError> {
        Ok(self.get(workspace_id).await?.sessions)
    }

    /// Évalue `path` (JSONPath) contre [`Workspace::vars`] de `workspace_id`
    /// et renvoie les valeurs trouvées — voir
    /// [`crate::workspace::WorkspaceVarsQueryRequest`].
    pub async fn query_vars(&self, workspace_id: WorkspaceId, path: impl Into<String>) -> Result<Vec<Value>, WorkspaceError> {
        let owner = self.select_owner(&workspace_id)?;
        let request = WorkspaceVarsQueryRequest { workspace_id, path: path.into() };

        self.rpc.invoke::<QueryVars>(request, [owner]).await?.map_err(WorkspaceError::Server)
    }

    /// Remplace, dans [`Workspace::vars`] de `workspace_id`, chaque nœud
    /// correspondant à `path` (JSONPath) par `value` — voir
    /// [`crate::workspace::WorkspaceVarsPatchRequest`].
    pub async fn patch_vars(&self, workspace_id: WorkspaceId, path: impl Into<String>, value: Value) -> Result<(), WorkspaceError> {
        let owner = self.select_owner(&workspace_id)?;
        let request = WorkspaceVarsPatchRequest { workspace_id, path: path.into(), value };

        self.rpc.invoke::<PatchVars>(request, [owner]).await?.map_err(WorkspaceError::Server)
    }

    pub async fn remove_vars(&self, workspace_id: WorkspaceId, path: impl Into<String>) -> Result<(), WorkspaceError> {
        let owner = self.select_owner(&workspace_id)?;
        let request = WorkspaceVarsRemoveRequest {workspace_id, path: path.into() };
        self.rpc.invoke::<RemoveVars>(request, [owner]).await?.map_err(WorkspaceError::Server)
    }

    /// Sélection déterministe du pair propriétaire (hachage cohérent) —
    /// même mécanique que `SessionClient::select_catalog`.
    fn select_owner(&self, id: impl AsRef<[u8]>) -> Result<PeerId, WorkspaceError> {
        use WorkspaceError::NoServerAvailable;
        self.bootstrap.select_peer(NS_WORKSPACE, &id).ok_or(NoServerAvailable)
    }
}
