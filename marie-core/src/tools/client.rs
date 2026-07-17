use libp2p::PeerId;
use thiserror::Error;

use crate::{
    network::bootstrap::BootstrapClient, rpc::{RpcClient, RpcError, Void, client::RpcCallArgs}, tools::{NS_TOOL, RPC_TOOL_EXECUTE, RPC_TOOL_GET, RPC_TOOL_LIST, RPC_TOOL_REMOVE, Tool, ToolCall, ToolCallError, ToolId}
};

#[derive(Debug, Error)]
pub enum ToolError {
    #[error("aucun serveur d'outils disponible")]
    NoServerFound,
    #[error("tool inconnu : {0}")]
    UnknownTool(ToolId),
    #[error("échec réseau : {0}")]
    RpcError(#[from] RpcError),
    #[error("échec d'exécution du tool : {0:?}")]
    Call(ToolCallError),
    #[error("le message n'est pas un évènement outil")]
    NotToolEvent,
}

/// Point d'entrée pour le CRUD du catalogue de tools (répliqué via Raft, sur
/// le même modèle que [`crate::model::ModelClient`]) et pour la déclaration
/// et l'appel de l'exécuteur d'un tool. Ces deux aspects sont volontairement
/// découplés : [`Self::set`]/[`Self::remove`] modifient la déclaration
/// persistante d'un tool (visible de tout le cluster, survit à un
/// redémarrage), tandis que [`Self::register_executor`] ne fait que
/// signaler, tant que ce nœud reste connecté, qu'il est prêt à exécuter les
/// appels visant ce tool — voir `network::cp::DynamicRpcRegistry`.
#[derive(Clone)]
pub struct ToolClient {
    local_peer_id: PeerId,
    rpc: RpcClient,
    bootstrap: BootstrapClient
}

impl ToolClient {
    #[must_use]
    pub fn new(local_peer_id: PeerId, rpc: RpcClient,bootstrap: BootstrapClient) -> Self {
        Self {
            local_peer_id,
            rpc,
            bootstrap
        }
    }

    /// Récupère la déclaration d'un tool auprès du control plane.
    pub async fn get(&self, id: impl Into<ToolId>) -> Result<Tool, ToolError> {
        let id = id.into();

        let server = self.select_server(&id)?;

        RpcCallArgs::builder()
            .name(RPC_TOOL_GET)
            .args(&id)
            .destination(server)
            .build()
            .call::<Option<Tool>>(&self.rpc)
            .await?
            .ok_or(ToolError::UnknownTool(id))
    }

    /// Liste tout le catalogue de tools connu du control plane.
    pub async fn list(&self) -> Result<Vec<Tool>, ToolError> {
        let server = self.select_server(&self.local_peer_id.to_bytes())?;

        let list = RpcCallArgs::builder()
            .name(RPC_TOOL_LIST)
            .args(Void)
            .destination(server)
            .build()
            .call::<Vec<Tool>>(&self.rpc)
            .await?;

        Ok(list)
    }

    /// Retire un tool du catalogue (répliqué via Raft, voir
    /// `ControlPlaneRequest::RemoveTool`).
    pub async fn remove(&self, id: impl Into<ToolId>) -> Result<(), ToolError> {
        let id = id.into();

        let server = self.select_server(&id)?;

        RpcCallArgs::builder()
            .name(RPC_TOOL_REMOVE)
            .args(&id)
            .destination(server)
            .build()
            .call::<Void>(&self.rpc)
            .await?;

        Ok(())
    }

    pub async fn execute(&self, args: ToolCall) -> Result<(), ToolError> {
        let server = self.select_server(&args.id)?;

        RpcCallArgs::builder()
            .name(RPC_TOOL_EXECUTE)
            .args(args)
            .destination(server)
            .build()
            .call::<Result<(), String>>(&self.rpc)
            .await?;

        Ok(())
    }

    pub fn select_server(&self, id: impl AsRef<[u8]>) -> Result<PeerId, ToolError> {
        self.bootstrap.select_peer(NS_TOOL, id).ok_or(ToolError::NoServerFound)
    }

}
