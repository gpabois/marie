use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use serde_json::Value;
use thiserror::Error;

use crate::{
    agent::GlobalAgentId,
    network::{actor::NetworkService, cp::rpc::RpcCall},
    tools::{
        ToolCall, ToolCallError, ToolCallRequest, ToolCallResponse,
        catalog::ToolId,
        declaration::ToolDeclaration,
    },
};

/// Nom de la RPC dynamique (voir `NetworkClient::register_rpc`) associée à
/// l'exécution du tool `id` — distinct du nom du tool lui-même pour éviter
/// toute collision avec les RPC natives du control plane (`RpcCall::GET_TOOL`
/// etc.) ou d'autres enregistrements dynamiques sans rapport.
fn executor_rpc_name(id: &ToolId) -> String {
    format!("tool:{id}")
}

#[derive(Debug, Error)]
pub enum ToolError {
    #[error("tool inconnu : {0}")]
    UnknownTool(ToolId),
    #[error("échec réseau : {0}")]
    Network(String),
    #[error("échec d'exécution du tool : {0:?}")]
    Call(ToolCallError),
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
pub struct ToolClient(NetworkService);

impl ToolClient {
    #[must_use]
    pub fn new(client: NetworkService) -> Self {
        Self(client)
    }

    /// Récupère la déclaration d'un tool auprès du control plane.
    pub async fn get(&self, id: impl Into<ToolId>) -> Result<ToolDeclaration, ToolError> {
        let id = id.into();

        self.0
            .get_tool(id.clone())
            .await
            .map_err(|error| ToolError::Network(error.to_string()))?
            .ok_or(ToolError::UnknownTool(id))
    }

    /// Liste tout le catalogue de tools connu du control plane.
    pub async fn list(&self) -> Result<HashMap<ToolId, ToolDeclaration>, ToolError> {
        self.0.list_tools().await.map_err(|error| ToolError::Network(error.to_string()))
    }

    /// Crée ou remplace la déclaration d'un tool dans le catalogue (répliqué
    /// via Raft, voir `ControlPlaneRequest::SetTool`).
    pub async fn set(&self, id: impl Into<ToolId>, declaration: ToolDeclaration) -> Result<(), ToolError> {
        self.0.set_tool(id, declaration).await.map_err(|error| ToolError::Network(error.to_string()))
    }

    /// Retire un tool du catalogue (répliqué via Raft, voir
    /// `ControlPlaneRequest::RemoveTool`).
    pub async fn remove(&self, id: impl Into<ToolId>) -> Result<(), ToolError> {
        self.0.remove_tool(id).await.map_err(|error| ToolError::Network(error.to_string()))
    }

    /// Déclare ce nœud comme exécuteur du RPC sous-jacent à `id` : tout appel
    /// [`Self::call`] visant ce tool, émis par n'importe quel nœud du
    /// cluster, sera relayé jusqu'à `handler` (voir
    /// `NetworkClient::register_rpc` et le relais dynamique dans
    /// `network::cp::execute_rpc`). Purement déclaratif et non répliqué via
    /// Raft, contrairement à [`Self::set`] : une capacité d'exécution est
    /// transitoire (retirée automatiquement quand ce nœud se déconnecte, voir
    /// `network::cp::DynamicRpcRegistry`), pas une propriété persistante du
    /// tool. Plusieurs nœuds peuvent s'enregistrer pour le même `id` — le
    /// premier à répondre l'emporte (voir `network::cp::forward_race`).
    pub async fn register_executor<F, Fut>(&self, id: impl Into<ToolId>, handler: F) -> Result<(), ToolError>
    where
        F: Fn(ToolCallRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ToolCallResponse, ToolCallError>> + Send + 'static,
    {
        let name = executor_rpc_name(&id.into());
        let handler = Arc::new(handler);

        self.0
            .register_rpc(name, move |args| {
                let handler = handler.clone();
                async move {
                    let request: ToolCallRequest = serde_json::from_value(args)?;
                    let response = handler(request).await.unwrap_or_else(ToolCallResponse::Failed);
                    Ok(serde_json::to_value(response)?)
                }
            })
            .await
            .map_err(|error| ToolError::Network(error.to_string()))
    }

    /// Exécute `call` en le relayant vers un nœud actuellement déclaré
    /// exécuteur de ce tool (voir [`Self::register_executor`]). Échoue si
    /// aucun nœud ne s'est déclaré exécuteur (voir
    /// `network::cp::DynamicRpcRegistry`).
    pub async fn call(&self, agent_id: GlobalAgentId, call: ToolCall) -> Result<Option<Value>, ToolError> {
        let name = executor_rpc_name(&ToolId::from(call.name.as_str()));
        let request = ToolCallRequest { agent_id, call };

        let response: ToolCallResponse =
            self.0.rpc(RpcCall::new(name, request)).await.map_err(|error| ToolError::Network(error.to_string()))?;

        match response {
            ToolCallResponse::Success { output } => Ok(output),
            ToolCallResponse::Failed(error) => Err(ToolError::Call(error)),
        }
    }
}
