use std::collections::HashMap;

use thiserror::Error;

use crate::{
    expert::{catalog::ExpertId, declaration::ExpertDeclaration},
    network::actor::NetworkService,
};

#[derive(Debug, Error)]
pub enum ExpertError {
    #[error("expert inconnu : {0}")]
    UnknownExpert(ExpertId),
    #[error("échec réseau : {0}")]
    Network(String),
}

/// Point d'entrée pour le CRUD du catalogue d'experts (répliqué via Raft, sur
/// le même modèle que [`crate::model::ModelClient`] et
/// [`crate::tools::client::ToolClient`]).
#[derive(Clone)]
pub struct ExpertClient(NetworkService);

impl ExpertClient {
    #[must_use]
    pub fn new(client: NetworkService) -> Self {
        Self(client)
    }

    /// Récupère la déclaration d'un expert auprès du control plane.
    pub async fn get(&self, id: impl Into<ExpertId>) -> Result<ExpertDeclaration, ExpertError> {
        let id = id.into();

        self.0
            .get_expert(id.clone())
            .await
            .map_err(|error| ExpertError::Network(error.to_string()))?
            .ok_or(ExpertError::UnknownExpert(id))
    }

    /// Liste tout le catalogue d'experts connu du control plane.
    pub async fn list(&self) -> Result<HashMap<ExpertId, ExpertDeclaration>, ExpertError> {
        self.0.list_experts().await.map_err(|error| ExpertError::Network(error.to_string()))
    }

    /// Crée ou remplace la déclaration d'un expert dans le catalogue
    /// (répliqué via Raft, voir `ControlPlaneRequest::SetExpert`).
    pub async fn set(&self, id: impl Into<ExpertId>, declaration: ExpertDeclaration) -> Result<(), ExpertError> {
        self.0.set_expert(id, declaration).await.map_err(|error| ExpertError::Network(error.to_string()))
    }

    /// Retire un expert du catalogue (répliqué via Raft, voir
    /// `ControlPlaneRequest::RemoveExpert`).
    pub async fn remove(&self, id: impl Into<ExpertId>) -> Result<(), ExpertError> {
        self.0.remove_expert(id).await.map_err(|error| ExpertError::Network(error.to_string()))
    }
}
