use thiserror::Error;

use crate::{
    expert::{catalog::ExpertId, declaration::Expert}, rpc::{RpcClient, RpcError, Void, client::RpcCallArgs},
};

#[derive(Debug, Error)]
pub enum ExpertError {
    #[error("expert inconnu : {0}")]
    UnknownExpert(ExpertId),
    #[error("échec réseau : {0}")]
    Network(#[from] RpcError),
}

/// Point d'entrée pour le CRUD du catalogue d'experts.
#[derive(Clone)]
pub struct ExpertClient(RpcClient);

impl ExpertClient {
    #[must_use]
    pub fn new(client: RpcClient) -> Self {
        Self(client)
    }

    /// Récupère la déclaration d'un expert auprès du control plane.
    pub async fn get(&self, id: impl Into<ExpertId>) -> Result<Expert, ExpertError> {
        let id = id.into();

        let maybe_expert = RpcCallArgs::builder()
            .name("experts/rpc/get/1.0.0")
            .args(&id)
            .build()
            .call::<Option<Expert>>(&self.0)
            .await?;

        maybe_expert.ok_or_else(|| ExpertError::UnknownExpert(id.clone()))
        
    }

    /// Liste tout le catalogue d'experts connu du control plane.
    pub async fn list(&self) -> Result<Vec<Expert>, ExpertError> {
       let experts = RpcCallArgs::builder()
            .name("experts/rpc/list/1.0.0")
            .args(Void)
            .build()
            .call(&self.0)
            .await?;

        Ok(experts)
    }

    /// Crée ou remplace la déclaration d'un expert dans le catalogue
    /// (répliqué via Raft, voir `ControlPlaneRequest::SetExpert`).
    pub async fn upsert(&self, expert: Expert) -> Result<(), ExpertError> {
        RpcCallArgs::builder()
            .name("experts/rpc/upsert/1.0.0")
            .args(expert)
            .build()
            .call::<Void>(&self.0)
            .await?;

        Ok(())
    }

    /// Retire un expert du catalogue (répliqué via Raft, voir
    /// `ControlPlaneRequest::RemoveExpert`).
    pub async fn remove(&self, id: impl Into<ExpertId>) -> Result<(), ExpertError> {
        RpcCallArgs::builder()
            .name("experts/rpc/delete/1.0.0")
            .args(id.into())
            .build()
            .call::<Void>(&self.0)
            .await?;

        Ok(())
    }
}
