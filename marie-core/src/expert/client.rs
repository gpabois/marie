use libp2p::PeerId;
use thiserror::Error;

use crate::{
    expert::{Expert, GetExpert, InsertExpert, ListExpert, RemoveExpert, UpdateExpert, NS_EXPERT, catalog::ExpertId},
    network::bootstrap::BootstrapClient,
    rpc::{RpcClient, RpcError, Void},
};

#[derive(Debug, Error)]
pub enum ExpertError {
    #[error("aucun catalogue d'experts n'est disponible")]
    NoCatalogAvailable,
    #[error("expert inconnu : {0}")]
    UnknownExpert(ExpertId),
    #[error("[Expert] échec de l'appel distant : {0}")]
    RpcError(#[from] RpcError),
}

/// Point d'entrée pour le CRUD du catalogue d'experts, sur le même modèle que
/// [`crate::model::client::ModelClient`] : chaque opération sélectionne de
/// manière déterministe le pair qui héberge le catalogue (voir
/// [`Self::select_catalog`]) plutôt que de s'appuyer sur une réplication Raft.
#[derive(Clone)]
pub struct ExpertClient {
    local_peer_id: PeerId,
    rpc: RpcClient,
    bootstrap: BootstrapClient
}

impl ExpertClient {
    #[must_use]
    pub fn new(local_peer_id: PeerId, rpc: RpcClient, bootstrap: BootstrapClient) -> Self {
        Self {
            rpc,
            bootstrap,
            local_peer_id
        }
    }

    /// Récupère la déclaration d'un expert auprès du control plane.
    pub async fn get(&self, id: impl Into<ExpertId>) -> Result<Expert, ExpertError> {
        let id = id.into();
        let catalog = self.select_catalog(&id)?;

        self.rpc
            .invoke::<GetExpert>(id.clone(), [catalog])
            .await?
            .ok_or_else(|| ExpertError::UnknownExpert(id))
    }

    /// Liste tout le catalogue d'experts connu du control plane.
    pub async fn list(&self) -> Result<Vec<Expert>, ExpertError> {
        let catalog = self.select_catalog(self.local_peer_id.to_bytes())?;

        self.rpc.invoke::<ListExpert>(Void, [catalog]).await.map_err(ExpertError::from)
    }

    /// Crée un expert dans le catalogue.
    pub async fn insert(&self, expert: Expert) -> Result<(), ExpertError> {
        let catalog = self.select_catalog(&expert.id)?;

        self.rpc.invoke::<InsertExpert>(expert, [catalog]).await?;

        Ok(())
    }

    /// Met à jour la déclaration d'un expert existant.
    pub async fn update(&self, expert: Expert) -> Result<(), ExpertError> {
        let catalog = self.select_catalog(&expert.id)?;

        self.rpc.invoke::<UpdateExpert>(expert, [catalog]).await?;

        Ok(())
    }

    /// Retire un expert du catalogue.
    pub async fn remove(&self, id: impl Into<ExpertId>) -> Result<(), ExpertError> {
        let id = id.into();
        let catalog = self.select_catalog(&id)?;

        self.rpc.invoke::<RemoveExpert>(id, [catalog]).await?;

        Ok(())
    }

    /// Sélection déterministe d'un catalogue.
    fn select_catalog(&self, id: impl AsRef<[u8]>) -> Result<PeerId, ExpertError> {
        use ExpertError::NoCatalogAvailable;
        self.bootstrap.select_peer(NS_EXPERT, &id).ok_or(NoCatalogAvailable)
    }
}
