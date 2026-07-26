use libp2p::PeerId;

use crate::{
    Result, 
    di::{Factory, Get}, 
    err, 
    expert::{Expert, ExpertId, NS_EXPERT}, 
    network::{LocalPeerId, bootstrap::BootstrapClient}, 
    rpc::{RpcClient, Void},
};

use super::rpc::*;

/// Point d'entrée pour le CRUD du catalogue d'experts, sur le même modèle que
/// [`crate::model::client::ModelClient`] : chaque opération sélectionne de
/// manière déterministe le pair qui héberge le catalogue (voir
/// [`Self::select_catalog`]) plutôt que de s'appuyer sur une réplication Raft.
#[derive(Clone)]
pub struct ExpertClient {
    local_peer_id: LocalPeerId,
    rpc: RpcClient,
    bootstrap: BootstrapClient
}

impl<D> Factory<D> for ExpertClient
    where D: Get<RpcClient> + Get<LocalPeerId> + Get<BootstrapClient>
{
    fn create(container: &D) -> Self {
        Self {
            local_peer_id: container.get(),
            rpc: container.get(),
            bootstrap: container.get()
        }
    }
}

impl ExpertClient {
    /// Récupère la déclaration d'un expert auprès du control plane.
    pub async fn get(&self, id: impl Into<ExpertId>) -> Result<Option<Expert>> {
        let id = id.into();
        let catalog = self.select_catalog(&id)?;

        self.rpc
            .invoke::<GetExpert>(id.clone(), [catalog])
            .await?
    }

    /// Liste tout le catalogue d'experts connu du control plane.
    pub async fn list(&self) -> Result<Vec<Expert>> {
        let catalog = self.select_catalog(self.local_peer_id.to_bytes())?;

        self.rpc.invoke::<ListExpert>(Void, [catalog]).await?
    }

    /// Crée un expert dans le catalogue.
    pub async fn insert(&self, expert: Expert) -> Result<()> {
        let catalog = self.select_catalog(&expert.id)?;

        self.rpc.invoke::<InsertExpert>(expert, [catalog]).await?;

        Ok(())
    }

    /// Met à jour la déclaration d'un expert existant.
    pub async fn replace(&self, expert: Expert) -> Result<()> {
        let catalog = self.select_catalog(&expert.id)?;

        self.rpc.invoke::<ReplaceExpert>(expert, [catalog]).await?;

        Ok(())
    }

    /// Retire un expert du catalogue.
    pub async fn delete(&self, id: impl Into<ExpertId>) -> Result<()> {
        let id = id.into();
        let catalog = self.select_catalog(&id)?;

        self.rpc.invoke::<DeleteExpert>(id, [catalog]).await?;

        Ok(())
    }

    /// Sélection déterministe d'un catalogue.
    fn select_catalog(&self, id: impl AsRef<[u8]>) -> Result<PeerId> {
        self.bootstrap.select_peer(NS_EXPERT, &id).ok_or(err!("no experts node found"))
    }
}
