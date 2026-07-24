use libp2p::PeerId;

use crate::{di::{Factory, Get}, model::{Model, ModelError::{self, SecretError}, NS_MODEL, catalog::{ModelChangeSet, ModelId}, rpc::{GetModel, InsertModel, ListModel, RemoveModel, UpdateModel}}, network::{LocalPeerId, bootstrap::BootstrapClient}, rpc::{RpcClient, Void}, secret::{Encryptable, SecretManager}};

#[derive(Clone)]
pub struct ModelClient {
    local_peer_id: LocalPeerId,
    rpc: RpcClient,
    secret: SecretManager,
    bootstrap: BootstrapClient
}

impl<C> Factory<C> for ModelClient 
    where C: Get<RpcClient> + Get<SecretManager> + Get<BootstrapClient> + Get<LocalPeerId>
{
    fn create(container: &C) -> Self {
        Self {
            local_peer_id: container.get(),
            rpc: container.get(),
            secret: container.get(),
            bootstrap: container.get()
        }
    }
}

impl ModelClient {

    pub async fn get(&self, id: impl Into<ModelId>) -> Result<super::model::Model, ModelError> {
        let id = id.into();
        
        let sec = self.secret.for_peer(self.local_peer_id)?;
        let catalog = self.select_catalog(&id)?;

        self.rpc
            .invoke::<GetModel>(id.clone(), [catalog])
            .await?
            .ok_or_else(|| ModelError::UnknownModel(id))
            .and_then(|encrypted| Model::decrypt(encrypted, &sec).map_err(SecretError))
    }

    /// Liste tout le catalogue de modèles connu du control plane.
    pub async fn list(&self) -> Result<Vec<Model>, ModelError> {
        let sec = self.secret.for_peer(self.local_peer_id)?;
        let catalog = self.select_catalog(self.local_peer_id.to_bytes())?;

        let list = self.rpc
            .invoke::<ListModel>(Void, [catalog])
            .await?
            .into_iter()
            .map(|encrypted| Model::decrypt(encrypted, &sec))
            .collect::<Result<Vec<_>,_>>()?;

        Ok(list)
    }

    pub async fn insert(&self, model: Model) -> Result<(), ModelError> {
        let catalog = self.select_catalog(model.id())?;
        let sec = self.secret.for_peer(catalog)?;

        self.rpc.invoke::<InsertModel>(model.encrypt(&sec)?, [catalog]).await?;

        Ok(())
    }

    /// Met à jour un modèle
    pub async fn update(&self, changeset: ModelChangeSet) -> Result<(), ModelError> {
        let catalog = self.select_catalog(&changeset.id)?;
        let sec = self.secret.for_peer(catalog)?;

        self.rpc.invoke::<UpdateModel>(changeset.encrypt(&sec)?, [catalog]).await?;

        Ok(())
    }

    /// Retire un modèle du catalogue.
    pub async fn remove(&self, id: impl Into<ModelId>) -> Result<(), ModelError> {
        let id = id.into();

        let catalog = self.select_catalog(&id)?;

        self.rpc.invoke::<RemoveModel>(id, [catalog]).await?;

        Ok(())
    }

    /// Sélection déterministe d'un catalogue.
    fn select_catalog(&self, id: impl AsRef<[u8]>) -> Result<PeerId, ModelError> {
        use ModelError::NoCatalogAvailable;
        self.bootstrap.select_peer(NS_MODEL, &id).ok_or(NoCatalogAvailable)
    }
}
