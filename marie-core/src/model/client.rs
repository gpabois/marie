use libp2p::PeerId;

use crate::{model::{EncryptedModel, Model, ModelError::{self, SecretError}, ModelResponse, NS_MODEL, RPC_MODEL_GET, RPC_MODEL_INSERT, RPC_MODEL_LIST, RPC_MODEL_REMOVE, RPC_MODEL_RUN, RPC_MODEL_UPDATE, RunModelArgs, catalog::{ModelChangeSet, ModelId}}, network::bootstrap::BootstrapClient, rpc::{RpcClient, Void, client::RpcCallArgs}, secret::{Encryptable, SecretManager}};

#[derive(Clone)]
pub struct ModelClient {
    local_peer_id: PeerId,
    rpc: RpcClient,
    secret: SecretManager,
    bootstrap: BootstrapClient
}

impl ModelClient {
    #[must_use]
    pub fn new(local_peer_id: PeerId, rpc: RpcClient, bootstrap: BootstrapClient, secret: SecretManager) -> Self {
        Self {
            rpc,
            secret,
            bootstrap,
            local_peer_id
        }
    }

    pub async fn get(&self, id: impl Into<ModelId>) -> Result<super::model::Model, ModelError> {
        let id = id.into();
        
        let sec = self.secret.for_peer(self.local_peer_id);
        let catalog = self.select_catalog(&id)?;

        RpcCallArgs::builder()
            .name(RPC_MODEL_GET)
            .args(&id)
            .destination(catalog)
            .build()
            .call::<Option<EncryptedModel>>(&self.rpc)
            .await?
            .ok_or_else(|| ModelError::UnknownModel(id))
            .and_then(|encrypted| Model::decrypt(encrypted, &sec).map_err(SecretError))
    }

    /// Liste tout le catalogue de modèles connu du control plane.
    pub async fn list(&self) -> Result<Vec<Model>, ModelError> {
        let sec = self.secret.for_peer(self.local_peer_id);
        let catalog = self.select_catalog(self.local_peer_id.to_bytes())?;

        let list = RpcCallArgs::builder()
            .name(RPC_MODEL_LIST)
            .args(Void)
            .destination(catalog)
            .build()
            .call::<Vec<EncryptedModel>>(&self.rpc)
            .await?
            .into_iter()
            .map(|encrypted| Model::decrypt(encrypted, &sec))
            .collect::<Result<Vec<_>,_>>()?;

        Ok(list)
    }

    pub async fn insert(&self, model: Model) -> Result<(), ModelError> {
        let catalog = self.select_catalog(model.id())?;
        let sec = self.secret.for_peer(catalog);

        RpcCallArgs::builder()
            .name(RPC_MODEL_INSERT)
            .args(model.encrypt(&sec)?)
            .build()
            .call::<Void>(&self.rpc)
            .await?;

        Ok(())
    }

    /// Met à jour un modèle
    pub async fn update(&self, changeset: ModelChangeSet) -> Result<(), ModelError> {
        let catalog = self.select_catalog(&changeset.id)?;
        let sec = self.secret.for_peer(catalog);

        RpcCallArgs::builder()
            .name(RPC_MODEL_UPDATE)
            .args(changeset.encrypt(&sec)?)
            .build()
            .call::<Void>(&self.rpc)
            .await?;

        Ok(())
    }

    /// Retire un modèle du catalogue.
    pub async fn remove(&self, id: impl Into<ModelId>) -> Result<(), ModelError> {
        let id = id.into();

        let catalog = self.select_catalog(&id)?;

        RpcCallArgs::builder()
            .name(RPC_MODEL_REMOVE)
            .args(id)
            .destination(catalog)
            .build()
            .call::<Void>(&self.rpc)
            .await?;

        Ok(())
    }

    pub async fn run(&self, args: RunModelArgs) -> Result<ModelResponse, ModelError> {
        let catalog = self.select_catalog(&args.model_id)?;

        let response = RpcCallArgs::builder()
            .name(RPC_MODEL_RUN)
            .args(args)
            .destination(catalog)
            .build()
            .call::<Result<ModelResponse, String>>(&self.rpc)
            .await?
            .map_err(ModelError::Custom)?;

        Ok(response)
    }

    /// Sélection déterministe d'un catalogue.
    fn select_catalog(&self, id: impl AsRef<[u8]>) -> Result<PeerId, ModelError> {
        use ModelError::NoCatalogAvailable;
        self.bootstrap.select_peer(NS_MODEL, &id).ok_or(NoCatalogAvailable)
    }
}
