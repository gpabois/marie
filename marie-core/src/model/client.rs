use crate::{model::{EncryptedModel, Model, ModelError, catalog::ModelId}, rpc::{RpcClient, Void, client::RpcCallArgs}, secret::PeerSecretManager};

#[derive(Clone)]
pub struct ModelClient {
    rpc: RpcClient,
    secret: PeerSecretManager
}

impl ModelClient {
    #[must_use]
    pub fn new(rpc: RpcClient, secret: PeerSecretManager) -> Self {
        Self {
            rpc,
            secret
        }
    }

    /// Récupère la déclaration d'un modèle auprès du control plane. La clé
    /// API a voyagé chiffrée sur le réseau — voir
    /// [`NetworkClient::get_model`] et `SecretManager` — et n'est déchiffrée
    /// en clair qu'à la réception, localement.
    pub async fn get(&self, id: impl Into<ModelId>) -> Result<super::model::Model, ModelError> {
        let id = id.into();
        
        let maybe_model = RpcCallArgs::builder()
            .name("rpc/models/get")
            .args(id.clone())
            .build()
            .call::<Option<EncryptedModel>>(&self.rpc)
            .await?
            .map(|encrypted| self.decrypt(encrypted));

        maybe_model.ok_or_else(|| ModelError::UnknownModel(id))
    }

    /// Liste tout le catalogue de modèles connu du control plane.
    pub async fn list(&self) -> Result<Vec<Model>, ModelError> {
        let list = RpcCallArgs::builder()
            .name("rpc/models/list")
            .args(Void)
            .build()
            .call::<Vec<EncryptedModel>>(&self.rpc)
            .await?
            .into_iter()
            .map(|encrypted| self.decrypt(encrypted));

        Ok(list.collect())
    }

    /// Crée ou remplace la déclaration d'un modèle dans le catalogue.
    pub async fn upsert(&self, model: Model) -> Result<(), ModelError> {
        RpcCallArgs::builder()
            .name("rpc/models/upsert")
            .args(model)
            .build()
            .call::<Void>(&self.rpc)
            .await?;

        Ok(())
    }

    /// Retire un modèle du catalogue.
    pub async fn remove(&self, id: impl Into<ModelId>) -> Result<(), ModelError> {
        RpcCallArgs::builder()
            .name("rpc/models/delete")
            .args(id.into())
            .build()
            .call::<Void>(&self.rpc)
            .await?;

        Ok(())
    }

    fn decrypt(&self, model: EncryptedModel) -> Model {
        let api_key = model.api_key();
        let api_key = self.secret.decrypt(api_key).unwrap();
        model.decrypt(api_key)
    }

    fn encrypt(&self, model: Model) -> EncryptedModel {
        let api_key = model.api_key();
        let api_key = self.secret.encrypt(api_key).unwrap();
        model.encrypt(api_key)
    }
}
