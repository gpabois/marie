use std::{borrow::Borrow, sync::Arc};

use async_trait::async_trait;
use libp2p::PeerId;
use parking_lot::Mutex;

use crate::{
    model::{EncryptedModel, Model, catalog::{EncryptedModelChangeSet, ModelCatalog, ModelChangeSet}, model::ModelId},
    rpc::{RemoteProcedureCall, Void},
    secret::{Encryptable as _, SecretManager},
};

/// Récupère la déclaration d'un modèle du catalogue, chiffrée pour
/// l'appelant (voir [`SecretManager::for_peer`]), ou `None` si inconnu de
/// ce nœud — voir [`crate::model::client::ModelClient::get`].
#[derive(Clone)]
pub struct GetModel(pub(crate) Arc<Mutex<ModelCatalog>>, pub(crate) SecretManager);

#[async_trait]
impl RemoteProcedureCall for GetModel {
    const NAME: &'static str = "/marie/models/get";

    type Args = ModelId;
    type Return = Option<EncryptedModel>;

    async fn execute(self, id: ModelId, caller: PeerId) -> Option<EncryptedModel> {
        let sec = self.1.for_peer(caller);
        self.0.lock().get(id.borrow()).map(|model| model.encrypt(&sec).unwrap())
    }
}

/// Liste tout le catalogue de modèles connu de ce nœud, chaque modèle
/// chiffré pour l'appelant.
#[derive(Clone)]
pub struct ListModel(pub(crate) Arc<Mutex<ModelCatalog>>, pub(crate) SecretManager);

#[async_trait]
impl RemoteProcedureCall for ListModel {
    const NAME: &'static str = "/marie/models/list";

    type Args = Void;
    type Return = Vec<EncryptedModel>;

    async fn execute(self, _: Void, caller: PeerId) -> Vec<EncryptedModel> {
        let sec = self.1.for_peer(caller);
        self.0.lock().list().into_iter().map(|model| model.encrypt(&sec).unwrap()).collect()
    }
}

/// Crée ou remplace un modèle dans le catalogue : `model` transite chiffré
/// pour ce nœud (voir `ModelClient::insert`), déchiffré ici avec sa propre
/// clé plutôt que celle de l'appelant.
#[derive(Clone)]
pub struct InsertModel(pub(crate) Arc<Mutex<ModelCatalog>>, pub(crate) SecretManager, pub(crate) PeerId);

#[async_trait]
impl RemoteProcedureCall for InsertModel {
    const NAME: &'static str = "/marie/models/insert";

    type Args = EncryptedModel;
    type Return = Void;

    async fn execute(self, model: EncryptedModel, _: PeerId) -> Void {
        let sec = self.1.for_peer(self.2);
        let model = Model::decrypt(model, &sec).unwrap();
        self.0.lock().insert(model);
        Void
    }
}

/// Met à jour un modèle existant : même déchiffrement que [`InsertModel`].
#[derive(Clone)]
pub struct UpdateModel(pub(crate) Arc<Mutex<ModelCatalog>>, pub(crate) SecretManager, pub(crate) PeerId);

#[async_trait]
impl RemoteProcedureCall for UpdateModel {
    const NAME: &'static str = "/marie/models/update";

    type Args = EncryptedModelChangeSet;
    type Return = Void;

    async fn execute(self, changeset: EncryptedModelChangeSet, _: PeerId) -> Void {
        let sec = self.1.for_peer(self.2);
        let changeset = ModelChangeSet::decrypt(changeset, &sec).unwrap();
        self.0.lock().update(changeset);
        Void
    }
}

/// Retire un modèle du catalogue.
#[derive(Clone)]
pub struct RemoveModel(pub(crate) Arc<Mutex<ModelCatalog>>);

#[async_trait]
impl RemoteProcedureCall for RemoveModel {
    const NAME: &'static str = "/marie/models/remove";

    type Args = ModelId;
    type Return = Void;

    async fn execute(self, id: ModelId, _: PeerId) -> Void {
        self.0.lock().remove(id.borrow());
        Void
    }
}
