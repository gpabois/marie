use std::sync::Arc;

use libp2p::PeerId;
use parking_lot::Mutex;

use crate::{model::{EncryptedModel, Model, RPC_MODEL_GET, RPC_MODEL_INSERT, RPC_MODEL_RUN, RPC_MODEL_UPDATE, RunModelArgs, catalog::{EncryptedModelChangeSet, ModelCatalog, ModelChangeSet}, execute}, rpc::RpcServer, secret::{Encryptable as _, SecretManager}};

#[derive(Clone)]
pub struct ModelServer {
    catalog: Arc<Mutex<ModelCatalog>>,
    local_peer_id: PeerId
}

impl ModelServer {
    pub fn new(local_peer_id: PeerId, mut rpc: RpcServer, secret: SecretManager) {
        let catalog: Arc<Mutex<ModelCatalog>> = Arc::new(Mutex::new(ModelCatalog::new()));

        let cat = catalog.clone();
        let sec = secret.clone();
        rpc.register(RPC_MODEL_GET, move |id: String, source: PeerId| {
            let cat = cat.clone();
            let sec = sec.for_peer(source);
            cat.lock().get(&id).map(|model| model.encrypt(&sec).unwrap());
            std::future::ready(())
        });

        let cat = catalog.clone();
        let sec = secret.clone();
        rpc.register(RPC_MODEL_INSERT, move |model: EncryptedModel, _| {
            let sec = sec.for_peer(local_peer_id);
            let model = Model::decrypt(model, &sec).unwrap();
            cat.lock().insert(model);
            std::future::ready(())
        });

        let cat = catalog.clone();
        let sec = secret.clone();
        rpc.register(RPC_MODEL_UPDATE, move |changeset: EncryptedModelChangeSet, _| {
            let sec = sec.for_peer(local_peer_id);
            let changeset = ModelChangeSet::decrypt(changeset, &sec).unwrap();
            cat.lock().update(changeset);
            std::future::ready(())
        });

        let cat = catalog.clone();
        rpc.register(RPC_MODEL_RUN, move |args: RunModelArgs, _| {
            let cat = cat.clone();

            async move {
                let Some(model) = cat.lock().get(&args.model_id) else { return Err(format!("model indisponible {}", args.model_id)) };
                execute(args.session_id, model, &args.tools, args.context).await.map_err(|err| err.to_string())
            }
        });

        
    }
}