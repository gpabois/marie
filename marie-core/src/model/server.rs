use std::sync::Arc;

use libp2p::PeerId;
use parking_lot::Mutex;

use crate::{model::{catalog::ModelCatalog, rpc::{GetModel, InsertModel, ListModel, RemoveModel, UpdateModel}}, rpc::{RemoteProcedureCall, RpcServer}, secret::SecretManager};

pub struct ModelServer;

impl ModelServer {
    pub fn new(local_peer_id: PeerId, mut rpc: RpcServer, secret: SecretManager) {
        let catalog: Arc<Mutex<ModelCatalog>> = Arc::new(Mutex::new(ModelCatalog::new()));

        GetModel(catalog.clone(), secret.clone()).register(&mut rpc);
        ListModel(catalog.clone(), secret.clone()).register(&mut rpc);
        InsertModel(catalog.clone(), secret.clone(), local_peer_id).register(&mut rpc);
        UpdateModel(catalog.clone(), secret, local_peer_id).register(&mut rpc);
        RemoveModel(catalog).register(&mut rpc);
    }
}
