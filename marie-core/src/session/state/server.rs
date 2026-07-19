use std::sync::Arc;

use parking_lot::Mutex;

use crate::{
    rpc::{RemoteProcedureCall, RpcServer},
    session::state::{
        catalog::StateGraphCatalog,
        rpc::{GetStateGraph, InsertStateGraph, ListStateGraph, RemoveStateGraph, UpdateStateGraph},
    },
};

/// Sert le catalogue de graphes d'états sur le réseau, sur le même modèle
/// que [`crate::model::server::ModelServer`] : un `Arc<Mutex<StateGraphCatalog>>`
/// partagé, exposé par RPC (voir `session::state::rpc`), hébergé sur le pair
/// choisi pour ce namespace (voir
/// [`crate::session::state::client::StateGraphClient::select_catalog`]).
pub struct StateGraphServer;

impl StateGraphServer {
    pub fn new(rpc: &mut RpcServer) {
        let catalog: Arc<Mutex<StateGraphCatalog>> = Arc::new(Mutex::new(StateGraphCatalog::new()));

        GetStateGraph(catalog.clone()).register(rpc);
        ListStateGraph(catalog.clone()).register(rpc);
        InsertStateGraph(catalog.clone()).register(rpc);
        UpdateStateGraph(catalog.clone()).register(rpc);
        RemoveStateGraph(catalog).register(rpc);
    }
}
