use std::sync::Arc;

use parking_lot::Mutex;

use crate::{expert::{GetExpert, InsertExpert, ListExpert, RemoveExpert, UpdateExpert, catalog::ExpertCatalog}, rpc::{RemoteProcedureCall, RpcServer}};

#[derive(Clone)]
pub struct ExpertServer {
    catalog: Arc<Mutex<ExpertCatalog>>
}

impl ExpertServer {
    pub fn new(rpc: &mut RpcServer) {
        let catalog: Arc<Mutex<ExpertCatalog>> = Arc::new(Mutex::new(ExpertCatalog::new()));

        GetExpert(catalog.clone()).register(rpc);
        ListExpert(catalog.clone()).register(rpc);
        InsertExpert(catalog.clone()).register(rpc);
        UpdateExpert(catalog.clone()).register(rpc);
        RemoveExpert(catalog).register(rpc);
    }
}
