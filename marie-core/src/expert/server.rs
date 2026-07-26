use crate::{
    di::{Factory, Get}, 
    expert::{Expert, ExpertId, store::{ExpertStorable as _, ExpertStorage}}, 
    rpc::{RemoteProcedureCall, RpcServer}
};

use super::rpc::*;

#[derive(Clone)]
pub struct ExpertServer {
    storage: ExpertStorage
}

impl<C> Factory<C> for ExpertServer 
    where C: Get<RpcServer> + Get<ExpertStorage> 
{
    
    fn create(container: &C) -> Self {
        ExpertServer::new(container.get(), container.get())
    }
}

impl ExpertServer {
    pub fn new(mut rpc: RpcServer, storage: ExpertStorage) -> Self {
        let experts = ExpertServer {storage};

        GetExpert::new(experts.clone()).register(&mut rpc);
        ListExpert::new(experts.clone()).register(&mut rpc);
        InsertExpert::new(experts.clone()).register(&mut rpc);
        ReplaceExpert::new(experts.clone()).register(&mut rpc);
        DeleteExpert::new(experts.clone()).register(&mut rpc);

        experts
    }

    pub async fn get(&self, id: ExpertId) -> crate::Result<Option<Expert>> {
        self.storage.get(id).await
    }

    pub async fn list(&self) -> crate::Result<Vec<Expert>> {
        self.storage.list().await
    }

    pub async fn insert(&self, expert: Expert) -> crate::Result<()> {
        self.storage.insert(expert).await
    }

    pub async fn replace(&self, expert: Expert) -> crate::Result<()> {
        self.storage.replace(expert).await
    }
    pub async fn delete(&self, id: ExpertId) -> crate::Result<()> {
        self.storage.delete(id).await
    }
}
