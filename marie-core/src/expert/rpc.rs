#[cfg(feature="rpc-executor")]
use crate::expert::server::ExpertServer;
use crate::{
    expert::{Expert, ExpertId},
    rpc::Void,
};
use crate::Result;

use marie_macros::core_rpc;

core_rpc! {
    #[rpc(name="/marie/experts/get")]
    /// Récupère la déclaration d'un expert du catalogue, ou `None` si inconnu de
    /// ce nœud — voir [`crate::expert::client::ExpertClient::get`].
    async fn get_expert(self: Self<ExpertServer>, id: ExpertId) -> Result<Option<Expert>> {
        self.0.get(id).await
    }
}

core_rpc! {
    #[rpc(name="/marie/experts/list")]
    async fn list_expert(self: Self<ExpertServer>, args: Void) -> Result<Vec<Expert>> {
        self.0.list().await
    }
}

core_rpc! {
    #[rpc(name="/marie/experts/insert")]
    async fn insert_expert(self: Self<ExpertServer>, expert: Expert) -> Void {
        self.0.insert(expert).await;
        Void
    }
}

core_rpc! {
    #[rpc(name="/marie/experts/replace")]
    async fn replace_expert(self: Self<ExpertServer>, expert: Expert) -> Void {
        self.0.replace(expert).await;
        Void
    }
}

core_rpc! {
    #[rpc(name="/marie/experts/delete")]
    async fn delete_expert(self: Self<ExpertServer>, id: ExpertId) -> Void {
        self.0.delete(id).await;
        Void
    }  
}