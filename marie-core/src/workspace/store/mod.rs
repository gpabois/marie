pub mod postgres;

use std::sync::Arc;

use async_trait::async_trait;

use crate::workspace::{Workspace, WorkspaceId};

/// Stockage CRUD d'un [`Workspace`] complet — même modèle que
/// `session::store::SessionStore`, y compris le split volontaire
/// [`Self::insert`]/[`Self::replace`] (pas un simple upsert) : il porte la
/// sémantique de `workspace::rpc::InsertWorkspace`/mutations ("crée" contre
/// "remplace l'état complet d'un workspace *existant*"), et c'est ce qui
/// permet à l'implémentation Postgres de ne poser [`Workspace::created_at`]
/// qu'à la création et de ne jamais y retoucher lors d'un remplacement —
/// voir la doc de ces deux champs.
#[async_trait]
pub trait WorkspaceStorable: Send + Sync + 'static {
    async fn get(&self, id: WorkspaceId) -> crate::Result<Option<Workspace>>;
    async fn insert(&self, workspace: Workspace) -> crate::Result<()>;
    async fn replace(&self, workspace: Workspace) -> crate::Result<()>;
    async fn delete(&self, id: WorkspaceId) -> crate::Result<()>;
    async fn list(&self) -> crate::Result<Vec<Workspace>>;
}

#[derive(Clone)]
pub struct WorkspaceStore(Arc<dyn WorkspaceStorable>);

#[async_trait]
impl WorkspaceStorable for WorkspaceStore {
    async fn get(&self, id: WorkspaceId) -> crate::Result<Option<Workspace>> {
        self.0.get(id).await
    }
    
    async fn insert(&self, workspace: Workspace) -> crate::Result<()> {
        self.0.insert(workspace).await
    }
    
    async fn replace(&self, workspace: Workspace) -> crate::Result<()> {
        self.0.replace(workspace).await
    }
    
    async fn delete(&self, id: WorkspaceId) -> crate::Result<()> {
        self.0.delete(id).await
    }

    async fn list(&self) -> crate::Result<Vec<Workspace>> {
        self.0.list().await
    }
}