pub mod postgres;

use std::sync::Arc;

use async_trait::async_trait;

use crate::expert::{Expert, ExpertId};

/// Stockage CRUD local du catalogue d'experts (voir `expert::catalog::store`),
/// sur le même principe que [`crate::session::store::SessionStore`] (voir sa
/// doc pour la justification du `self` par valeur + `Clone` plutôt que
/// `&self`, et du découpage `insert`/`replace`).
#[async_trait]
pub trait ExpertStorable: Send + Sync + 'static {
    async fn get(&self, id: ExpertId) -> crate::Result<Option<Expert>>;
    async fn insert(&self, value: Expert) -> crate::Result<()>;
    async fn replace(&self, value: Expert) -> crate::Result<()>;
    async fn delete(&self, id: ExpertId) -> crate::Result<()>;
    /// Toutes les entrées actuellement stockées.
    async fn list(&self) -> crate::Result<Vec<Expert>>;
}

#[derive(Clone)]
pub struct ExpertStorage(Arc<dyn ExpertStorable>);

impl ExpertStorage {
    pub fn new<S: ExpertStorable>(store: S) -> Self {
        Self(Arc::new(store))
    }
}

#[async_trait]
impl ExpertStorable for ExpertStorage {
    async fn get(&self, id: ExpertId) -> crate::Result<Option<Expert>> {
        self.0.get(id).await
    }

    async fn insert(&self, value: Expert) -> crate::Result<()> {
        self.0.insert(value).await
    }
    async fn replace(&self, value: Expert) -> crate::Result<()> {
        self.0.replace(value).await
    }
    async fn delete(&self, id: ExpertId) -> crate::Result<()> {
        self.0.delete(id).await
    }
    async fn list(&self) -> crate::Result<Vec<Expert>> {
        self.0.list().await
    }
}
