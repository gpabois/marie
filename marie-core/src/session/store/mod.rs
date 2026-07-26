#[cfg(feature="postgres")]
pub mod postgres;

use std::sync::Arc;
use async_trait::async_trait;
use crate::session::{Session, SessionId};

/// Stockage CRUD d'une [`Session`] complète (voir `session::model::Session`)
/// — implémenté directement pour [`PgStore`] ci-dessous plutôt que dérivé
/// d'un trait CRUD générique, même principe que
/// `persistency::workspace::WorkspaceStore`/`expert::catalog::store::ExpertStore`.
///
/// [`Self::insert`] et [`Self::replace`] sont volontairement deux méthodes
/// distinctes (pas un simple upsert comme `WorkspaceStore::put`) : elles
/// portent la sémantique de `session::rpc::InsertSession`/`UpdateSession`
/// ("crée" contre "remplace l'état complet d'une session *existante*"), et
/// c'est ce qui permet à l'implémentation Postgres de ne poser
/// `Session::created_at` qu'à la création et de ne jamais y retoucher lors
/// d'un remplacement — voir la doc de ces deux champs.
#[async_trait]
pub trait SessionStorable: Send + Sync + 'static {
    async fn get(&self, id: SessionId) -> crate::Result<Option<Session>>;
    async fn insert(&self, session: Session) -> crate::Result<()>;
    async fn replace(&self, session: Session) -> crate::Result<()>;
    async fn delete(&self, id: SessionId) -> crate::Result<()>;
    async fn list(&self) -> crate::Result<Vec<Session>>;
}

#[derive(Clone)]
pub struct SessionStore(Arc<dyn SessionStorable>);

#[async_trait]
impl SessionStorable for SessionStore {
    async fn get(&self, id: SessionId) -> crate::Result<Option<Session>> {
        self.0.get(id).await
    }
    async fn insert(&self, session: Session) -> crate::Result<()> {
        self.0.insert(session).await
    }
    async fn replace(&self, session: Session) -> crate::Result<()> {
        self.0.replace(session).await
    }
    async fn delete(&self, id: SessionId) -> crate::Result<()> {
        self.0.delete(id).await
    }
    async fn list(&self) -> crate::Result<Vec<Session>> {
        self.0.list().await
    }
}