mod protocol;
#[cfg(feature="postgres")]
pub mod postgres;
pub mod client;

use async_trait::async_trait;

pub use client::SessionStoreClient;

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
pub trait SessionStore: Send + Sync + Clone {
    async fn get(self, id: SessionId) -> anyhow::Result<Option<Session>>;
    async fn insert(mut self, session: Session) -> anyhow::Result<()>;
    async fn replace(mut self, session: Session) -> anyhow::Result<()>;
    async fn delete(mut self, id: SessionId) -> anyhow::Result<()>;
    async fn list(self) -> anyhow::Result<Vec<Session>>;
}

