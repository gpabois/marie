use sqlx::Row as _;
use yrs::StateVector;

use crate::{
    persistency::{PostgresStore, RedbStore},
    session::{SessionId, crdt::YrsSession},
};

/// Espace de clé (`RedbStore`) / nom de table (`PostgresStore`) dédié aux
/// sessions — voir la doc de [`SessionStore`].
const NAMESPACE: &str = "session";

/// Snapshot complet, encodé comme un diff depuis un vecteur d'état vide (voir
/// [`YrsSession::diff_since`]) — c'est aussi le format que
/// [`YrsSession::from_diff`] sait relire, utilisé symétriquement par
/// [`decode`].
fn encode(session: &YrsSession) -> Vec<u8> {
    session.diff_since(&StateVector::default())
}

fn decode(bytes: &[u8]) -> anyhow::Result<YrsSession> {
    YrsSession::from_diff(bytes)
}

/// Stockage CRUD du contenu CRDT d'une session (voir
/// `session::crdt::YrsSession`) — utilisé par le nœud `Persistency` (voir
/// `network::persistency`) pour tenir un exemplaire durable de chaque
/// session et répondre à `RpcCall::FETCH_SESSION` sans dépendre d'un worker
/// encore en vie. Implémenté directement pour [`RedbStore`] et
/// [`PostgresStore`] ci-dessous plutôt que dérivé d'un trait CRUD générique :
/// ce trait est le seul point d'entrée public, spécifique au contenu de
/// session (pas de fuite d'un `Id`/`T` générique dans la signature).
#[async_trait::async_trait]
pub trait SessionStore: Send + Sync {
    async fn get(&self, id: &SessionId) -> anyhow::Result<Option<YrsSession>>;
    async fn put(&self, id: &SessionId, value: &YrsSession) -> anyhow::Result<()>;
    async fn delete(&self, id: &SessionId) -> anyhow::Result<()>;
    /// Toutes les sessions actuellement stockées.
    async fn list(&self) -> anyhow::Result<Vec<YrsSession>>;

    /// Diff de la session depuis `state_vector`, ou `None` si elle est
    /// inconnue de ce nœud — évite de transférer tout le contenu CRDT quand
    /// un pair n'a besoin que de ce qui lui manque (voir
    /// `RpcCall::FETCH_SESSION`).
    async fn diff_since(&self, session_id: SessionId, state_vector: &StateVector) -> anyhow::Result<Option<Vec<u8>>> {
        let Some(session) = self.get(&session_id).await? else {
            return Ok(None);
        };
        Ok(Some(session.diff_since(state_vector)))
    }
}

#[async_trait::async_trait]
impl SessionStore for RedbStore {
    async fn get(&self, id: &SessionId) -> anyhow::Result<Option<YrsSession>> {
        self.get_raw(NAMESPACE, &id.to_string()).await?.as_deref().map(decode).transpose()
    }

    async fn put(&self, id: &SessionId, value: &YrsSession) -> anyhow::Result<()> {
        self.put_raw(NAMESPACE, &id.to_string(), encode(value)).await
    }

    async fn delete(&self, id: &SessionId) -> anyhow::Result<()> {
        self.delete_raw(NAMESPACE, &id.to_string()).await
    }

    async fn list(&self) -> anyhow::Result<Vec<YrsSession>> {
        self.list_raw(NAMESPACE).await?.iter().map(|bytes| decode(bytes)).collect()
    }
}

#[async_trait::async_trait]
impl SessionStore for PostgresStore {
    async fn get(&self, id: &SessionId) -> anyhow::Result<Option<YrsSession>> {
        let id = id.to_string();
        let row = sqlx::query("SELECT value FROM session WHERE id = $1").bind(&id).fetch_optional(self.pool()).await?;
        row.map(|row| decode(&row.get::<Vec<u8>, _>("value"))).transpose()
    }

    async fn put(&self, id: &SessionId, value: &YrsSession) -> anyhow::Result<()> {
        let id = id.to_string();
        let bytes = encode(value);
        sqlx::query("INSERT INTO session (id, value) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value")
            .bind(&id)
            .bind(&bytes)
            .execute(self.pool())
            .await?;
        Ok(())
    }

    async fn delete(&self, id: &SessionId) -> anyhow::Result<()> {
        let id = id.to_string();
        sqlx::query("DELETE FROM session WHERE id = $1").bind(&id).execute(self.pool()).await?;
        Ok(())
    }

    async fn list(&self) -> anyhow::Result<Vec<YrsSession>> {
        let rows = sqlx::query("SELECT value FROM session").fetch_all(self.pool()).await?;
        rows.iter().map(|row| decode(&row.get::<Vec<u8>, _>("value"))).collect()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Mutex};

    use crate::id::IdGenerator;
    use crate::session::SessionApi;

    use super::*;

    /// Stocke la forme encodée (voir [`encode`]/[`decode`]) plutôt que
    /// [`YrsSession`] elle-même : ce type n'est pas `Clone` (il porte un
    /// `yrs::Doc`), donc un simple `HashMap<SessionId, YrsSession>` ne
    /// permettrait pas de renvoyer une copie depuis [`SessionStore::get`].
    #[derive(Default)]
    struct MemoryStore(Mutex<HashMap<SessionId, Vec<u8>>>);

    #[async_trait::async_trait]
    impl SessionStore for MemoryStore {
        async fn get(&self, id: &SessionId) -> anyhow::Result<Option<YrsSession>> {
            self.0.lock().unwrap().get(id).map(|bytes| decode(bytes)).transpose()
        }

        async fn put(&self, id: &SessionId, value: &YrsSession) -> anyhow::Result<()> {
            self.0.lock().unwrap().insert(*id, encode(value));
            Ok(())
        }

        async fn delete(&self, id: &SessionId) -> anyhow::Result<()> {
            self.0.lock().unwrap().remove(id);
            Ok(())
        }

        async fn list(&self) -> anyhow::Result<Vec<YrsSession>> {
            self.0.lock().unwrap().values().map(|bytes| decode(bytes)).collect()
        }
    }

    #[tokio::test]
    async fn test_unknown_session_returns_none() {
        let store = MemoryStore::default();
        let id = IdGenerator::default().next_id();

        assert!(store.get(&id).await.unwrap().is_none());
        assert!(store.diff_since(id, &StateVector::default()).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_put_then_fetch_by_object_and_state_vector() {
        let store = MemoryStore::default();
        let id = IdGenerator::default().next_id();
        let session = YrsSession::new(id);

        store.put(&id, &session).await.unwrap();

        let reloaded = store.get(&id).await.unwrap().expect("session connue après put");
        assert_eq!(reloaded.id(), id);

        let diff = store.diff_since(id, &StateVector::default()).await.unwrap();
        assert!(diff.is_some());
    }
}
