use sqlx::Row as _;
use yrs::StateVector;

use crate::{
    persistency::{PostgresStore, RedbStore},
    workspace::{WorkspaceId, crdt::YrsWorkspace},
};

/// Espace de clé (`RedbStore`) / nom de table (`PostgresStore`) dédié aux
/// workspaces — voir la doc de [`WorkspaceStore`].
const NAMESPACE: &str = "workspace";

/// Snapshot complet, encodé comme un diff depuis un vecteur d'état vide (voir
/// [`YrsWorkspace::diff_since`]) — c'est aussi le format que
/// [`YrsWorkspace::from_diff`] sait relire, utilisé symétriquement par
/// [`decode`]. Même principe que `persistency::session::encode`.
fn encode(workspace: &YrsWorkspace) -> Vec<u8> {
    workspace.diff_since(&StateVector::default())
}

fn decode(bytes: &[u8]) -> anyhow::Result<YrsWorkspace> {
    YrsWorkspace::from_diff(bytes)
}

/// Stockage CRUD du contenu CRDT d'un workspace (voir
/// `workspace::crdt::YrsWorkspace`) — utilisé par le nœud `Persistency`
/// (voir `network::persistency`) pour tenir un exemplaire durable de chaque
/// workspace et répondre à `RpcCall::FETCH_WORKSPACE` sans dépendre d'un
/// worker encore en vie. Sur exactement le même principe que
/// `persistency::session::SessionStore` (voir sa doc pour la justification
/// de l'implémentation directe, sans trait CRUD générique).
#[async_trait::async_trait]
pub trait WorkspaceStore: Send + Sync {
    async fn get(&self, id: &WorkspaceId) -> anyhow::Result<Option<YrsWorkspace>>;
    async fn put(&self, id: &WorkspaceId, value: &YrsWorkspace) -> anyhow::Result<()>;
    async fn delete(&self, id: &WorkspaceId) -> anyhow::Result<()>;
    /// Tous les workspaces actuellement stockés.
    async fn list(&self) -> anyhow::Result<Vec<YrsWorkspace>>;

    /// Diff du workspace depuis `state_vector`, ou `None` s'il est inconnu
    /// de ce nœud.
    async fn diff_since(&self, workspace_id: WorkspaceId, state_vector: &StateVector) -> anyhow::Result<Option<Vec<u8>>> {
        let Some(workspace) = self.get(&workspace_id).await? else {
            return Ok(None);
        };
        Ok(Some(workspace.diff_since(state_vector)))
    }
}

#[async_trait::async_trait]
impl WorkspaceStore for RedbStore {
    async fn get(&self, id: &WorkspaceId) -> anyhow::Result<Option<YrsWorkspace>> {
        self.get_raw(NAMESPACE, &id.to_string()).await?.as_deref().map(decode).transpose()
    }

    async fn put(&self, id: &WorkspaceId, value: &YrsWorkspace) -> anyhow::Result<()> {
        self.put_raw(NAMESPACE, &id.to_string(), encode(value)).await
    }

    async fn delete(&self, id: &WorkspaceId) -> anyhow::Result<()> {
        self.delete_raw(NAMESPACE, &id.to_string()).await
    }

    async fn list(&self) -> anyhow::Result<Vec<YrsWorkspace>> {
        self.list_raw(NAMESPACE).await?.iter().map(|bytes| decode(bytes)).collect()
    }
}

#[async_trait::async_trait]
impl WorkspaceStore for PostgresStore {
    async fn get(&self, id: &WorkspaceId) -> anyhow::Result<Option<YrsWorkspace>> {
        let id = id.to_string();
        let row = sqlx::query("SELECT value FROM workspace WHERE id = $1").bind(&id).fetch_optional(self.pool()).await?;
        row.map(|row| decode(&row.get::<Vec<u8>, _>("value"))).transpose()
    }

    async fn put(&self, id: &WorkspaceId, value: &YrsWorkspace) -> anyhow::Result<()> {
        let id = id.to_string();
        let bytes = encode(value);
        sqlx::query("INSERT INTO workspace (id, value) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value")
            .bind(&id)
            .bind(&bytes)
            .execute(self.pool())
            .await?;
        Ok(())
    }

    async fn delete(&self, id: &WorkspaceId) -> anyhow::Result<()> {
        let id = id.to_string();
        sqlx::query("DELETE FROM workspace WHERE id = $1").bind(&id).execute(self.pool()).await?;
        Ok(())
    }

    async fn list(&self) -> anyhow::Result<Vec<YrsWorkspace>> {
        let rows = sqlx::query("SELECT value FROM workspace").fetch_all(self.pool()).await?;
        rows.iter().map(|row| decode(&row.get::<Vec<u8>, _>("value"))).collect()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Mutex};

    use crate::id::IdGenerator;
    use crate::workspace::WorkspaceApi as _;

    use super::*;

    /// Stocke la forme encodée plutôt que [`YrsWorkspace`] elle-même — voir
    /// `persistency::session::tests::MemoryStore` pour la justification
    /// (pas `Clone`, porte un `yrs::Doc`).
    #[derive(Default)]
    struct MemoryStore(Mutex<HashMap<WorkspaceId, Vec<u8>>>);

    #[async_trait::async_trait]
    impl WorkspaceStore for MemoryStore {
        async fn get(&self, id: &WorkspaceId) -> anyhow::Result<Option<YrsWorkspace>> {
            self.0.lock().unwrap().get(id).map(|bytes| decode(bytes)).transpose()
        }

        async fn put(&self, id: &WorkspaceId, value: &YrsWorkspace) -> anyhow::Result<()> {
            self.0.lock().unwrap().insert(*id, encode(value));
            Ok(())
        }

        async fn delete(&self, id: &WorkspaceId) -> anyhow::Result<()> {
            self.0.lock().unwrap().remove(id);
            Ok(())
        }

        async fn list(&self) -> anyhow::Result<Vec<YrsWorkspace>> {
            self.0.lock().unwrap().values().map(|bytes| decode(bytes)).collect()
        }
    }

    #[tokio::test]
    async fn test_unknown_workspace_returns_none() {
        let store = MemoryStore::default();
        let id = IdGenerator::default().next_id();

        assert!(store.get(&id).await.unwrap().is_none());
        assert!(store.diff_since(id, &StateVector::default()).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_put_then_fetch_by_object_and_state_vector() {
        let store = MemoryStore::default();
        let id = IdGenerator::default().next_id();
        let workspace = YrsWorkspace::new(id);

        store.put(&id, &workspace).await.unwrap();

        let reloaded = store.get(&id).await.unwrap().expect("workspace connu après put");
        assert_eq!(reloaded.id(), id);

        let diff = store.diff_since(id, &StateVector::default()).await.unwrap();
        assert!(diff.is_some());
    }
}
