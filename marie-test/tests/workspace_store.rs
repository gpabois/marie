use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use marie_core::id::generate_id;
use marie_core::session::SessionId;
use marie_core::workspace::store::{WorkspaceStore, WorkspaceStoreActor};
use marie_core::workspace::{Workspace, WorkspaceId};
use serde_json::json;

/// Store mémoire minimal pour exercer `workspace::store` depuis l'extérieur
/// du crate, sur le même principe que
/// `marie-test/tests/secret_rotation.rs::MemoryModelStore` (pas de Postgres
/// dans ce crate de tests). Contrairement à l'implémentation Postgres, ne
/// gère pas les horodatages (`created_at`/`last_updated_at`) — c'est une
/// responsabilité du store SQL, pas du contrat `WorkspaceStore` lui-même.
#[derive(Clone, Default)]
struct MemoryWorkspaceStore(Arc<Mutex<HashMap<WorkspaceId, Workspace>>>);

#[async_trait]
impl WorkspaceStore for MemoryWorkspaceStore {
    async fn get(self, id: WorkspaceId) -> anyhow::Result<Option<Workspace>> {
        Ok(self.0.lock().unwrap().get(&id).cloned())
    }

    async fn insert(self, workspace: Workspace) -> anyhow::Result<()> {
        self.0.lock().unwrap().insert(workspace.id, workspace);
        Ok(())
    }

    async fn replace(self, workspace: Workspace) -> anyhow::Result<()> {
        self.0.lock().unwrap().insert(workspace.id, workspace);
        Ok(())
    }

    async fn delete(self, id: WorkspaceId) -> anyhow::Result<()> {
        self.0.lock().unwrap().remove(&id);
        Ok(())
    }

    async fn list(self) -> anyhow::Result<Vec<Workspace>> {
        Ok(self.0.lock().unwrap().values().cloned().collect())
    }
}

#[tokio::test]
async fn get_unknown_returns_none() {
    let store = WorkspaceStoreActor::create(MemoryWorkspaceStore::default());

    let found = store.get(WorkspaceId::new(generate_id())).await.unwrap();
    assert!(found.is_none());
}

#[tokio::test]
async fn insert_then_get_roundtrips() {
    let store = WorkspaceStoreActor::create(MemoryWorkspaceStore::default());

    let mut workspace = Workspace::new(WorkspaceId::new(generate_id()));
    let session_id = SessionId::new(generate_id());
    workspace.add_session(session_id);
    workspace.vars.insert("budget".to_string(), json!(42));

    store.clone().insert(workspace.clone()).await.unwrap();

    let found = store.get(workspace.id).await.unwrap().unwrap();
    assert_eq!(found.sessions, vec![session_id]);
    assert_eq!(found.vars.get("budget"), Some(&json!(42)));
}

#[tokio::test]
async fn replace_overwrites_content() {
    let store = WorkspaceStoreActor::create(MemoryWorkspaceStore::default());

    let mut workspace = Workspace::new(WorkspaceId::new(generate_id()));
    store.clone().insert(workspace.clone()).await.unwrap();

    let session_id = SessionId::new(generate_id());
    workspace.add_session(session_id);
    store.clone().replace(workspace.clone()).await.unwrap();

    let found = store.get(workspace.id).await.unwrap().unwrap();
    assert_eq!(found.sessions, vec![session_id]);
}

#[tokio::test]
async fn delete_removes_the_workspace() {
    let store = WorkspaceStoreActor::create(MemoryWorkspaceStore::default());

    let workspace = Workspace::new(WorkspaceId::new(generate_id()));
    store.clone().insert(workspace.clone()).await.unwrap();
    store.clone().delete(workspace.id).await.unwrap();

    assert!(store.get(workspace.id).await.unwrap().is_none());
}

#[tokio::test]
async fn list_returns_every_workspace() {
    let store = WorkspaceStoreActor::create(MemoryWorkspaceStore::default());

    let first = Workspace::new(WorkspaceId::new(generate_id()));
    let second = Workspace::new(WorkspaceId::new(generate_id()));
    store.clone().insert(first.clone()).await.unwrap();
    store.clone().insert(second.clone()).await.unwrap();

    let mut ids: Vec<WorkspaceId> = store.list().await.unwrap().into_iter().map(|w| w.id).collect();
    let mut expected = vec![first.id, second.id];

    ids.sort_by_key(|id| id.to_string());
    expected.sort_by_key(|id| id.to_string());
    assert_eq!(ids, expected);
}
