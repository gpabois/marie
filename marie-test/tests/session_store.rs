use std::collections::HashMap;
use std::sync::Mutex;

use marie_core::id::IdGenerator;
use marie_core::persistency::SessionStore;
use marie_core::session::{SessionApi, SessionId, crdt::YrsSession};
use yrs::StateVector;

/// Store mémoire minimal pour exercer [`SessionStore`] depuis l'extérieur du
/// crate — stocke la forme encodée (`diff_since`/`from_diff`, voir
/// [`YrsSession`]) plutôt que [`YrsSession`] elle-même : ce type n'est pas
/// `Clone` (il porte un `yrs::Doc`), donc un simple
/// `HashMap<SessionId, YrsSession>` ne permettrait pas de renvoyer une copie
/// depuis `get`.
#[derive(Default)]
struct MemoryStore(Mutex<HashMap<SessionId, Vec<u8>>>);

#[async_trait::async_trait]
impl SessionStore for MemoryStore {
    async fn get(&self, id: &SessionId) -> anyhow::Result<Option<YrsSession>> {
        self.0.lock().unwrap().get(id).map(|bytes| YrsSession::from_diff(bytes)).transpose()
    }

    async fn put(&self, id: &SessionId, value: &YrsSession) -> anyhow::Result<()> {
        self.0.lock().unwrap().insert(*id, value.diff_since(&StateVector::default()));
        Ok(())
    }

    async fn delete(&self, id: &SessionId) -> anyhow::Result<()> {
        self.0.lock().unwrap().remove(id);
        Ok(())
    }

    async fn list(&self) -> anyhow::Result<Vec<YrsSession>> {
        self.0.lock().unwrap().values().map(|bytes| YrsSession::from_diff(bytes)).collect()
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
