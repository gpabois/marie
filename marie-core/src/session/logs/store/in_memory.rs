use std::collections::HashMap;
use std::collections::hash_map::Entry;

use async_trait::async_trait;
use parking_lot::Mutex;

use crate::session::SessionId;
use crate::session::logs::{SessionLog, SessionLogId};
use crate::session::store::InMemorySessionStore;

use super::{SearchLogQuery, StoreSessionLogs};

/// Implémentation en mémoire de [`StoreSessionLogs`], autonome — contrairement
/// à [`InMemorySessionStore`] (qui délègue à ce type, voir sa doc), elle ne
/// connaît que le journal, pas le reste d'une session. Même principe que
/// [`crate::session::frames::store::InMemorySessionFrameStore`].
#[derive(Default)]
pub struct InMemorySessionLogsStore {
    pub(crate) logs: Mutex<HashMap<(SessionId, SessionLogId), SessionLog>>,
}

impl InMemorySessionLogsStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl StoreSessionLogs for InMemorySessionLogsStore {
    async fn insert_log(&self, log: SessionLog) -> crate::Result<()> {
        match self.logs.lock().entry((log.session_id, log.id)) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().content = log.content;
                entry.get_mut().last_updated_at = log.last_updated_at;
            }
            Entry::Vacant(entry) => {
                entry.insert(log);
            }
        }
        Ok(())
    }

    async fn list_log(&self, session_id: SessionId) -> crate::Result<Vec<SessionLog>> {
        let mut logs: Vec<SessionLog> = self.logs.lock().values().filter(|log| log.session_id == session_id).cloned().collect();
        logs.sort_by_key(|log| log.created_at);
        Ok(logs)
    }

    async fn list_log_after(&self, session_id: SessionId, query: SearchLogQuery) -> crate::Result<Vec<SessionLog>> {
        let mut logs: Vec<SessionLog> = self.logs.lock().values()
            .filter(|log| log.session_id == session_id)
            .filter(|log| query.after.map_or(true, |after| log.created_at > after))
            .filter(|log| query.before.map_or(true, |before| log.created_at < before))
            .cloned()
            .collect();
        logs.sort_by_key(|log| log.created_at);
        Ok(logs)
    }
}

/// Implémentation en mémoire de [`StoreSessionLogs`] pour
/// [`InMemorySessionStore`] — pure délégation à [`InMemorySessionLogsStore`]
/// (voir sa doc pour la logique réelle) : le champ `logs` d'
/// [`InMemorySessionStore`] en est une instance, pas une `HashMap` brute.
#[async_trait]
impl StoreSessionLogs for InMemorySessionStore {
    async fn insert_log(&self, log: SessionLog) -> crate::Result<()> {
        self.logs.insert_log(log).await
    }

    async fn list_log(&self, session_id: SessionId) -> crate::Result<Vec<SessionLog>> {
        self.logs.list_log(session_id).await
    }

    async fn list_log_after(&self, session_id: SessionId, query: SearchLogQuery) -> crate::Result<Vec<SessionLog>> {
        self.logs.list_log_after(session_id, query).await
    }
}
