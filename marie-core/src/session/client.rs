use std::sync::Arc;

use crate::{
    annuary::Annuary, events::{EventBus, EventSubscription}, hitl::protocol::HitlResponse, rpc::RpcClient, session::{SessionId, controller::SessionError, dto, hitls::Hitls, logs::SessionsLogs, protocol::SessionEvent, store::SessionStore}
};

pub struct SessionClient {
    bus: EventBus,
    rpc: RpcClient,
    annuary: Annuary,
    store: SessionStore,
    session_logs: SessionsLogs,
    hitls: Hitls
}

impl SessionClient {
    pub fn stream_session_events(&self, session_id: SessionId) -> EventSubscription<SessionEvent> {
        self.bus.stream_events::<SessionEvent>(
            SessionEvent::session_scope_topic(session_id)
        )
    }

    pub fn hitl_reply(&self, Response: HitlResponse) {
        
    }

    /// Récupère l'état courant de la session `session_id` — commence par
    /// vérifier qu'elle existe bien via [`StoreSession::get_session`]
    /// (échoue sinon, ex. `sqlx::Error::RowNotFound` remonté en
    /// [`SessionError::StorageError`]) avant d'aller chercher son journal et
    /// ses requêtes hitl en attente : pas de réponse partielle construite à
    /// partir d'un `session_id` qui ne correspond à aucune session réelle.
    pub async fn get_session(&self, session_id: SessionId) -> Result<dto::SessionView, SessionError> {
        self.store.get_session_view(session_id).await.map_err(|err| SessionError::StorageError(Arc::new(err)))
    }
}