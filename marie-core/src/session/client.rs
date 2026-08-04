use std::sync::Arc;

use thiserror::Error;

use crate::{
    annuary::{Annuary, capabilities::Capability}, di::{Constructible, Factory, Get, Resolve}, events::{EventBus, EventSubscription}, hitl::{HitlId, model::Answers, protocol::HitlResponse}, rpc::{RpcClient, RpcError}, session::{SessionId, controller::SessionError, dto, protocol::SessionEvent, rpc::ReplyHitl, store::SessionStore}
};

#[derive(Debug, Error)]
pub enum SessionClientError {
    #[error("l'appel distant a échoué: {0}")]
    RpcFailed(#[from] RpcError),
    #[error("aucun controlleur de session n'a été trouvé")]
    NoControllerFound
}

/// Client attaché à une session précise (`session_id` fixé à la
/// construction, voir [`SessionClientFactory`]) — contrairement à une
/// version qui prendrait `session_id` en paramètre de chaque appel, un
/// exemplaire de ce type ne parle jamais que d'une seule session, ce qui
/// correspond à son unique usage réel : un appelant (ex. le REPL du CLI,
/// voir l'état `ExecutingSession`) qui s'attache à une session pour toute
/// la durée de son suivi (flux d'évènements + réponses hitl), jamais à
/// plusieurs à la fois avec le même exemplaire.
#[derive(Clone)]
pub struct SessionClient {
    session_id: SessionId,
    bus: EventBus,
    rpc: RpcClient,
    annuary: Annuary,
    store: SessionStore,
}

impl SessionClient {
    pub fn new(session_id: SessionId, bus: EventBus, rpc: RpcClient, annuary: Annuary, store: SessionStore) -> Self {
        Self { session_id, bus, rpc, annuary, store }
    }

    pub fn stream_session_events(&self) -> EventSubscription<SessionEvent> {
        self.bus.stream_events::<SessionEvent>(
            SessionEvent::session_scope_topic(self.session_id)
        )
    }

    pub async fn hitl_reply(&self, id: HitlId, answers: Answers) -> Result<(), SessionClientError> {
        let response = HitlResponse { session_id: self.session_id, id, answers };

        let controller_id = self.annuary.pick_top_n(response.session_id.as_ref(), Capability::SessionOrchestrator, 1)
            .into_iter()
            .next()
            .ok_or_else(|| SessionClientError::NoControllerFound)?;

        self.rpc.invoke::<ReplyHitl>(response, [controller_id]).await?;

        Ok(())
    }

    /// Récupère l'état courant de la session liée — commence par vérifier
    /// qu'elle existe bien via [`StoreSession::get_session`] (échoue sinon,
    /// ex. `sqlx::Error::RowNotFound` remonté en
    /// [`SessionError::StorageError`]) avant d'aller chercher son journal et
    /// ses requêtes hitl en attente : pas de réponse partielle construite à
    /// partir d'un `session_id` qui ne correspond à aucune session réelle.
    pub async fn get_session(&self) -> Result<dto::SessionView, SessionError> {
        self.store.get_session_view(self.session_id).await.map_err(|err| SessionError::StorageError(Arc::new(err)))
    }
}

/// Fabrique de [`SessionClient`] liés à une session précise — un seul jeu
/// de dépendances partagées (bus d'évènements, client RPC, annuaire, store)
/// résolu une fois à la construction, puis un `SessionClient` frais par
/// appel à [`Factory::create`], sur le même modèle que
/// [`crate::hitl::service::SessionHitlsFactory`]/
/// [`crate::session::logs::SessionLogsFactory`].
pub type SessionClientFactory = Factory<SessionClient, SessionId>;

impl<C> Constructible<C> for SessionClientFactory
    where C: Clone + Send + Sync + 'static
            + Get<SessionStore>
            + Resolve<EventBus>
            + Resolve<RpcClient>
            + Resolve<Annuary>
{
    fn construct(container: &C, _: ()) -> Self {
        let bus: EventBus = container.resolve(());
        let rpc: RpcClient = container.resolve(());
        let annuary: Annuary = container.resolve(());
        let store: SessionStore = container.get();

        Self::new(move |session_id| {
            SessionClient::new(session_id, bus.clone(), rpc.clone(), annuary.clone(), store.clone())
        })
    }
}
