use std::sync::Arc;
use futures::{FutureExt, TryFutureExt} ;
use moka::future::{Cache, CacheBuilder};

use thiserror::Error;
use tokio::{select, sync::mpsc};
use tokio_stream::StreamExt;
use typed_builder::TypedBuilder;

use crate::{
    catalog::CatalogError, di::{Constructible, Get, Resolve}, events::{Event as _, EventBus}, graph::GraphId, hitl::{protocol::HitlResponse, service::HitlError}, id::IdGenerator, session::{checkpointer::{SessionCheckpointer, SessionCheckpointerFactory},
    protocol::{NewSessionArgs, SessionCheckpointEvent, SessionEvent}, rpc, store::SessionStore}, worker::WorkerError
};

#[cfg(feature = "rpc-executor")]
use crate::rpc::{RemoteProcedureCall as _, RpcServer};

use super::{Session, SessionId};

/// Erreur qu'un gestionnaire `on_*` de [`SessionHandler`] peut renvoyer —
/// point de sortie unique qui déclenche [`SessionHandler::fail`] (voir les
/// délégateurs `on_*` de [`SessionController`]) : quelle que soit la
/// variante, la session en cours bascule en [`SessionStatus::Failed`] avec
/// ce message, et le [`Message`] en cours de traitement est simplement
/// abandonné (pas de retry automatique).
#[derive(Debug, Error)]
pub enum SessionError {
    /// Échec d'une opération de [`StoreSession`] (chargement/écriture d'une
    /// session, d'un frame ou d'un cliché). Pas de `#[from]` : [`crate::Error`]
    /// n'implémente volontairement pas `std::error::Error` (voir sa doc),
    /// donc `thiserror` ne peut pas en dériver `source()` automatiquement —
    /// conversion manuelle via `.map_err(SessionError::StorageError)`, même
    /// idiome que `CatalogError::StoreError`. `Arc` plutôt que `crate::Error`
    /// nu : cette variante traverse aussi les erreurs d'initialisation de
    /// `moka::future::Cache::try_get_with` (voir [`SessionController::get`]/
    /// [`FrameTree::try_get`]/[`Snapshots::try_get`]), qui enveloppe
    /// l'erreur du futur d'init dans un `Arc` pour pouvoir la partager entre
    /// tous les appelants concurrents en attente de la même clé.
    #[error("erreur lors des opérations de stockage: {0}")]
    StorageError(Arc<crate::Error>),
    /// Le superstep relu au moment du CAS (voir
    /// [`SessionHandler::commit_snapshot`]) ne correspond plus à celui
    /// attendu par l'appelant : un autre `commit_snapshot` a pris de
    /// vitesse celui-ci sur le même frame.
    #[error("superstep périmé: attendu {expected}, trouvé {got}")]
    StaleSuperstep {
        expected: u32,
        got: u32
    },
    /// Échec du catalogue de graphes ([`Graphs::latest`]/[`Graphs::get`]) —
    /// typiquement une erreur de stockage ou de désérialisation, pas une
    /// simple absence (voir [`Self::GraphNotFound`] pour ce cas).
    #[error("erreur de catalogue: {0}")]
    CatalogError(#[from] CatalogError),
    /// [`Graphs::latest`] n'a trouvé aucune version active pour ce
    /// [`GraphId`] — distinct de [`Self::CatalogError`] : ici le catalogue a
    /// répondu normalement, il n'y a simplement rien publié sous cet id.
    #[error("graphe introuvable: {0}")]
    GraphNotFound(GraphId),
    /// Le worker n'a pas pu prendre en charge le run (voir
    /// [`SessionHandler::run_frame`]/[`WorkerClient::spawn`]) — aucun
    /// travailleur disponible, timeout de programmation, etc.
    #[error("erreur du worker: {0}")]
    WorkerError(#[from] WorkerError),
    /// Échec d'une opération sur les requêtes human-in-the-loop (voir
    /// [`SessionHandler::append_hitl`]) — contrairement à
    /// [`Self::StorageError`], `#[from]` s'applique ici : [`HitlError`] est
    /// une erreur `thiserror` à part entière (elle implémente
    /// `std::error::Error`), pas [`crate::Error`] nu.
    #[error("erreur hitl: {0}")]
    HitlError(#[from] HitlError),
}

#[derive(TypedBuilder)]
pub struct SessionControllerArgs {
    store: SessionStore,
    events: EventBus,
    session_ckp_factory: SessionCheckpointerFactory,
    id: IdGenerator,
    #[cfg(feature = "rpc-executor")]
    rpc: RpcServer,
}

#[derive(Clone)]
pub struct SessionController {
    store: SessionStore,
    events: EventBus,
    id: IdGenerator,
    session_ckp_factory: SessionCheckpointerFactory,
    sessions: Cache<SessionId, Arc<SessionCheckpointer>>,
    queue: mpsc::UnboundedSender<SessionCheckpointEvent>
}

#[cfg(feature = "rpc-executor")]
impl<C> Constructible<C> for SessionController
    where C: Get<SessionStore>
            + Resolve<EventBus>
            + Resolve<RpcServer>
            + Resolve<SessionCheckpointerFactory>
            + Resolve<IdGenerator>
{
    fn construct(container: &C, _: ()) -> Self {
        let args = SessionControllerArgs::builder()
            .store(container.get())
            .session_ckp_factory(container.resolve(()))
            .rpc(container.resolve(()))
            .events(container.resolve(()))
            .id(container.resolve(()))
            .build();

        Self::new(args)
    }
}

#[cfg(not(feature = "rpc-executor"))]
impl<C> Constructible<C> for SessionController
    where C: Get<SessionStore>
            + Resolve<EventBus>
            + Resolve<SessionCheckpointerFactory>
            + Resolve<IdGenerator>
{
    fn construct(container: &C, _: ()) -> Self {
        let args = SessionControllerArgs::builder()
            .store(container.get())
            .session_ckp_factory(container.resolve(()))
            .events(container.resolve(()))
            .id(container.resolve(()))
            .build();

        Self::new(args)
    }
}

impl SessionController {
    /// Construit un `SessionController` et démarre immédiatement sa boucle
    /// de traitement (voir [`Self::run`]) sur une tâche tokio détachée —
    /// l'appelant ne récupère jamais l'exemplaire qui a servi à spawner,
    /// seulement des clones bon marché : les champs de `SessionController`
    /// sont eux-mêmes des poignées partagées (`SessionStore`, `WorkerClient`,
    /// `Cache`, `mpsc::UnboundedSender`), donc `new` et tous ses `clone()`
    /// parlent à la même boucle et à la même queue de messages. C'est aussi
    /// ici qu'est branché l'`eviction_listener` du cache de sessions : une
    /// session évincée (ou expirée) de `self.sessions` est persistée dans
    /// `store` avant d'être perdue.
    ///
    /// # Exemple
    ///
    /// ```ignore
    /// let controller = SessionController::new(SessionControllerArgs {
    ///     store: SessionStore::new(Arc::new(PgStore::connect("postgres://...").await?)),
    ///     events: EventService::new(/* ... */),
    ///     worker: WorkerClient::new(/* ... */),
    /// });
    ///
    /// // `controller` peut être cloné librement ; chaque clone partage la
    /// // même boucle de traitement démarrée par cet appel à `new`.
    /// let handle = controller.clone();
    /// ```
    pub fn new(args: SessionControllerArgs) -> Self {
        let (queue_tx, queue_rx) = mpsc::unbounded_channel();

        let sessions = CacheBuilder::new(300)
            .async_eviction_listener(|_: Arc<SessionId>, session: Arc<SessionCheckpointer>, cause: moka::notification::RemovalCause| async move {
                session
                .close()
                .await;
            }.boxed())
            .build();

        let controller = Self {
            store: args.store,
            sessions,
            session_ckp_factory: args.session_ckp_factory,
            events: args.events,
            id: args.id,
            queue: queue_tx,
        };

        #[cfg(feature = "rpc-executor")]
        {
            let mut rpc = args.rpc;
            rpc::CreateSession::new(controller.clone()).register(&mut rpc);
            rpc::ReplyHitl::new(controller.clone()).register(&mut rpc);
        }

        tokio::spawn(controller.clone().run(queue_rx));

        controller
    }

    pub async fn reply_hitl(&self, response: HitlResponse) -> Result<(), SessionError> {
        let _ = self.queue.send(SessionCheckpointEvent::HitlAnswered{session_id: response.session_id, response});
        Ok(())
    }

    pub async fn create_session(&self, args: NewSessionArgs) -> Result<SessionId, SessionError> {
        let id: SessionId = self.id.next();

        let session = Session::new(id);
        let ckp = self.session_ckp_factory.create((session, self.queue.clone()));
        ckp.initialise_as_new(args).await?;

        Ok(id)
    }

    /// Toutes les sessions connues (voir [`StoreSession::list_sessions`]) —
    /// lecture directe du store, sans passer par [`Self::get`]/le cache
    /// `self.sessions` : un consommateur de gestion (ex. `list sessions` du
    /// CLI) veut l'état persisté de toutes les sessions, pas seulement
    /// celles actuellement chargées en mémoire.
    pub async fn list_sessions(&self) -> Result<Vec<Session>, SessionError> {
        self.store.list_sessions().await.map_err(|err| SessionError::StorageError(Arc::new(err)))
    }

    /// Supprime `id` — invalide d'abord le [`SessionCheckpointer`] éventuellement
    /// encore chargé en mémoire (sans quoi l'éviction du cache le
    /// persisterait de nouveau dans le store après coup, voir
    /// l'`eviction_listener` branché dans [`Self::new`]), puis supprime la
    /// ligne persistée via [`StoreSession::delete_session`].
    pub async fn delete_session(&self, id: SessionId) -> Result<(), SessionError> {
        self.sessions.invalidate(&id).await;
        self.store.delete_session(&id).await.map_err(|err| SessionError::StorageError(Arc::new(err)))?;
        Ok(())
    }

    /// Boucle de traitement principale, spawnée une seule fois par
    /// [`Self::new`] — consomme indéfiniment `queue` et délègue chaque
    /// [`Message`] à [`Self::process_message`]. Prend `self` par valeur
    /// (pas `&mut self`) pour pouvoir être passée telle quelle à
    /// `tokio::spawn`, qui exige un futur `'static` : c'est pour ça que
    /// `SessionController` ne porte que des poignées `Clone` bon marché
    /// plutôt que des données possédées à emprunter.
    ///
    /// # Exemple
    ///
    /// ```ignore
    /// let (tx, rx) = mpsc::unbounded_channel();
    /// tokio::spawn(controller.clone().run(rx));
    /// // Plus tard, tout envoi sur `tx` (ou sur `controller.queue`, le même
    /// // canal) sera consommé par cette boucle et traité par
    /// // `process_message`.
    /// ```
    async fn run(self, mut queue: mpsc::UnboundedReceiver<SessionCheckpointEvent>) {
        let mut sessions_events_stream = self.events.stream_events::<SessionEvent>(SessionEvent::TOPIC);
        loop {
            select! {
                Some(event) = sessions_events_stream.next() 
                    => self.process_session_event(event.payload).await,
                Some(msg) = queue.recv() 
                    => self.process_checkpoint_event(msg).await
            }
        }
    }

    async fn process_session_event(&self, event: SessionEvent) {
        match event {
            SessionEvent::HitlAnswered(response) 
                => {
                    let ckp_event = SessionCheckpointEvent::HitlAnswered { session_id: response.session_id, response };
                    self.process_checkpoint_event(ckp_event).await
            },
            _ => {}
        }
    }

    async fn process_checkpoint_event(&self, event: SessionCheckpointEvent) {
        let Ok(handler) = self.get(&event.session_id()).await else { return };
        handler.send_ckp_event(event);
    }


    /// Résout `id` vers sa [`Session`] en mémoire, chargée depuis
    /// `self.store` au premier accès puis gardée dans `self.sessions`
    /// (voir l'`eviction_listener` branché dans [`Self::new`], qui
    /// persiste la session dans `store` au moment de son éviction du
    /// cache) — le point d'entrée unique de toute méthode qui a besoin de
    /// lire/modifier une session, pour que le chargement paresseux et la
    /// mise en cache restent centralisés ici plutôt que dupliqués à chaque
    /// appelant.
    ///
    /// # Exemple
    ///
    /// ```ignore
    /// let Ok(session) = self.get(&session_id).await else { return };
    /// let guard = session.lock();
    /// // ... lecture de `guard.frames` — à libérer avant tout `.await`
    /// // suivant, voir le correctif Send dans
    /// // `Self::handle_terminated_frame_run`/`Self::run_frame`.
    /// ```
    async fn get(&self, id: &SessionId) -> Result<Arc<SessionCheckpointer>, SessionError> {
        let result = self
            .sessions
            .try_get_with(*id, self.instantiate_session_ckp(id))
            .map_err(|err| SessionError::StorageError(err))
            .await?;

        Ok(result)
    }

    async fn instantiate_session_ckp(&self, id: &SessionId) -> crate::Result<Arc<SessionCheckpointer>> {
        let session = self.store.get_session(id).await?;
        let ckp = self.session_ckp_factory.create((session, self.queue.clone()));
        Ok(Arc::new(ckp))
    }

}
