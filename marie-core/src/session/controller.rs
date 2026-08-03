use std::sync::Arc;
use futures::TryFutureExt ;
use moka::future::{Cache, CacheBuilder};
use std::collections::HashMap;
use thiserror::Error;
use tokio::{select, sync::mpsc};
use tokio_stream::StreamExt;
use typed_builder::TypedBuilder;

use crate::{
    catalog::CatalogError, di::{Constructible, Get, Resolve}, events::EventBus, expert::{ExpertAskId, RequestAskExpert}, graph::{GraphId, GraphRef, Graphs, NodeId}, hitl::{Answer, HitlFrame, HitlId, HitlRequest, model::Answers, protocol::HitlResponse, service::HitlError}, id::generate_id, job::JobState, rpc::{RemoteProcedureCall as _, RpcServer}, session::{channel::{ChannelName, ChannelUpdate, Reducer}, checkpointer::{SessionCheckpointer, SessionCheckpointerFactory}, frames::{FrameData, FrameId, FramePolicy, FrameSpecRef::{self, Hitl}, FrameStatus, FrameTree, NewFrameNodeArgs, ParentPolicy}, hitls::Hitls, logs::SessionsLogs, protocol::{Branch, CreateSessionArgs, FrameResponse, SessionCheckpointEvent, SessionEvent}, rpc, run_log::RunLogContent, snapshot::{SessionSnapshots, Snapshot, SnapshotRef}, store::SessionStore, worker::{RunFrame, RunFrameArgs}}, tools::{RequestToolCall, ToolCallId}, worker::{WorkerClient, WorkerError}
};

use super::{Session, SessionId, SessionStatus};

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
    rpc: RpcServer,
}

#[derive(Clone)]
pub struct SessionController {
    store: SessionStore,
    events: EventBus,
    session_ckp_factory: SessionCheckpointerFactory,
    sessions: Cache<SessionId, Arc<SessionCheckpointer>>,
    queue: mpsc::UnboundedSender<SessionCheckpointEvent>
}

impl<C> Constructible<C> for SessionController 
    where C: Get<SessionStore> 
            + Resolve<EventBus>
            + Resolve<RpcServer>
            + Resolve<SessionCheckpointerFactory>
{
    fn construct(container: &C, _: ()) -> Self {
        let args = SessionControllerArgs::builder()
            .store(container.get())
            .session_ckp_factory(container.resolve(()))
            .rpc(container.resolve(()))
            .events(container.resolve(()))
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
    pub fn new(mut args: SessionControllerArgs) -> Self {
        let (queue_tx, queue_rx) = mpsc::unbounded_channel();

        let sessions = CacheBuilder::new(300)
            .build();

        let controller = Self {
            store: args.store,
            sessions,
            session_ckp_factory: args.session_ckp_factory,
            events: args.events,
            queue: queue_tx,
        };

        rpc::CreateSession::new(controller.clone()).register(&mut args.rpc);

        tokio::spawn(controller.clone().run(queue_rx));

        controller
    }

    pub async fn create_session(&self, args: CreateSessionArgs) -> Result<SessionId, SessionError> {
        let session_id = SessionId::new(generate_id());
        let session = Session::new(session_id);
        


        match args {
            CreateSessionArgs::Shell(shell_mode) => {
                
            },
            CreateSessionArgs::Graph { graph_id, initial } => todo!(),
        }
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
    async fn run(mut self, mut queue: mpsc::UnboundedReceiver<SessionCheckpointEvent>) {
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
            SessionEvent::HitlAnswered(HitlResponse { session_id, id, answers }) 
                => self.on_hitl_response(session_id, hitl_id, answers),
        }
    }

    /// Traite un [`Message`] unique — le point d'entrée de la machine à
    /// états de `SessionController` : chaque variante correspond à une
    /// étape du cycle de vie d'un frame (création, agrégation des enfants,
    /// déclenchement d'un run, suivi de son état, terminaison), et
    /// `process_message` elle-même n'est qu'un aiguillage vers la méthode
    /// `handle_*`/`mark_*` correspondante (documentées individuellement
    /// ci-dessous). C'est cette méthode déléguée, pas `process_message`,
    /// qui peut ré-émettre un nouveau `Message` sur `self.queue` pour
    /// enchaîner l'étape suivante plutôt que d'appeler directement l'étape
    /// suivante — ce qui garde chaque étape atomique vis-à-vis de la boucle
    /// [`Self::run`] et évite les races entre deux traitements concurrents
    /// du même frame.
    ///
    /// # Exemple
    ///
    /// ```ignore
    /// // Émis en interne (ex: depuis une méthode `handle_*`), jamais
    /// // appelé directement par du code hors de ce module.
    /// self.process_message(Message::FrameTerminated { session_id, frame_id }).await;
    /// ```
    async fn process_checkpoint_event(&mut self, msg: SessionCheckpointEvent) {
        use SessionCheckpointEvent::{FrameCreated, ChildFrameTerminated, FrameTerminated, FrameReady, FrameRunJobStateUpdate, FrameRunTerminated};

        match msg {
            FrameCreated { session_id, frame_id } 
                => self.on_frame_created(session_id, frame_id).await,
            // Une frame a terminé (done ou failed)
            // 1. On vérifie si une frame parent attend que ses enfants aient terminés
            // 2. Si tous les enfants ont terminés, on va envoyer un message-évènement `AllChildrenFrameHaveTerminated`
            FrameTerminated { session_id, frame_id } 
                => self.on_frame_terminated(session_id, frame_id).await,
            // On va déclencher un run
            FrameReady { session_id, frame_id } 
                => self.on_frame_ready(session_id, frame_id).await,
            // On a terminé un run
            FrameRunJobStateUpdate { session_id, frame_id, job_state}
                => self.on_frame_run_update(session_id, frame_id, job_state).await,
            FrameRunTerminated { session_id, frame_id } 
                => self.on_frame_terminated(session_id, frame_id).await,
            ChildFrameTerminated { session_id, parent_id, child_id } 
                => self.on_child_frame_terminated(session_id, parent_id, child_id).await
        }
    }

    async fn on_hitl_response(&self, session_id: SessionId, hitl_id: HitlId, answers: Answers) {
        let Ok(handler) = self.get(&session_id).await else { return };
        if let Err(err) = handler.on_hitl_response(&hitl_id, answers).await {
            handler.fail(err).await;
        }       
    }

    async fn on_child_frame_terminated(&self, session_id: SessionId, parent_id: FrameId, child_id: FrameId) {
        let Ok(handler) = self.get(&session_id).await else { return };
        if let Err(err) = handler.on_child_frame_terminated(&parent_id, &child_id).await {
            handler.fail(err).await;
        }
    }

    /// Réagit à [`Message::FrameCreated`] : un frame tout juste créé (voir
    /// [`Self::append_frame`]) est immédiatement prêt à tourner, puisqu'il
    /// n'a par construction aucun enfant à attendre — délègue donc
    /// directement à [`Self::mark_frame_as_ready`].
    ///
    /// # Exemple
    ///
    /// ```ignore
    /// self.handle_created_frame(session_id, frame_id).await;
    /// ```
    async fn on_frame_created(&self, session_id: SessionId, frame_id: FrameId) {
        let Ok(handler) = self.get(&session_id).await else { return };
        if let Err(err) = handler.on_frame_created(&frame_id).await {
            handler.fail(err).await;
        }
    }

    /// Réagit à [`Message::FrameRunTerminated`] : relit le statut que
    /// [`Self::handle_frame_run_update`] vient d'écrire et le fait
    /// progresser — un échec devient [`FrameStatus::Failed`] puis pousse
    /// [`Message::FrameTerminated`] (voir [`Self::handle_terminated_frame`]),
    /// une complétion délègue à [`Self::handle_frame_run_completion`] pour
    /// interpréter la [`FrameResponse`] produite par le run.
    ///
    /// # Exemple
    ///
    /// ```ignore
    /// self.handle_terminated_frame_run(session_id, frame_id).await;
    /// ```
    async fn on_frame_run_terminated(&self, session_id: SessionId, frame_id: FrameId) {
        let Ok(session) = self.get(&session_id).await else { return };
        if let Err(err) = session.on_frame_run_terminated(&frame_id).await {
            session.fail(err).await;
        }
    }

    /// Réagit à [`Message::FrameRunJobStateUpdate`] : traduit le
    /// [`JobState`] brut renvoyé par le worker (voir [`Self::run_frame`])
    /// en [`FrameStatus::RunCompleted`]/[`FrameStatus::RunFailed`], puis
    /// pousse [`Message::FrameRunTerminated`] pour que
    /// [`Self::handle_terminated_frame_run`] prenne le relais — les autres
    /// variantes de [`JobState`] (mises à jour intermédiaires, sans
    /// équivalent en [`FrameStatus`]) sont ignorées.
    ///
    /// # Exemple
    ///
    /// ```ignore
    /// self.handle_frame_run_update(session_id, frame_id, job_state).await;
    /// ```
    async fn on_frame_run_update(&self, session_id: SessionId, frame_id: FrameId, job_state: JobState<FrameResponse>) {
        let Ok(session) = self.get(&session_id).await else { return };
        if let Err(err) = session.on_frame_run_update(&frame_id, job_state).await {
            session.fail(err).await;
        }
    }

    /// Réagit à [`Message::FrameReady`] en spawnant [`Self::run_frame`] sur
    /// sa propre tâche tokio plutôt que de l'attendre ici — un run peut
    /// durer arbitrairement longtemps (appel modèle, tool, etc.) et ne doit
    /// pas bloquer la boucle [`Self::run`] pendant ce temps, qui doit
    /// rester libre de traiter les autres frames/sessions en attente. Seule
    /// méthode `handle_*`/`mark_*` non `async` : elle ne fait qu'initier le
    /// spawn, sans rien attendre elle-même.
    ///
    /// # Exemple
    ///
    /// ```ignore
    /// self.handle_ready_frame(session_id, frame_id);
    /// ```
    async fn on_frame_ready(&self, session_id: SessionId, frame_id: FrameId) {
        let Ok(session) = self.get(&session_id).await else { return };
        if let Err(err) = session.on_frame_ready(&frame_id).await {
            session.fail(err).await;
        }
    }

    /// Réagit à [`Message::FrameTerminated`]/[`Message::FrameRunTerminated`] :
    /// si `frame_id` a un parent et que tous les enfants de ce dernier ont
    /// désormais atteint un statut terminal (voir [`all_have_terminated`]),
    /// pousse [`Message::AllChildrenFrameHaveTerminated`] pour que
    /// [`Self::handle_all_terminated_children`] prenne le relais — ne fait
    /// rien si `frame_id` est la racine (pas de parent à débloquer) ou si
    /// d'autres enfants sont encore en cours.
    ///
    /// # Exemple
    ///
    /// ```ignore
    /// self.handle_terminated_frame(session_id, frame_id).await;
    /// ```
    async fn on_frame_terminated(&self, session_id: SessionId, frame_id: FrameId) {
        let Ok(handler) = self.get(&session_id).await else { return };
        if let Err(err) = handler.on_frame_terminated(&frame_id).await {
            handler.fail(err).await;
        }
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
            .try_get_with(*id, SessionCheckpointer::load(*id, self.clone()))
            .map_err(|err| SessionError::StorageError(err))
            .await?;

        Ok(result)
    }

    fn instantiate_session_handler(&self, session: Session) -> SessionCheckpointer {
        let handler = SessionCheckpointer::load(*id, self.clone());
    }

    /// Journal de `session_id` — accès direct, sans passer par
    /// [`Self::get`]/[`SessionHandler`] : contrairement à [`FrameTree`]/
    /// [`Snapshots`]/[`Hitls`], [`SessionsLogs`] ne porte aucun cache à
    /// partager entre appelants, donc pas besoin de faire charger tout
    /// l'état d'une session (frames, clichés) juste pour y écrire une
    /// entrée de journal.
    pub fn logs(&self) -> SessionsLogs {
        SessionsLogs::new(self.store.clone(), self.events.clone())
    }

    fn emit(&self, msg: Message) {
        let _ =self.queue.send(msg);
    }
}
