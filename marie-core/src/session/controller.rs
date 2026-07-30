use std::{collections::HashMap, sync::Arc};
use futures::{StreamExt, TryFutureExt, stream};
use moka::future::{Cache, CacheBuilder};
use parking_lot::Mutex;
use serde_json::Value;
use thiserror::Error;
use tokio::{select, sync::mpsc};

use crate::{
    events::EventService, 
    graph::{GraphId, GraphRef, Graphs, NodeId}, 
    job::JobState, 
    session::{channel::{ChannelName, ChannelUpdate}, 
    controller::Message::FrameCreated, 
    frames::{FrameData, FrameId, FramePolicy, FrameSpecRef, FrameStatus, FrameTree, NewFrameNodeArgs, ParentPolicy}, 
    protocol::FrameResponse, snapshot::{Snapshot, SnapshotRef, Snapshots}, 
    store::SessionStore, worker::{RunFrame, RunFrameArgs}}, 
    store::PgStore, 
    worker::WorkerClient
};

use super::{Session, SessionId};

#[derive(Debug, Error)]
pub enum SessionError {
    #[error("erreur lors des opérations de stockage: {0}")]
    StorageError(#[from] Arc<sqlx::Error>),
    #[error("superstep périmé: attendu {expected}, trouvé {got}")]
    StaleSuperstep {
        expected: u32,
        got: u32
    }
}
pub struct SessionControllerArgs {
    store: PgStore,
    graphs: Graphs,
    events: EventService,
    worker: WorkerClient
}

#[derive(Clone)]
pub struct SessionController {
    store: PgStore,
    worker: WorkerClient,
    graphs: Graphs,
    sessions: Cache<SessionId, Arc<SessionHandler>>,
    queue: mpsc::UnboundedSender<Message>
}

impl SessionController {
    /// Construit un `SessionController` et démarre immédiatement sa boucle
    /// de traitement (voir [`Self::run`]) sur une tâche tokio détachée —
    /// l'appelant ne récupère jamais l'exemplaire qui a servi à spawner,
    /// seulement des clones bon marché : les champs de `SessionController`
    /// sont eux-mêmes des poignées partagées (`PgStore`, `WorkerClient`,
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
    ///     store: PgStore::connect("postgres://...").await?,
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
            .build();

        let controller = Self {
            store: args.store,
            worker: args.worker,
            sessions,
            graphs: args.graphs,
            queue: queue_tx,
        };

        tokio::spawn(controller.clone().run(queue_rx));

        controller
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
    async fn run(mut self, mut queue: mpsc::UnboundedReceiver<Message>) {
        loop {
            select! {
                Some(msg) = queue.recv() => self.process_message(msg).await
            }
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
    async fn process_message(&mut self, msg: Message) {
        use Message::{ChildFrameTerminated, FrameTerminated, FrameReady, FrameRunJobStateUpdate, FrameRunTerminated};

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
            ChildFrameTerminated { session_id, parent_id, child_id } => todo!(),
        }
    }

    async fn on_child_frame_terminated(&self, session_id: SessionId, parent_id: FrameId, child_id: FrameId) {
        let Ok(handler) = self.get(&session_id).await else { return };
        handler.on_child_frame_terminated(&parent_id, &child_id).await
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
        handler.on_frame_created(&frame_id).await
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
        session.on_frame_run_terminated(&frame_id).await
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
        session.on_frame_run_update(&frame_id, job_state).await
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
        session.on_frame_ready(&frame_id).await
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
        handler.on_frame_terminated(&frame_id).await;  
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
    async fn get(&self, id: &SessionId) -> Result<Arc<SessionHandler>, SessionError> {
        let result = self
            .sessions
            .try_get_with(*id, SessionHandler::new(*id, self.clone()))
            .map_err(|err| SessionError::StorageError(err))
            .await?;

        Ok(result)
    }


    fn emit(&self, msg: Message) {
        let _ =self.queue.send(msg);
    }
}

pub struct SessionHandler {
    store: PgStore,
    controller: SessionController,
    id: SessionId,
    worker: WorkerClient,
    graphs: Graphs,
    session: Arc<Mutex<Session>>,
    frames: FrameTree,
    snapshots: Snapshots,
}

impl SessionHandler {
    pub async fn new(id: SessionId, controller: SessionController) -> Result<Arc<Self>, sqlx::Error> {
        let session = controller.store.get_session(&id).await.map(|session| Arc::new(Mutex::new(session)))?;
        Ok(Arc::new(Self {
            store: controller.store.clone(),
            worker: controller.worker.clone(),
            graphs: controller.graphs.clone(),
            frames: FrameTree::new(controller.store.clone(), controller.graphs.clone(), id, session.clone()),
            snapshots: Snapshots::new(controller.store.clone(), id),
            controller,
            id,
            session
        }))
    }
}

impl SessionHandler {
    pub async fn mark_as_waiting_children(&self, frame_id: &FrameId) {
        self.frames.set_status(frame_id, FrameStatus::WaitingChildren).await;
    }

    pub async fn mark_frame_run_completed(&self, frame_id: &FrameId, response: FrameResponse) {
        use Message::FrameRunTerminated;
        self.frames.set_status(frame_id, FrameStatus::RunCompleted(response)).await;
        self.controller.emit(FrameRunTerminated { session_id: self.id, frame_id: *frame_id });       
    }

    pub async fn mark_frame_run_failed(&self, frame_id: &FrameId, error: impl ToString) {
        use Message::FrameRunTerminated;
        self.frames.set_status(frame_id, FrameStatus::RunFailed(error.to_string())).await;
        self.controller.emit(FrameRunTerminated { session_id: self.id, frame_id: *frame_id });      
    }

    pub async fn mark_ready_frame(&self, frame_id: &FrameId) {
        use Message::FrameReady;
        self.frames.set_status(frame_id, FrameStatus::Ready).await;
        self.controller.emit(FrameReady { session_id: self.id, frame_id: *frame_id });
    }
    
    pub async fn mark_failed_frame(&self, err: impl ToString, frame_id: &FrameId) {
        use Message::FrameTerminated;
        self.frames.set_status(frame_id, FrameStatus::Failed(err.to_string())).await;
        self.controller.emit(FrameTerminated { session_id: self.id, frame_id: *frame_id });
    }

    pub async fn mark_completed_frame(&self, frame_id: &FrameId) {
        use Message::{FrameTerminated, ChildFrameTerminated};
        self.frames.set_status(frame_id, FrameStatus::Completed).await;
        self.controller.emit(FrameTerminated { session_id: self.id, frame_id: *frame_id });

        if let Some(parent_id) = self.frames.parent_of(frame_id).await {
            self.controller.emit(ChildFrameTerminated { session_id: self.id, parent_id, child_id: *frame_id });
        }
    }
}

impl SessionHandler {
    async fn run_frame(&self, frame_id:& FrameId) {
        let args = RunFrameArgs::builder().session_id(self.id).frame_id(*frame_id).build();
       
        match self.worker.spawn::<RunFrame>(args, None).await  {
            Ok(job_handle) => {

                let controller = self.controller.clone();
                let session_id = self.id;
                let frame_id = *frame_id;

                tokio::spawn(async move {
                    use Message::FrameRunJobStateUpdate;
                    let mut stream = job_handle.stream().boxed();
                    while let Some(Ok(job_state)) = stream.next().await {
                        let _ = controller.emit(FrameRunJobStateUpdate {
                            session_id,
                            frame_id,
                            job_state
                        });
                    }
                });
            },
            Err(_) => todo!(),
        }
    }

    async fn set_root(&self, args: NewFrameNodeArgs, initial_channels: HashMap<ChannelName, Value>) -> FrameId {
        let frame_id = self.frames.set_root(args).await;
        let frame = self.frames.get(&frame_id).await;
        let mut frame = frame.lock();
        frame.inherited_channels.extend(initial_channels.into_iter());    
        frame_id
    }

    async fn inherit_from_parent(&self, frame_id: &FrameId) {
        let Some(parent_id) = self.frames.parent_of(frame_id).await else { return };
        let parent_snapshot = self.snapshots.latest(&parent_id).await.unwrap();
        let generation = parent_snapshot.lock().superstep;

        let spec = self.frames.common_spec_of(&frame_id).await;
        
        let inherited = spec.inherited_channels
            .into_iter()
            .flat_map(|ch_name| parent_snapshot.lock().channels.get(&ch_name).cloned().map(|ch| (ch_name, ch)));

        let frame = self.frames.get(&frame_id).await;
        let mut frame = frame.lock();
        frame.inherited_channels.extend(inherited);
        frame.superstep = generation;
    }

    async fn inherit_from_prev_sibling(&self, frame_id: &FrameId) {
        let Some(sibling) = self.frames.prev_sibling_of(frame_id).await else { return };
        let sibling_snapshot = self.snapshots.latest(&sibling).await.unwrap();
        let generation = sibling_snapshot.lock().superstep;

        let spec = self.frames.common_spec_of(&frame_id).await;
        
        let inherited = spec.inherited_channels
            .into_iter()
            .flat_map(|ch_name| sibling_snapshot.lock().channels.get(&ch_name).cloned().map(|ch| (ch_name, ch)));

        let frame = self.frames.get(&frame_id).await;
        let mut frame = frame.lock();
        frame.inherited_channels.extend(inherited);
        frame.superstep = generation;
    }


    async fn append_frame(&self, parent_id: &FrameId, args: NewFrameNodeArgs) -> FrameId {
        use Message::FrameCreated;

        let frame_id = self.frames.append(parent_id, args).await;
        let policy = self.frames.policy_of(&parent_id).await;

        if policy.parent_policy == ParentPolicy::Sequential {
            // on va reprendre les snapshots du précédent
            if self.frames.iter_children_of(&parent_id).await.next().is_some() {
                self.inherit_from_prev_sibling(&frame_id).await;
            } else {
                self.inherit_from_parent(&frame_id).await;
            }
        } else {
            self.inherit_from_parent(&frame_id).await;
        }
        
        self.controller.emit(FrameCreated { session_id: self.id, frame_id });

        frame_id
    } 
}

impl SessionHandler {
    async fn drain_pending_accumulators(&self, frame_id: &FrameId, relevants: &[FrameId]) -> Result<Vec<ChannelUpdate>, SessionError> {
        let mut per_channel: HashMap<ChannelName, Vec<Value>> = HashMap::new();
        
        for child_id in relevants {
            let child_spec = self.frames.common_spec_of(&child_id).await;
            let child_snapshot = self.snapshots.latest(child_id).await?;
            for exported in &child_spec.exported_channels {
                if let Some(value) = child_snapshot.lock().channels.get(exported).cloned() {
                    per_channel
                        .entry(exported.clone())
                        .or_default()
                        .push(value);
                }
            }
        }

        let updates = per_channel
            .into_iter()
            .flat_map(|(name, values)| {
                values
                    .into_iter()
                    .map(move |value| ChannelUpdate { name: name.clone(), value, contributor: *frame_id }) 
             })
            .collect();

        Ok(updates)
    }

    async fn commit_snapshot(&self, frame_id: &FrameId, expected_superstep: u32, updates: Vec<ChannelUpdate>, join_sources: Vec<SnapshotRef>) -> Result<SnapshotRef, SessionError> {
        // 1. Lecture CAS
        let last = self.snapshots.latest(frame_id).await?;
        if last.lock().superstep != expected_superstep {
            return Err(SessionError::StaleSuperstep { expected: expected_superstep, got: last.lock().superstep});
        }

        // 2. Copie de la carte des canaux
        let mut channels = last.lock().channels.clone();

         // 3. Application des mises à jour
        for update in updates {
            channels.insert(update.name.clone(), update.value);
        }

        // 4. Construction d'un nouveau cliché
        let new_snapshot = Snapshot::new(
            self.id, 
            *frame_id, 
            expected_superstep + 1, 
            channels, 
            join_sources
        );

        let snap_ref = self.snapshots.push(new_snapshot.clone()).await;
        
        Ok(snap_ref)
    }
}

impl SessionHandler {
    async fn append_graph_node(&self, parent_id: &FrameId, node_id: NodeId) {
        let graph_ref = self.frames.spec_ref_of(&parent_id).await.into_graph_ref();
        
        let args = NewFrameNodeArgs::builder()
            .session_id(self.id)
            .frame_policy(FramePolicy::default())
            .spec_ref(FrameSpecRef::Graph(graph_ref.clone()))
            .data(FrameData::GraphNode {
                graph_ref,
                node_id
            })
            .build();

        self.append_frame(&parent_id, args).await;
    }

    async fn append_graph_span(&self, parent_id: &FrameId, graph_ref: &GraphRef, branches: Vec<NodeId>, join: NodeId) {
        let mut policy = FramePolicy::default();
        policy.child_failure_policy = super::frames::ChildFailurePolicy::FailIfAtLeastHasFailed(1);
        policy.parent_policy = super::frames::ParentPolicy::FanIn;

        let args = NewFrameNodeArgs::builder()
            .session_id(self.id)
            .frame_policy(policy)
            .spec_ref(FrameSpecRef::Graph(graph_ref.clone()))
            .data(FrameData::GraphSpan { graph_ref: graph_ref.clone(), join })
            .build();         

        let span_id = self.append_frame(&parent_id, args).await;
        self.mark_as_waiting_children(&span_id).await;

        stream::iter(branches)
            .for_each(|branch| async move {
                self.append_graph_thread(
                    &span_id, 
                    graph_ref, 
                    branch
                ).await;
            });


    }

    async fn append_graph_thread(&self, parent_id: &FrameId, graph_ref: &GraphRef, start: NodeId) {
        let mut policy = FramePolicy::default();
        policy.child_failure_policy = super::frames::ChildFailurePolicy::FailIfAtLeastHasFailed(1);
        policy.parent_policy = super::frames::ParentPolicy::Sequential;

        let args = NewFrameNodeArgs::builder()
            .session_id(self.id)
            .frame_policy(policy)
            .spec_ref(FrameSpecRef::Graph(graph_ref.clone()))
            .data(FrameData::GraphThread { graph_ref: graph_ref.clone() })
            .build();   

        let thread_id = self.append_frame(&parent_id, args).await;
        self.mark_as_waiting_children(&thread_id).await;

        self.append_graph_node(&thread_id, start);
    }

    /// Ajoute un graph auprès du parent.
    async fn append_graph(&self, parent_id: &FrameId, graph_id: &GraphId) {
        let graph_ref = self.graphs.latest(&graph_id).await.unwrap().unwrap();
        let graph_spec = self.graphs.get(&graph_ref).await.unwrap();

        let mut policy = FramePolicy::default();
        policy.child_failure_policy = super::frames::ChildFailurePolicy::FailIfAtLeastHasFailed(1);
        policy.parent_policy = super::frames::ParentPolicy::Sequential;

        let args = NewFrameNodeArgs::builder()
            .session_id(self.id)
            .frame_policy(policy)
            .spec_ref(FrameSpecRef::Graph(graph_ref.clone()))
            .data(FrameData::Graph {
                graph_ref: graph_ref.clone(),
            })
            .build();   

        let frame_id = self.append_frame(&parent_id, args).await;
        self.mark_as_waiting_children(&frame_id);

        self.append_graph_thread(&frame_id, &graph_ref, graph_spec.entry).await;
        
    }
}

impl SessionHandler {
    pub async fn on_frame_created(&self, frame_id: &FrameId) {
        // Si la frame est en attente, on la marque en ready
        // Dans d'autres cas, notamment si on spawn un graph, 
        // on créé un graph frame dont on passe automatiquement le statut en WaitingChildren
        if FrameStatus::Pending == self.frames.status_of(frame_id).await {
            self.mark_ready_frame(frame_id);
        }
    }

    pub async fn on_frame_ready(&self, frame_id: &FrameId) {
        self.run_frame(frame_id).await;
    }

    pub async fn on_frame_run_update(&self, frame_id: &FrameId, state: JobState<FrameResponse>) {
        match state {
            JobState::Completed(response) => self.mark_frame_run_completed(frame_id, response).await,
            JobState::Failed { error } => self.mark_frame_run_failed(frame_id, error).await,
            _ => {}
        }
    }

    pub async fn on_frame_run_terminated(&self, frame_id: &FrameId) {
        use super::protocol::FrameResult::{
            RequestHitl,
            AskExperts,
            RequestToolsCalls,
            ExecuteGraph,
            Continue,
            GoTo,
            Fork,
            Completed,
            Failed
        };

        let FrameStatus::RunCompleted(response) = self.frames.status_of(frame_id).await else { return };

        // On a des mise à jour à push dans les canaux du noeud.
        let updates = response.updates;
        if updates.len() > 0 {
            let generation = self.snapshots.latest(frame_id).await.unwrap().lock().r#ref().superstep;
            self.commit_snapshot(frame_id, generation, updates, vec![]).await;
        }
        
        match response.result {
            RequestHitl(hitl) => {},
            AskExperts(expert_ids) => todo!(),
            RequestToolsCalls(request_tool_calls) => todo!(),
            ExecuteGraph(graph_id) => {
                
            },
            Continue => {
                let Some(parent_id) = self.frames.parent_of(frame_id).await else { return };
                let FrameData::GraphNode { graph_ref, node_id } = self.frames.data_of(frame_id).await else { return };
                let graph_spec = self.graphs.get(&graph_ref).await.unwrap();
                let Some(next) = graph_spec.edges.get(&node_id) else { return };
                self.append_graph_node(&parent_id, next.clone()).await;
                self.mark_completed_frame(frame_id).await;
            },
            GoTo(node_id) => {
                let Some(parent_id) = self.frames.parent_of(frame_id).await else { return };
                self.append_graph_node(&parent_id, node_id).await;
                self.mark_completed_frame(frame_id).await;
            },
            Fork { branches, join } => {
                let Some(parent_id) = self.frames.parent_of(frame_id).await else { return };
                let graph_ref = self.frames.spec_ref_of(&parent_id).await.into_graph_ref();
                self.append_graph_span(&parent_id, &graph_ref, branches, join).await;
                self.mark_completed_frame(frame_id).await;
            },
            Completed => {
                self.mark_completed_frame(frame_id).await;
            }
            Failed(err) => {
                self.mark_failed_frame(err, frame_id).await;
            }
        }
    }  

    pub async fn on_frame_terminated(&self, frame_id: &FrameId) {
        todo!("...")
    }

    pub async fn on_child_frame_terminated(&self, frame_id: &FrameId, child_id: &FrameId) {
        use super::frames::ChildFailurePolicy::{FailIfAtLeastHasFailed, DontFail};
        use crate::session::frames::ParentPolicy::{Sequential, FanIn};
        
        let child_status = self.frames.status_of(child_id).await;
        let parent_policy = self.frames.policy_of(frame_id).await;

        // on traite d'abord des enfants qui ont échoués
        // et de comment le parent doit réagir en fonction
        // de sa politique.
        if let FrameStatus::Failed(_) = child_status {
            match parent_policy.child_failure_policy {
                FailIfAtLeastHasFailed(count) if count_failed_frame_child(&self.frames, frame_id).await >= count => 
                {
                    self.mark_failed_frame(format!("au moins {count} enfants ont échoué"), frame_id);
                    return;
                },

                DontFail | FailIfAtLeastHasFailed(_) => {},
            }
        }
        
        match parent_policy.parent_policy {
            Sequential => {
                // par défaut un sequential sans enfants resume (c'est pas supposé arrivé)
                let Some(last_child) = self.frames.last_child_of(frame_id).await else {
                    self.mark_ready_frame(frame_id);
                    return;
                };

                let child_status = self.frames.status_of(&last_child).await;
                // commit_snapshot
                if let FrameStatus::Completed = child_status {
                    let generation = self.snapshots.latest(frame_id).await.unwrap().lock().r#ref().superstep;
                    let child_snapshot_ref = self.snapshots.latest(frame_id).await.unwrap().lock().r#ref();
                    let updates = self.drain_pending_accumulators(frame_id, &[last_child]).await.unwrap();
                    self.commit_snapshot(
                        frame_id, 
                        generation, 
                        updates, 
                        vec![child_snapshot_ref]
                    ).await;
                }
                self.mark_ready_frame(frame_id).await;
            },
            FanIn if all_children_have_terminated(&self.frames, frame_id).await  => {
                // commmit
                let generation = self.snapshots.latest(frame_id).await.unwrap().lock().r#ref().superstep;
                let relevant_children: Vec<_> = stream::iter(self.frames.iter_children_of(&frame_id).await)
                    .filter(|child_id| {
                        let child_id = *child_id;
                        async move { self.frames.status_of(&child_id).await.has_completed() }
                    })
                    .collect()
                    .await;
                
                let join_sources = stream::iter(relevant_children.iter().copied())
                    .then(|frame_id| async move { self.snapshots.latest(&frame_id).await.unwrap().lock().r#ref() })
                    .collect()
                    .await;

                let updates = self.drain_pending_accumulators(frame_id, &relevant_children).await.unwrap();
                self.commit_snapshot(
                    frame_id, 
                    generation, 
                    updates, 
                    join_sources
                ).await;

                self.mark_ready_frame(frame_id).await;
            }
            FanIn => {}
        }


    }
}


/// Vrai si chaque frame de `iter` (typiquement [`FrameNode::iter_children`])
/// a atteint un statut terminal (voir [`FrameNode::has_terminated`]) —
/// utilisée par [`SessionController::handle_terminated_frame`] pour décider
/// si le parent peut passer à l'agrégation
/// ([`Message::AllChildrenFrameHaveTerminated`]). Fonction libre plutôt que
/// méthode de [`SessionController`] : ne dépend que d'un [`FrameTree`] et
/// d'un itérateur, pas du reste de son état (store, cache, queue).
///
/// # Exemple
///
/// ```ignore
/// if all_have_terminated(&guard.frames, parent.iter_children()) {
///     let _ = self.queue.send(Message::AllChildrenFrameHaveTerminated { session_id, parent_id });
/// }
/// ```
async fn all_have_terminated(tree: &FrameTree, iter: impl Iterator<Item=FrameId>) -> bool {
    stream::iter(iter)
        .all(|id| async move { tree.get(&id).await.lock().has_terminated() })
        .await
}

async fn all_children_have_terminated(tree: &FrameTree, parent_id: &FrameId) -> bool {
    all_have_terminated(tree, tree.iter_children_of(parent_id).await.into_iter()).await
}

async fn count_failed_frame_child(tree: &FrameTree, parent_id: &FrameId) -> usize {
    let childs =    tree.iter_children_of(parent_id)
        .await
        .into_iter();
    
    stream::iter(childs)
        .filter(|child_id| {
            let child_id = *child_id;
            async move { tree.status_of(&child_id).await.has_failed() }
        })
        .count()
        .await
}

enum Message {
    FrameCreated {
        session_id: SessionId,
        frame_id: FrameId
    },
    FrameRunJobStateUpdate {
        session_id: SessionId,
        frame_id: FrameId,
        job_state: JobState<FrameResponse>,
    },
    /// The frame is ready to run
    FrameReady {
        session_id: SessionId,
        frame_id: FrameId,
    },
    FrameRunTerminated {
        session_id: SessionId,
        frame_id: FrameId,
    },
    ChildFrameTerminated {
        session_id: SessionId,
        parent_id: FrameId,
        child_id: FrameId 
    },
    /// A frame has terminated
    FrameTerminated {
        session_id: SessionId,
        frame_id: FrameId
    }
}