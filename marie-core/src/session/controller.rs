use std::sync::Arc;
use futures::{FutureExt, StreamExt, stream};
use moka::future::{Cache, CacheBuilder};
use parking_lot::Mutex;
use thiserror::Error;
use tokio::{select, sync::mpsc};

use crate::{
    agent::context::ContextEntry, 
    events::EventService, 
    graph::{GraphSpanFrame, GraphThreadFrame}, 
    hitl::HitlFrame, 
    job::JobState, 
    session::{controller::Message::FrameCreated, frames::{Frame, FrameId, FrameStatus, FrameTree},
    protocol::{FrameResponse, GraphCommand}, store::SessionStore, worker::RunFrame}, store::PgStore, worker::WorkerClient
};

use super::{Session, SessionId};

#[derive(Debug, Error)]
pub enum SessionError {
    #[error("erreur lors des opérations de stockage: {0}")]
    StorageError(#[from] Arc<sqlx::Error>)
}
pub struct SessionControllerArgs {
    store: PgStore,
    events: EventService,
    worker: WorkerClient
}

#[derive(Clone)]
pub struct SessionController {
    store: PgStore,
    worker: WorkerClient,
    sessions: Cache<SessionId, Arc<Mutex<Session>>>,
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
        let store = args.store.clone();
        let eviction_listener = move |_, session: Arc<Mutex<Session>>, _| {
            let store = store.clone();
            let session = session.lock().clone();
            async move {
                let _ = store.upsert_session(session).await;
            }.boxed()
        };

        let (queue_tx, queue_rx) = mpsc::unbounded_channel();

        let sessions = CacheBuilder::new(300)
            .async_eviction_listener(eviction_listener)
            .build();

        let controller = Self {
            store: args.store,
            worker: args.worker,
            sessions,
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
        use Message::{AllChildrenFrameHaveTerminated, FrameTerminated, FrameReady, FrameRunJobStateUpdate, FrameRunTerminated};

        match msg {
            FrameCreated { session_id, frame_id } 
                => self.handle_created_frame(session_id, frame_id).await,
            // Une frame a terminé (done ou failed)
            // 1. On vérifie si une frame parent attend que ses enfants aient terminés
            // 2. Si tous les enfants ont terminés, on va envoyer un message-évènement `AllChildrenFrameHaveTerminated`
            FrameTerminated { session_id, frame_id } 
                => self.handle_terminated_frame(session_id, frame_id).await,
            // On aggrège les sorties des enfants et on injecte dans dans le contexte de la frame parent.
            AllChildrenFrameHaveTerminated { session_id, parent_id } 
                => self.handle_all_terminated_children(session_id, parent_id).await,
            // On va déclencher un run
            FrameReady { session_id, frame_id } 
                => self.handle_ready_frame(session_id, frame_id),
            // On a terminé un run
            FrameRunJobStateUpdate { session_id, frame_id, job_state}
                => self.handle_frame_run_update(session_id, frame_id, job_state).await,
            FrameRunTerminated { session_id, frame_id } 
                => self.handle_terminated_frame(session_id, frame_id).await
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
    async fn handle_created_frame(&self, session_id: SessionId, frame_id: FrameId) {
        self.mark_frame_as_ready(session_id, frame_id).await
    }

    /// Marque `frame_id` comme [`FrameStatus::Ready`] et pousse
    /// [`Message::FrameReady`] pour déclencher son run — point commun à
    /// deux origines différentes : un frame qui vient d'être créé (voir
    /// [`Self::handle_created_frame`]) et un frame parent dont tous les
    /// enfants viennent de terminer (voir
    /// [`Self::handle_all_terminated_children`]).
    ///
    /// # Exemple
    ///
    /// ```ignore
    /// self.mark_frame_as_ready(session_id, frame_id).await;
    /// ```
    async fn mark_frame_as_ready(&self, session_id: SessionId, frame_id: FrameId) {
        use Message::FrameReady;

        let Ok(session) = self.get(&session_id).await else { return }; 
        let mut guard = session.lock();
        let frame = guard.frames.get_mut(&frame_id);
        frame.status = FrameStatus::Ready;
        let _ = self.queue.send(FrameReady { session_id, frame_id });
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
    async fn handle_terminated_frame_run(&self, session_id: SessionId, frame_id: FrameId) {
        use FrameStatus::{RunFailed, RunCompleted};
        use Message::FrameTerminated;

        let Ok(session) = self.get(&session_id).await else { return };

        // Le statut est extrait dans ce bloc, qui ne contient aucun `.await` :
        // le `MutexGuard` (`parking_lot`, non `Send`) est ainsi garanti d'être
        // libéré avant tout point de suspension de la fonction, faute de quoi
        // le futur de `run` (spawné sur tokio, voir `SessionController::new`)
        // ne serait plus `Send`, même avec un `drop(guard)` explicite mais
        // atteint seulement sur certains chemins (voir les autres branches).
        let status = {
            let mut guard = session.lock();
            let node = guard.frames.get_mut(&frame_id);
            let status = node.status.clone();

            if let RunFailed(error) = &status {
                node.status = FrameStatus::Failed(error.clone());
            }

            status
        };

        match status {
            RunFailed(_) => {
                let _ = self.queue.send(FrameTerminated { session_id, frame_id });
            }
            RunCompleted(response) => {
                self.handle_frame_run_completion(session_id, response).boxed().await;
            }
            _ => {}
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
    async fn handle_frame_run_update(&self, session_id: SessionId, frame_id: FrameId, job_state: JobState<FrameResponse>) {
        use Message::FrameRunTerminated;

        let Ok(session) = self.get(&session_id).await else { return };

        match job_state {
            JobState::Completed(value) => {
                let mut guard = session.lock();
                let frame = guard.frames.get_mut(&frame_id);
                frame.status = FrameStatus::RunCompleted(value);
                drop(guard);
                let _ = self.queue.send(FrameRunTerminated {session_id, frame_id});
            },
            JobState::Failed { error } => {
                let mut guard = session.lock();
                let frame = guard.frames.get_mut(&frame_id);
                frame.status = FrameStatus::RunFailed(error.to_string());      
                drop(guard);
                let _ = self.queue.send(FrameRunTerminated {session_id, frame_id});            
            },
            // others job updates are not relevant
            _ => {}
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
    fn handle_ready_frame(&self, session_id: SessionId, frame_id: FrameId) {
        let _ = tokio::spawn(self.clone().run_frame(session_id, frame_id));      
    }

    /// Réagit à [`Message::AllChildrenFrameHaveTerminated`] : agrège la
    /// sortie de chaque enfant de `parent_id` (ceux dont
    /// [`FrameNode::output`] n'est pas vide) dans le contexte du parent,
    /// puis le marque prêt à reprendre via [`Self::mark_frame_as_ready`] —
    /// c'est cette agrégation, déclenchée par [`Self::handle_terminated_frame`]
    /// une fois tous les enfants terminés, qui permet à un frame de
    /// reprendre son exécution avec les résultats de ceux qu'il attendait.
    ///
    /// # Exemple
    ///
    /// ```ignore
    /// self.handle_all_terminated_children(session_id, parent_id).await;
    /// ```
    async fn handle_all_terminated_children(&self, session_id: SessionId, parent_id: FrameId) {
        {
            let Ok(session) = self.get(&session_id).await else { return };
            let mut guard = session.lock();
            let outputs = guard.frames.iter_children_of(&parent_id)
                .map(|node| guard.frames.get(&node))
                .filter(|node| !node.output.is_empty())
                .map(|node| node.output.clone())
                .map(|output| ContextEntry::assistant(output))
                .collect::<Vec<_>>();
            let parent = guard.frames.get_mut(&parent_id);
            parent.context.extend(outputs.into_iter());
        }

        self.mark_frame_as_ready(session_id, parent_id).await;
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
    async fn handle_terminated_frame(&self, session_id: SessionId, frame_id: FrameId) {
        use Message::AllChildrenFrameHaveTerminated;

        let Ok(session) = self.get(&session_id).await else { return };
        let guard = session.lock();
        let Some(parent_id) = guard.frames.parent_of(&frame_id) else { return };
        let parent = guard.frames.get(&parent_id);
        
        if all_have_terminated(&guard.frames, parent.iter_children()) {
            drop(guard);
            let _ = self.queue.send(AllChildrenFrameHaveTerminated { session_id, parent_id });
        }       
    }

    /// Ajoute `frame` à l'arbre de la session `session_id` — comme nouvelle
    /// racine si `parent` est `None`, sinon comme dernier enfant de
    /// `parent` (voir [`FrameTree::set_root`]/[`FrameTree::append`]) — puis
    /// notifie le reste de la boucle via [`Message::FrameCreated`] plutôt
    /// que de laisser l'appelant le faire : toute création de frame doit
    /// passer par ce point unique pour que `FrameCreated` soit fiablement
    /// émis à chaque fois, sans risque qu'un appelant l'oublie.
    ///
    /// # Exemple
    ///
    /// ```ignore
    /// // Ajoute un nouveau frame `Hitl` comme enfant du frame qui vient de
    /// // s'épuiser (voir `Self::handle_frame_run_completion`, cas
    /// // `RunExhausted`).
    /// let child_id = controller
    ///     .append_frame(session_id, HitlFrame::text(), Some(parent_frame_id))
    ///     .await;
    /// ```
    async fn append_frame(&self, session_id: SessionId, frame: impl Into<Frame>, parent: Option<FrameId>) -> Option<FrameId> {
        let Ok(session) = self.get(&session_id).await else { return None };
        let mut guard = session.lock();
        
        let frame_id = if let Some(parent) = parent {
            guard.frames.append(&parent, frame)
        } else {
            guard.frames.set_root(frame)
        };

        let _ = self.queue.send(Message::FrameCreated { session_id, frame_id });

        Some(frame_id)
    }

    /// Réagit à la fin d'un run de frame (statut [`FrameStatus::RunCompleted`],
    /// détecté par [`Self::handle_terminated_frame_run`]) selon ce que le
    /// run a produit : budget d'exécution épuisé
    /// ([`FrameResult::RunExhausted`], qui pousse un frame `Hitl` de
    /// relance via [`Self::append_frame`]), des frames enfants à exécuter
    /// en parallèle ([`FrameResult::Yield`], un [`Self::append_frame`] par
    /// frame), une commande de graphe (relayée à
    /// [`Self::handle_graph_command`]) ou une complétion terminale (relayée
    /// à [`Self::report_completed_frame`]).
    ///
    /// # Exemple
    ///
    /// ```ignore
    /// if let FrameStatus::RunCompleted(response) = status {
    ///     self.handle_frame_run_completion(session_id, response).boxed().await;
    /// }
    /// ```
    async fn handle_frame_run_completion(&self, session_id: SessionId, response: FrameResponse) {
        use super::protocol::FrameResult::{RunExhausted, Yield, ExecuteGraphCommand, Completed};

        let Ok(session) = self.get(&session_id).await else { return };

        match response.result {
            RunExhausted => {
                let mut guard = session.lock();

                guard.frames.append(&response.frame_id, HitlFrame::text());
                guard.frames.get_mut(&response.frame_id).status = FrameStatus::WaitingChildren;
                
                drop(guard);
                return;
            },
            Yield(create_frames) => {
                stream::iter(create_frames.into_iter())
                    .for_each(|frame| {
                        let controller = self.clone();
                        let frame_id = response.frame_id;
                        async move {
                            controller.append_frame(session_id, frame, Some(frame_id)).await;
                        }
                    })
                    .await;
                
                session.lock().frames.get_mut(&response.frame_id).status = FrameStatus::WaitingChildren;

                return;
            },
            ExecuteGraphCommand(command) => {
                self.handle_graph_command(session_id, response.frame_id, command).await;
            },
            Completed => {
                self.report_completed_frame(session_id, response.frame_id).await;
            }
        }
    }

    /// Applique une [`GraphCommand`] émise par le run d'un frame `Graph`
    /// (voir [`FrameResult::ExecuteGraphCommand`], relayée depuis
    /// [`Self::handle_frame_run_completion`]) :
    ///
    /// - [`GraphCommand::Fork`] : ajoute un frame `GraphSpan` comme enfant
    ///   du parent de `frame_id`, puis un [`GraphThreadFrame`] par nœud de
    ///   départ comme enfant de ce span — un thread par branche du fork ;
    /// - [`GraphCommand::GoTo`] : ajoute un unique [`GraphThreadFrame`]
    ///   (poursuite linéaire, sans fork) comme enfant du même parent ;
    /// - [`GraphCommand::Finished`] : rien de plus à créer.
    ///
    /// Dans tous les cas, termine par [`Self::report_completed_frame`] sur
    /// `frame_id` lui-même : le frame `Graph`/`GraphThread` qui a émis
    /// cette commande a fini son rôle une fois celle-ci appliquée, que de
    /// nouveaux enfants aient été créés ou non.
    ///
    /// # Exemple
    ///
    /// ```ignore
    /// // Relayé automatiquement depuis `handle_frame_run_completion`,
    /// // jamais appelé directement.
    /// self.handle_graph_command(session_id, frame_id, command).await;
    /// ```
    async fn handle_graph_command(&self, session_id: SessionId, frame_id: FrameId, command: GraphCommand) {
        let Ok(session) = self.get(&session_id).await else { return };
        match command {
            // Emis par un graph node, on va devoir remonter au frame parent pour lui injecter un span
            // pour lui ajouter un span frame.
            GraphCommand::Fork(graph_node_ids) => {
                let maybe_parent =  session.lock().frames.parent_of(&frame_id);

                if let Some(parent) = maybe_parent{
                    let span_id = self.clone().append_frame(session_id, GraphSpanFrame {}, Some(parent)).await.unwrap();
                    stream::iter(graph_node_ids.into_iter())
                        .map(|start| GraphThreadFrame::new(start))
                        .for_each(|frame| {
                            let controller = self.clone();
                            async move {
                                controller.append_frame(session_id, frame, Some(span_id)).await;
                            }
                        })
                        .await;
                }

                self.report_completed_frame(session_id, frame_id).await;
            },
            GraphCommand::GoTo(id) => {
                let maybe_parent =  session.lock().frames.parent_of(&frame_id);
                if let Some(parent) = maybe_parent {
                    self.clone().append_frame(session_id, GraphThreadFrame::new(id), Some(parent)).await;
                }
                self.report_completed_frame(session_id, frame_id).await;
            },
            GraphCommand::Finished => {
                self.report_completed_frame(session_id, frame_id).await;
            }
        }
    }

    /// Marque `frame_id` comme [`FrameStatus::Completed`] et pousse
    /// [`Message::FrameTerminated`] — point de sortie commun à
    /// [`Self::handle_frame_run_completion`] (cas `Completed`) et
    /// [`Self::handle_graph_command`] (ses trois variantes de
    /// [`GraphCommand`]) : dans les deux cas, le frame concerné n'a plus
    /// rien à faire et peut débloquer son parent via
    /// [`Self::handle_terminated_frame`].
    ///
    /// # Exemple
    ///
    /// ```ignore
    /// self.report_completed_frame(session_id, frame_id).await;
    /// ```
    async fn report_completed_frame(&self, session_id: SessionId, frame_id: FrameId) {
        let Ok(session) = self.get(&session_id).await else { return };
        session.lock().frames.get_mut(&frame_id).status = FrameStatus::Completed;
        let _ = self.queue.send(Message::FrameTerminated { session_id, frame_id });
    }

    /// Soumet un run pour `frame_id` au worker (job [`RunFrame`]) puis
    /// relaie chaque mise à jour de son état comme
    /// [`Message::FrameRunJobStateUpdate`] sur `self.queue`, dans une tâche
    /// tokio séparée pour ne pas bloquer la boucle [`Self::run`] pendant
    /// toute la durée du run — c'est cette relance asynchrone, pas
    /// `run_frame` elle-même, qui fait le pont entre le [`JobState`] brut
    /// renvoyé par le worker et le [`FrameStatus`] de haut niveau que
    /// [`Self::process_message`] sait interpréter.
    ///
    /// # Exemple
    ///
    /// ```ignore
    /// // Déclenché par `Message::FrameReady`, jamais appelé directement
    /// // en dehors de `process_message`.
    /// tokio::spawn(controller.clone().run_frame(session_id, frame_id));
    /// ```
    async fn run_frame(self, session_id: SessionId, frame_id: FrameId) {
        let Ok(session) = self.get(&session_id).await else { return };

        // Comme dans `Self::handle_terminated_frame_run` : le `MutexGuard`
        // doit être confiné à un bloc sans `.await`, un `drop(guard)`
        // explicite avant l'await ne suffit pas à garantir que le futur de
        // `run_frame` reste `Send` (voir `tokio::spawn` dans
        // `Self::handle_ready_frame`).
        let frame = {
            let guard = session.lock();
            guard.frames.get(&frame_id).frame.clone()
        };

        match self.worker.spawn::<RunFrame>((frame_id, frame), None).await {
            Ok(job_handle) => {
                let controller = self.clone();
                tokio::spawn(async move {
                    use Message::FrameRunJobStateUpdate;
                    let mut stream = job_handle.stream().boxed();
                    while let Some(Ok(job_state)) = stream.next().await {
                        let _ = controller.queue.send(FrameRunJobStateUpdate {
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
    async fn get(&self, id: &SessionId) -> Result<Arc<Mutex<Session>>, SessionError> {
        let store = self.store.clone();
        let result = self.sessions.try_get_with(*id, async move {
            store.get_session(id).await.map(|session| Arc::new(Mutex::new(session)))
        }).await?;

        Ok(result)
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
fn all_have_terminated(tree: &FrameTree, mut iter: impl Iterator<Item=FrameId>) -> bool {
    iter.all(|id| tree.get(&id).has_terminated())
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
    /// Tous les enfants d'un frame ont terminés.
    AllChildrenFrameHaveTerminated {
        session_id: SessionId,
        parent_id: FrameId
    },
    /// A frame has terminated
    FrameTerminated {
        session_id: SessionId,
        frame_id: FrameId
    }
}