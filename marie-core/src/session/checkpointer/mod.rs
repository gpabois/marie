use std::collections::HashMap;

use futures::{StreamExt as _, TryStreamExt as _, stream};
use parking_lot::Mutex;
use serde_json::Value;
use tokio::sync::mpsc;
use typed_builder::TypedBuilder;

use crate::{
    expert::{ExpertAskId, RequestAskExpert}, graph::{GraphId, GraphRef, Graphs, NodeId}, hitl::{Hitl, HitlId, model::Answers, service::SessionHitls}, id::IdGenerator, job::JobState, session::{
        Session, SessionId, SessionStatus, channel::{ChannelName, ChannelUpdate, Reducer}, controller::SessionError, frames::{FrameData, FrameId, FramePolicy, FrameSpecRef, FrameTree, ParentPolicy}, logs::{SessionLogs, SessionsLogs}, protocol::{Branch, FrameResponse, SessionCheckpointEvent}, run_log::RunLogContent, snapshot::{SessionSnapshots, Snapshot, SnapshotRef}, store::SessionStore, worker::{RunFrame, RunFrameArgs}
    }, tools::RequestToolCall, worker::WorkerClient
};

use crate::session::frames::{NewFrameNodeArgs, FrameStatus};

mod factory;

pub use factory::SessionCheckpointerFactory;

#[derive(TypedBuilder)]
pub struct SessionCheckpointerArgs {
    session: Session,
    id: IdGenerator,
    queue: mpsc::UnboundedSender<SessionCheckpointEvent>,
    store: SessionStore,
    graphs: Graphs,
    hitls: SessionHitls,
    snapshots: SessionSnapshots,
    session_logs: SessionLogs,
    frames: FrameTree,
    worker: WorkerClient
}

pub struct SessionCheckpointer {
    store: SessionStore,
    id: IdGenerator,
    queue: mpsc::UnboundedSender<SessionCheckpointEvent>,
    worker: WorkerClient,
    graphs: Graphs,
    session: Mutex<Session>,
    frames: FrameTree,
    snapshots: SessionSnapshots,
    hitls: SessionHitls,
    session_logs: SessionLogs
}

impl SessionCheckpointer {
    pub fn new(args: SessionCheckpointerArgs) -> Self {
        Self {
            store: args.store,
            id: args.id,
            worker: args.worker,
            graphs: args.graphs,
            session: Mutex::new(args.session),
            queue: args.queue,
            frames: args.frames,
            snapshots: args.snapshots,
            hitls: args.hitls,
            session_logs: args.session_logs
        }
    }
}

impl SessionCheckpointer {
    fn emit(&self, event: SessionCheckpointEvent) {
        self.queue.send(event);
    }

    fn session_id(&self) -> SessionId {
        self.session.lock().id
    }
}

impl SessionCheckpointer {
    /// Marque `frame_id` comme attendant ses enfants (voir
    /// [`FrameStatus::WaitingChildren`]) — posé sur un frame qui vient
    /// d'être créé avec des enfants déjà attachés (span/thread/graph, voir
    /// [`Self::append_graph_span`]/[`Self::append_graph_thread`]/
    /// [`Self::append_graph`]), pour qu'il ne soit pas traité comme prêt à
    /// tourner par [`Self::on_frame_created`] avant que sa politique de
    /// parent ne le remette en [`FrameStatus::Ready`] (voir
    /// [`Self::on_child_frame_terminated`]).
    pub async fn mark_as_waiting_children(&self, frame_id: &FrameId) {
        self.frames.set_status(frame_id, FrameStatus::WaitingChildren).await;
    }

    /// Enregistre le résultat d'un run terminé avec succès puis pousse
    /// [`Message::FrameRunTerminated`], pour que
    /// [`Self::on_frame_run_terminated`] interprète la [`FrameResponse`]
    /// qu'il transporte (continuation du graphe, fork, complétion, etc.).
    pub async fn mark_frame_run_completed(&self, frame_id: &FrameId, response: FrameResponse) {
        use SessionCheckpointEvent::FrameRunTerminated;
        self.frames.set_status(frame_id, FrameStatus::RunCompleted(response)).await;
        self.emit(FrameRunTerminated { session_id: self.session_id(), frame_id: *frame_id });
    }

    /// Comme [`Self::mark_frame_run_completed`], pour un run qui a échoué —
    /// `error` est réduit à sa représentation textuelle (voir
    /// [`FrameStatus::RunFailed`]) avant persistance : la session ne
    /// conserve jamais le type d'erreur d'origine du worker.
    pub async fn mark_frame_run_failed(&self, frame_id: &FrameId, error: impl ToString) {
        use SessionCheckpointEvent::FrameRunTerminated;
        self.frames.set_status(frame_id, FrameStatus::RunFailed(error.to_string())).await;
        self.emit(FrameRunTerminated { session_id: self.session_id(), frame_id: *frame_id });
    }

    /// Marque `frame_id` prêt à être exécuté et pousse
    /// [`Message::FrameReady`], pour que [`Self::on_frame_ready`] déclenche
    /// [`Self::run_frame`].
    pub async fn mark_ready_frame(&self, frame_id: &FrameId) {
        use SessionCheckpointEvent::FrameReady;
        self.frames.set_status(frame_id, FrameStatus::Ready).await;
        self.emit(FrameReady { session_id: self.session_id(), frame_id: *frame_id });
    }

    /// Marque `frame_id` en échec terminal et pousse
    /// [`Message::FrameTerminated`] — contrairement à
    /// [`SessionHandler::fail`] (qui bascule la *session* entière en échec
    /// suite à une erreur du contrôleur lui-même), ceci ne concerne que ce
    /// frame : le reste de l'arbre peut continuer selon la politique
    /// d'échec de son parent (voir [`Self::on_child_frame_terminated`]).
    pub async fn mark_failed_frame(&self, err: impl ToString, frame_id: &FrameId) {
        use SessionCheckpointEvent::FrameTerminated;
        self.frames.set_status(frame_id, FrameStatus::Failed(err.to_string())).await;
        self.emit(FrameTerminated { session_id: self.session_id(), frame_id: *frame_id });
    }

    /// Marque `frame_id` complété et pousse [`Message::FrameTerminated`],
    /// en notifiant en plus son parent via [`Message::ChildFrameTerminated`]
    /// s'il en a un — contrairement aux autres `mark_*`, une complétion
    /// doit toujours réveiller le parent pour qu'il réévalue sa politique
    /// d'agrégation, pas seulement clore ce frame.
    pub async fn mark_completed_frame(&self, frame_id: &FrameId) {
        use SessionCheckpointEvent::{FrameTerminated, ChildFrameTerminated};
        self.frames.set_status(frame_id, FrameStatus::Completed).await;
        self.emit(FrameTerminated { session_id: self.session_id(), frame_id: *frame_id });

        if let Some(parent_id) = self.frames.parent_of(frame_id).await {
            self.emit(ChildFrameTerminated { session_id: self.session_id(), parent_id, child_id: *frame_id });
        }
    }

    /// Bascule la session en [`SessionStatus::Failed`] suite à l'échec d'un
    /// gestionnaire `on_*` (voir les délégateurs `on_*` de
    /// [`SessionController`]) — persistée immédiatement via
    /// `store.upsert_session` plutôt que de compter sur une éviction du
    /// cache de sessions, pour que le statut d'échec reste visible même si
    /// la session concernée ne redevient jamais active en mémoire.
    async fn fail(&self, err: SessionError) {
        tracing::error!("session {} en échec: {err}", self.session_id());

        let session = {
            let mut guard = self.session.lock();
            guard.status = SessionStatus::Failed(err.to_string());
            guard.clone()
        };

        if let Err(err) = self.store.upsert_session(session).await {
            tracing::error!("échec de la persistance du statut Failed de la session {}: {err}", self.session_id());
        }
    }
}

impl SessionCheckpointer {
    /// Programme l'exécution de `frame_id` auprès du worker (voir
    /// [`WorkerClient::spawn`]) puis détache un `tokio::spawn` qui relaie
    /// chaque [`JobState`] intermédiaire vers la queue sous forme de
    /// [`Message::FrameRunJobStateUpdate`] — cette tâche tourne
    /// indépendamment du futur retourné ici pour ne pas bloquer
    /// [`SessionController::run`] pendant toute la durée du run (voir la
    /// doc de [`SessionController::on_frame_ready`]).
    async fn run_frame(&self, frame_id: &FrameId) -> Result<(), SessionError> {
        let channels = self.snapshots.latest(frame_id).await?.lock().channels.clone();
        let logs = self.frames.logs_of(frame_id).await;

        let args = RunFrameArgs::builder()
            .session_id(self.session_id())
            .frame_id(*frame_id)
            .channels(channels)
            .logs(logs)
            .data(self.frames.data_of(frame_id).await)
            .build();

        let job_handle = self.worker.spawn::<RunFrame>(args, None).await?;

        let queue = self.queue.clone();
        let session_id = self.session_id();
        let frame_id = *frame_id;

        tokio::spawn(async move {
            use SessionCheckpointEvent::FrameRunJobStateUpdate;
            let mut stream = job_handle.stream().boxed();
            while let Some(Ok(job_state)) = stream.next().await {
                queue.send(FrameRunJobStateUpdate {
                    session_id,
                    frame_id,
                    job_state
                });
            }
        });

        Ok(())
    }

    /// Crée le frame racine de la session et y injecte `initial_channels` —
    /// contrairement à [`Self::append_frame`], pas d'héritage de canaux
    /// depuis un parent ou un sibling précédent, puisqu'il n'y en a pas.
    async fn set_root(&self, args: NewFrameNodeArgs, initial_channels: HashMap<ChannelName, Value>) -> FrameId {
        let frame_id = self.frames.set_root(args).await;
        let frame = self.frames.get(&frame_id).await;
        let mut frame = frame.lock();
        frame.inherited_channels.extend(initial_channels.into_iter());    
        frame_id
    }

    async fn write_to_inherited_channels(&self, frame_id: &FrameId, values: HashMap<ChannelName, Value>) -> Result<(), SessionError> {
        let spec = self.frames.common_spec_of(&frame_id).await;

        let written_channels = spec.inherited_channels
            .into_iter()
            .flat_map(|ch_name| values.get(&ch_name).cloned().map(|ch| (ch_name, ch)));
        
        let frame = self.frames.try_get(&frame_id).await?;
        let mut frame = frame.lock();
        frame.inherited_channels.extend(written_channels);
        Ok(())
    }

    /// Copie dans `frame_id` les canaux `inherited_channels` de son spec
    /// (voir [`CommonSpec::inherited_channels`]) depuis le dernier cliché de
    /// son parent, et aligne son `superstep` sur celui de ce cliché — ne
    /// fait rien si `frame_id` est la racine (pas de parent). Utilisée par
    /// [`Self::append_frame`] pour tout frame dont le parent n'est pas
    /// [`ParentPolicy::Sequential`], ou qui en est le premier enfant (voir
    /// [`Self::inherit_from_prev_sibling`] pour les suivants).
    async fn inherit_from_parent(&self, frame_id: &FrameId) -> Result<(), SessionError> {
        let Some(parent_id) = self.frames.parent_of(frame_id).await else { return Ok(()) };
        let parent_snapshot = self.snapshots.latest(&parent_id).await?;
        let generation = parent_snapshot.lock().superstep;

        let spec = self.frames.common_spec_of(&frame_id).await;

        let inherited = spec.inherited_channels
            .into_iter()
            .flat_map(|ch_name| parent_snapshot.lock().channels.get(&ch_name).cloned().map(|ch| (ch_name, ch)));

        let frame = self.frames.get(&frame_id).await;
        let mut frame = frame.lock();
        frame.inherited_channels.extend(inherited);
        frame.superstep = generation;

        Ok(())
    }

    /// Comme [`Self::inherit_from_parent`], mais depuis le dernier cliché du
    /// sibling précédent plutôt que du parent — c'est ce qui fait qu'un
    /// parent [`ParentPolicy::Sequential`] voit ses canaux progresser d'un
    /// enfant au suivant plutôt que de repartir à chaque fois de l'état du
    /// parent. Ne fait rien si `frame_id` est le premier enfant de son
    /// parent (pas de sibling précédent).
    async fn inherit_from_prev_sibling(&self, frame_id: &FrameId) -> Result<(), SessionError> {
        let Some(sibling) = self.frames.prev_sibling_of(frame_id).await else { return Ok(()) };
        let sibling_snapshot = self.snapshots.latest(&sibling).await?;
        let generation = sibling_snapshot.lock().superstep;

        let spec = self.frames.common_spec_of(&frame_id).await;

        let inherited = spec.inherited_channels
            .into_iter()
            .flat_map(|ch_name| sibling_snapshot.lock().channels.get(&ch_name).cloned().map(|ch| (ch_name, ch)));

        let frame = self.frames.get(&frame_id).await;
        let mut frame = frame.lock();
        frame.inherited_channels.extend(inherited);
        frame.superstep = generation;

        Ok(())
    }


    /// Attache un nouveau frame à `parent_id`, fait hériter ses canaux
    /// (depuis le sibling précédent si le parent est
    /// [`ParentPolicy::Sequential`] et qu'un tel sibling existe, sinon
    /// depuis le parent lui-même — voir
    /// [`Self::inherit_from_prev_sibling`]/[`Self::inherit_from_parent`]),
    /// puis pousse [`Message::FrameCreated`] pour que
    /// [`Self::on_frame_created`] le fasse progresser. Point d'entrée
    /// commun à [`Self::append_graph_node`]/[`Self::append_graph_span`]/
    /// [`Self::append_graph_thread`]/[`Self::append_graph`], qui ne
    /// diffèrent que par la [`FramePolicy`]/[`FrameData`] posée.
    async fn append_frame(&self, parent_id: &FrameId, args: NewFrameNodeArgs) -> Result<FrameId, SessionError> {
        use SessionCheckpointEvent::FrameCreated;

        let frame_id = self.frames.append(parent_id, args).await;
        let policy = self.frames.policy_of(&parent_id).await;

        let spec = self.frames.common_spec_of(&frame_id).await;
        let frame = self.frames.get(&frame_id).await;
        frame.lock().inherited_channels.extend(spec.default_values);

        if policy.parent_policy == ParentPolicy::Sequential {
            // on va reprendre les snapshots du précédent
            if self.frames.iter_children_of(&parent_id).await.next().is_some() {
                self.inherit_from_prev_sibling(&frame_id).await?;
            } else {
                self.inherit_from_parent(&frame_id).await?;
            }
        } else {
            self.inherit_from_parent(&frame_id).await?;
        }

        

        self.emit(FrameCreated { session_id: self.session_id(), frame_id });

        Ok(frame_id)
    }
}

impl SessionCheckpointer {
    /// Collecte, pour chaque canal `exported_channels` (voir
    /// [`CommonSpec::exported_channels`]) déclaré par au moins un frame de
    /// `relevants` **et** présent dans `imported_channels` (voir
    /// [`CommonSpec::imported_channels`]) de `frame_id` — le parent —, la
    /// valeur que ce frame a écrite dans son dernier cliché — une entrée par
    /// contributeur, pas fusionnées entre elles ici : c'est
    /// [`Self::commit_snapshot`], via le [`Reducer`] déclaré pour ce canal,
    /// qui les combine (`Reducer::Append` accumule un tableau,
    /// `Reducer::LastWriteWins` ne garde que la dernière). Un canal exporté
    /// par un enfant mais absent de `imported_channels` du parent est
    /// silencieusement ignoré : c'est le parent, pas l'enfant, qui décide de
    /// ce qu'il accepte de recevoir. Utilisée par
    /// [`Self::on_child_frame_terminated`] juste avant de committer un
    /// nouveau cliché sur le parent, aussi bien pour
    /// [`ParentPolicy::Sequential`] (un seul enfant pertinent) que
    /// [`ParentPolicy::FanIn`] (potentiellement plusieurs).
    async fn drain_pending_accumulators(&self, frame_id: &FrameId, relevants: &[FrameId]) -> Result<Vec<ChannelUpdate>, SessionError> {
        let parent_spec = self.frames.common_spec_of(frame_id).await;
        let mut per_channel: HashMap<ChannelName, Vec<Value>> = HashMap::new();

        for child_id in relevants {
            let child_spec = self.frames.common_spec_of(&child_id).await;
            let child_snapshot = self.snapshots.latest(child_id).await?;
            for exported in &child_spec.exported_channels {
                if !parent_spec.imported_channels.contains(exported) {
                    continue;
                }
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

    /// Résout l'unique entrée de journal de rejeu encore en attente de
    /// `frame_id` (voir [`crate::session::run_log::RunLogs`] — au plus une à
    /// la fois par construction) à partir du/des enfant(s) tout juste
    /// terminé(s) de `relevants`, en lisant directement le canal exporté de
    /// l'enfant concerné — celui-ci porte déjà la valeur correctement réduite
    /// (voir [`Self::commit_snapshot`]) une fois cet enfant lui-même terminé,
    /// donc sans avoir besoin de redescendre jusqu'à ses propres enfants.
    /// Sans effet si aucune entrée n'est en attente, ou si aucun enfant de
    /// `relevants` ne correspond au type de l'entrée en attente. Appelée par
    /// [`Self::on_child_frame_terminated`] juste après
    /// [`Self::commit_snapshot`], sur le même principe que
    /// [`Self::drain_pending_accumulators`] mais indépendante d'elle : un
    /// journal n'est pas versionné par superstep comme les canaux.
    async fn resolve_pending_log(&self, frame_id: &FrameId, relevants: &[FrameId]) -> Result<(), SessionError> {
        let Some(pending) = self.frames.pending_log_of(frame_id).await else { return Ok(()) };

        for child_id in relevants {
            let child_spec_ref = self.frames.spec_ref_of(child_id).await;
            let channels = self.snapshots.latest(child_id).await?.lock().channels.clone();

            let resolved = match (&pending.content, &child_spec_ref) {
                (RunLogContent::HitlLog { .. }, FrameSpecRef::Hitl) =>
                    channels.get(&ChannelName::from("hitl_answer")).cloned(),
                (RunLogContent::AskExpertLog { .. }, FrameSpecRef::ExpertAggregator) =>
                    channels.get(&ChannelName::from("expert_answer")).cloned(),
                (RunLogContent::ToolCallLog { .. }, FrameSpecRef::ToolAggregator) =>
                    channels.get(&ChannelName::from("tool_result")).cloned(),
                (RunLogContent::GraphLog { .. }, FrameSpecRef::Graph(_)) => {
                    let sub_spec = self.frames.common_spec_of(child_id).await;
                    let mut map = serde_json::Map::new();
                    for name in &sub_spec.exported_channels {
                        if let Some(v) = channels.get(name) {
                            map.insert(name.to_string(), v.clone());
                        }
                    }
                    Some(Value::Object(map))
                }
                _ => None,
            };

            if let Some(value) = resolved {
                self.frames.resolve_log(frame_id, pending.index, value).await;
                break;
            }
        }

        Ok(())
    }

    /// Publie un nouveau cliché de `frame_id` à `expected_superstep + 1`, en
    /// partant du dernier cliché connu — protégé par une lecture-avant-
    /// écriture façon CAS ([`SessionError::StaleSuperstep`]) : si le
    /// superstep relu ne correspond plus à `expected_superstep`, un autre
    /// appelant a déjà fait progresser ce frame entretemps, et celui-ci doit
    /// relire avant de réessayer plutôt que d'écraser silencieusement un
    /// cliché plus récent.
    async fn commit_snapshot(&self, frame_id: &FrameId, expected_superstep: u32, updates: Vec<ChannelUpdate>, join_sources: Vec<SnapshotRef>) -> Result<SnapshotRef, SessionError> {
        // 1. Lecture CAS
        let last = self.snapshots.latest(frame_id).await?;
        if last.lock().superstep != expected_superstep {
            return Err(SessionError::StaleSuperstep { expected: expected_superstep, got: last.lock().superstep});
        }

        // 2. Copie de la carte des canaux
        let mut channels = last.lock().channels.clone();

        // 3. Application des mises à jour, groupées par canal puis réduites
        // via le `Reducer` déclaré pour ce canal (voir
        // `CommonSpec::channels`) — plusieurs contributions sur un même canal
        // en un seul commit n'arrivent qu'en `ParentPolicy::FanIn` (voir
        // `drain_pending_accumulators`), mais grouper systématiquement laisse
        // le comportement inchangé pour un commit à une seule contribution
        // (tout réducteur renvoie alors simplement cette contribution).
        let mut per_channel: HashMap<ChannelName, Vec<Value>> = HashMap::new();
        for update in updates {
            per_channel.entry(update.name).or_default().push(update.value);
        }

        if !per_channel.is_empty() {
            let spec = self.frames.common_spec_of(frame_id).await;
            for (name, contributions) in per_channel {
                let reducer = spec.channels.iter()
                    .find(|c| c.name() == &name)
                    .map(|c| c.reducer().clone())
                    .unwrap_or(Reducer::LastWriteWins);
                let current = channels.get(&name).cloned();
                channels.insert(name, reducer.reduce(current, &contributions));
            }
        }

        // 4. Construction d'un nouveau cliché
        let new_snapshot = Snapshot::new(
            self.session_id(),
            *frame_id,
            expected_superstep + 1,
            channels,
            join_sources
        );

        let snap_ref = self.snapshots.push(new_snapshot.clone()).await;
        
        Ok(snap_ref)
    }
}

impl SessionCheckpointer {
    /// Attache à `parent_id` un frame [`FrameData::GraphNode`] pointant sur
    /// `node_id` du même graphe que son parent (voir
    /// [`FrameTree::spec_ref_of`]) — brique de base des trois autres
    /// `append_graph_*`, qui matérialisent chacun une séquence de nœuds ou
    /// un fork/join du graphe sous forme de frames.
    async fn append_graph_node(&self, parent_id: &FrameId, node_id: NodeId) -> Result<(), SessionError> {
        let graph_ref = self.frames.spec_ref_of(&parent_id).await.into_graph_ref();

        let args = NewFrameNodeArgs::builder()
            .session_id(self.session_id())
            .frame_policy(FramePolicy::default())
            .spec_ref(FrameSpecRef::Graph(graph_ref.clone()))
            .data(FrameData::GraphNode {
                graph_ref,
                node_id
            })
            .build();

        self.append_frame(&parent_id, args).await?;

        Ok(())
    }

    /// Matérialise un fork/join (voir
    /// [`super::protocol::FrameResult::Fork`]) : un frame
    /// [`FrameData::GraphSpan`] en [`ParentPolicy::FanIn`] — le join n'a
    /// lieu que quand toutes les branches ont terminé, voir
    /// [`Self::on_child_frame_terminated`] — sous lequel chaque entrée de
    /// `branches` devient un [`Self::append_graph_thread`] indépendant.
    async fn append_graph_span(&self, parent_id: &FrameId, graph_ref: &GraphRef, branches: Vec<Branch>, join: NodeId) -> Result<(), SessionError> {
        let mut policy = FramePolicy::default();
        policy.child_failure_policy = super::frames::ChildFailurePolicy::FailIfAtLeastHasFailed(1);
        policy.parent_policy = super::frames::ParentPolicy::FanIn;

        let args = NewFrameNodeArgs::builder()
            .session_id(self.session_id())
            .frame_policy(policy)
            .spec_ref(FrameSpecRef::Graph(graph_ref.clone()))
            .data(FrameData::GraphSpan { graph_ref: graph_ref.clone(), join })
            .build();

        let span_id = self.append_frame(&parent_id, args).await?;
        self.mark_as_waiting_children(&span_id).await;

        stream::iter(branches)
            .map(Ok)
            .try_for_each(|branch| async move {
                let frame_id = self.append_graph_thread(&span_id, graph_ref, branch.start).await?;
                
                if !branch.overrides.is_empty() {
                    self.write_to_inherited_channels(&frame_id, branch.overrides).await?;
                }

                Ok::<(), SessionError>(())
            })
            .await?;

        Ok(())
    }

    /// Démarre une branche séquentielle du graphe : un frame
    /// [`FrameData::GraphThread`] en [`ParentPolicy::Sequential`] (ses
    /// futurs enfants s'enchaînent l'un après l'autre, voir
    /// [`Self::inherit_from_prev_sibling`]), sous lequel `start` est posé
    /// comme premier [`Self::append_graph_node`].
    async fn append_graph_thread(&self, parent_id: &FrameId, graph_ref: &GraphRef, start: NodeId) -> Result<FrameId, SessionError> {
        let mut policy = FramePolicy::default();
        policy.child_failure_policy = super::frames::ChildFailurePolicy::FailIfAtLeastHasFailed(1);
        policy.parent_policy = super::frames::ParentPolicy::Sequential;

        let args = NewFrameNodeArgs::builder()
            .session_id(self.session_id())
            .frame_policy(policy)
            .spec_ref(FrameSpecRef::Graph(graph_ref.clone()))
            .data(FrameData::GraphThread { graph_ref: graph_ref.clone() })
            .build();

        let thread_id = self.append_frame(&parent_id, args).await?;
        self.mark_as_waiting_children(&thread_id).await;

        self.append_graph_node(&thread_id, start).await?;

        Ok(thread_id)
    }

    /// Ajoute un graph auprès du parent.
    async fn append_graph(&self, parent_id: &FrameId, graph_id: &GraphId) -> Result<(), SessionError> {
        let graph_ref = self.graphs.latest(&graph_id).await?
            .ok_or_else(|| SessionError::GraphNotFound(graph_id.clone()))?;
        let graph_spec = self.graphs.get(&graph_ref).await?;

        let mut policy = FramePolicy::default();
        policy.child_failure_policy = super::frames::ChildFailurePolicy::FailIfAtLeastHasFailed(1);
        policy.parent_policy = super::frames::ParentPolicy::Sequential;

        let args = NewFrameNodeArgs::builder()
            .session_id(self.session_id())
            .frame_policy(policy)
            .spec_ref(FrameSpecRef::Graph(graph_ref.clone()))
            .data(FrameData::Graph {
                graph_ref: graph_ref.clone(),
            })
            .build();

        let frame_id = self.append_frame(&parent_id, args).await?;
        self.mark_as_waiting_children(&frame_id).await;

        self.append_graph_thread(&frame_id, &graph_ref, graph_spec.entry).await?;

        Ok(())
    }
}

impl SessionCheckpointer {
    async fn append_hitl(&self, frame_id: &FrameId, hitl: Hitl) -> Result<FrameId, SessionError> {
        let hitl_id = self.hitls.request(hitl).await?;

        let args = NewFrameNodeArgs::builder()
            .session_id(self.session_id())
            .spec_ref(FrameSpecRef::Hitl)
            .data(FrameData::Hitl(hitl_id))
            .build();

        let hitl_frame_id = self.append_frame(frame_id, args).await?;

        Ok(hitl_frame_id)
    }
}

impl SessionCheckpointer {
    async fn append_experts_askings(&self, callee_id: &FrameId, requests: Vec<RequestAskExpert>)  -> Result<FrameId, SessionError> {
        let agg_id = self.append_expert_aggregator(callee_id).await?;
        
        stream::iter(requests)
            .map(Ok)
            .try_for_each(|request| {
                async move {
                    self.append_expert_asking(&agg_id, request).await?;
                    Ok::<_, SessionError>(())
                }
            })
            .await?;

        Ok(agg_id)
    }

    async fn append_expert_aggregator(&self, callee_id: &FrameId) -> Result<FrameId, SessionError> {
        let mut policy = FramePolicy::default();
        policy.child_failure_policy = super::frames::ChildFailurePolicy::FailIfAtLeastHasFailed(1);
        policy.parent_policy = super::frames::ParentPolicy::FanIn;
        
        let args = NewFrameNodeArgs::builder()
            .session_id(self.session_id())
            .frame_policy(policy)
            .spec_ref(FrameSpecRef::ExpertAggregator)
            .data(FrameData::Void)
            .build();

        let agg_id = self.append_frame(callee_id, args).await?;
        self.mark_as_waiting_children(&agg_id).await;
        Ok(agg_id)
    }

    /// Génère lui-même l'[`ExpertAskId`] du frame enfant — voir la doc de
    /// [`RequestAskExpert`] : le déterminisme du rejeu interdit qu'il soit
    /// fourni par l'appelant (le corps de node/script à l'origine de la
    /// demande, potentiellement rejoué).
    async fn append_expert_asking(&self, parent_id: &FrameId, request: RequestAskExpert) -> Result<FrameId, SessionError> {
        let args = NewFrameNodeArgs::builder()
            .session_id(self.session_id())
            .spec_ref(FrameSpecRef::Expert)
            .data(FrameData::AskExpert { id: ExpertAskId::new(), expert_id: request.expert_id, task: request.task })
            .build();

        let expert_id = self.append_frame(parent_id, args).await?;

        Ok(expert_id)
    }
}

impl SessionCheckpointer {
    async fn append_tool_calls(&self, callee_id: &FrameId, requests: Vec<RequestToolCall>)  -> Result<FrameId, SessionError> {
        let agg_id = self.append_tool_aggregator(callee_id).await?;
        
        stream::iter(requests)
            .map(Ok)
            .try_for_each(|request| {
                async move {
                    self.append_tool_call(&agg_id, request).await?;
                    Ok::<_, SessionError>(())
                }
            })
            .await?;

        Ok(agg_id)
    }

    /// Génère lui-même le [`ToolCallId`] du frame enfant — voir la doc de
    /// [`RequestToolCall`] : le déterminisme du rejeu interdit qu'il soit
    /// fourni par l'appelant (le corps de node/script à l'origine de la
    /// demande, potentiellement rejoué).
    async fn append_tool_call(&self, parent_id: &FrameId, request: RequestToolCall) -> Result<FrameId, SessionError> {
        let args = NewFrameNodeArgs::builder()
            .session_id(self.session_id())
            .spec_ref(FrameSpecRef::ToolCall)
            .data(FrameData::ToolCall { id: self.id.next(), name: request.name, parameters: request.parameters })
            .build();

        let tool_id = self.append_frame(parent_id, args).await?;

        Ok(tool_id)
    }

    async fn append_tool_aggregator(&self, callee_id: &FrameId) -> Result<FrameId, SessionError> {
        let mut policy = FramePolicy::default();
        policy.child_failure_policy = super::frames::ChildFailurePolicy::FailIfAtLeastHasFailed(1);
        policy.parent_policy = super::frames::ParentPolicy::FanIn;
        
        let args = NewFrameNodeArgs::builder()
            .session_id(self.session_id())
            .frame_policy(policy)
            .spec_ref(FrameSpecRef::ToolAggregator)
            .data(FrameData::Void)
            .build();

        let agg_id = self.append_frame(callee_id, args).await?;
        self.mark_as_waiting_children(&agg_id).await;
        Ok(agg_id)
    }
}

impl SessionCheckpointer {
    pub async fn on_hitl_response(&self, hitl_id: HitlId, answers: Answers) -> Result<(), SessionError> {
        let Some(frame_id) = self.frames.frame_id_bound_to_hitl_id(&hitl_id).await? else {
            return Ok(());
        };

        let run_log_index = self.frames.log_index_bound_to_hitl_id(&frame_id, hitl_id).await.unwrap();
        
        self.frames.resolve_log(
            &frame_id, 
            run_log_index, 
            answers
        ).await;

        self.mark_ready_frame(&frame_id).await;

        todo!()
    }
    /// Fait progresser un frame tout juste créé : s'il est encore
    /// [`FrameStatus::Pending`] (le cas courant), le marque
    /// [`FrameStatus::Ready`] — pas si un `append_graph_*` l'a déjà posé en
    /// [`FrameStatus::WaitingChildren`] (voir
    /// [`Self::mark_as_waiting_children`]), auquel cas c'est
    /// [`Self::on_child_frame_terminated`] qui le fera avancer plus tard.
    pub async fn on_frame_created(&self, frame_id: &FrameId) -> Result<(), SessionError> {
        // Si la frame est en attente, on la marque en ready
        // Dans d'autres cas, notamment si on spawn un graph,
        // on créé un graph frame dont on passe automatiquement le statut en WaitingChildren
        if FrameStatus::Pending == self.frames.status_of(frame_id).await {
            self.mark_ready_frame(frame_id).await;
        }

        Ok(())
    }

    /// Déclenche l'exécution d'un frame prêt — simple relais vers
    /// [`Self::run_frame`].
    pub async fn on_frame_ready(&self, frame_id: &FrameId) -> Result<(), SessionError> {
        self.run_frame(frame_id).await
    }

    /// Traduit un [`JobState`] brut renvoyé par le worker en
    /// [`FrameStatus::RunCompleted`]/[`FrameStatus::RunFailed`] (voir
    /// [`Self::mark_frame_run_completed`]/[`Self::mark_frame_run_failed`]) —
    /// les autres variantes de [`JobState`] (mises à jour intermédiaires,
    /// sans équivalent en [`FrameStatus`]) sont ignorées.
    pub async fn on_frame_run_update(&self, frame_id: &FrameId, state: JobState<FrameResponse>) -> Result<(), SessionError> {
        match state {
            JobState::Completed(response) => {
                self.mark_frame_run_completed(frame_id, response).await;
                self.emit(SessionCheckpointEvent::FrameRunTerminated { session_id: self.session_id(), frame_id: *frame_id });
            }
            JobState::Failed { error } => {
                self.mark_frame_run_failed(frame_id, error).await;
                self.emit(SessionCheckpointEvent::FrameRunTerminated { session_id: self.session_id(), frame_id: *frame_id });
            },
            _ => {}
        }

        Ok(())
    }

    /// Interprète le [`FrameResponse`] d'un run tout juste terminé, une fois
    /// son statut passé en [`FrameStatus::RunCompleted`] (voir
    /// [`Self::mark_frame_run_completed`]) : committe d'abord ses mises à
    /// jour de canaux le cas échéant, puis dispatch sur
    /// [`super::protocol::FrameResult`] — `Continue`/`GoTo`/`Fork`
    /// créent la suite du graphe avant de clore ce frame,
    /// `Completed`/`Failed` le closent directement.
    pub async fn on_frame_run_terminated(&self, frame_id: &FrameId) -> Result<(), SessionError> {
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

        let FrameStatus::RunCompleted(response) = self.frames.status_of(frame_id).await else { return Ok(()) };

        // On a des mise à jour à push dans les canaux du noeud.
        let updates = response.updates;
        if updates.len() > 0 {
            let generation = self.snapshots.latest(frame_id).await?.lock().r#ref().superstep;
            self.commit_snapshot(frame_id, generation, updates, vec![]).await?;
        }

        // Nouvelles réservations de journal de rejeu (voir
        // `session::run_log::RunLogs`) — indépendant du cliché ci-dessus, un
        // journal n'est pas versionné par superstep.
        if !response.new_logs.is_empty() {
            self.frames.append_logs(frame_id, response.new_logs).await;
        }

        match response.result {
            RequestHitl(hitl) => {
                let hitl_id = self.hitls.request(hitl).await?;
                self.frames.bind_hitl_to_log(
                    frame_id, 
                    self.frames.logs_of(frame_id).await.len() as u32, 
                    hitl_id
                );
                self.mark_as_waiting_children(frame_id).await
            },
            AskExperts(experts_askings) => {
                self.append_experts_askings(frame_id, experts_askings).await?;
                self.mark_as_waiting_children(frame_id).await
            },
            RequestToolsCalls(requests) => {
                self.append_tool_calls(frame_id, requests).await?;
                self.mark_as_waiting_children(frame_id).await
            },
            ExecuteGraph(graph_id) => {
                self.append_graph(frame_id, &graph_id).await?;
                self.mark_as_waiting_children(frame_id).await
            },
            Continue => {
                let Some(parent_id) = self.frames.parent_of(frame_id).await else { return Ok(()) };
                let FrameData::GraphNode { graph_ref, node_id } = self.frames.data_of(frame_id).await else { return Ok(()) };
                let graph_spec = self.graphs.get(&graph_ref).await?;
                let Some(next) = graph_spec.edges.get(&node_id) else { return Ok(()) };
                self.append_graph_node(&parent_id, next.clone()).await?;
                self.mark_completed_frame(frame_id).await;
            },
            GoTo(node_id) => {
                let Some(parent_id) = self.frames.parent_of(frame_id).await else { return Ok(()) };
                self.append_graph_node(&parent_id, node_id).await?;
                self.mark_completed_frame(frame_id).await;
            },
            Fork { branches, join } => {
                let Some(parent_id) = self.frames.parent_of(frame_id).await else { return Ok(()) };
                let graph_ref = self.frames.spec_ref_of(&parent_id).await.into_graph_ref();
                self.append_graph_span(&parent_id, &graph_ref, branches, join).await?;
                self.mark_completed_frame(frame_id).await;
            },
            Completed => {
                self.mark_completed_frame(frame_id).await;
            }
            Failed(err) => {
                self.mark_failed_frame(err, frame_id).await;
            }
        }

        Ok(())
    }

    /// Propage la terminaison de `frame_id` vers son parent (voir
    /// [`Message::ChildFrameTerminated`]) — ne fait rien de plus ici : c'est
    /// [`Self::on_child_frame_terminated`], côté parent, qui décide si
    /// cette terminaison le fait lui-même progresser.
    pub async fn on_frame_terminated(&self, frame_id: &FrameId) -> Result<(), SessionError> {
        if let Some(parent_id) = self.frames.parent_of(frame_id).await {
            self.emit(SessionCheckpointEvent::ChildFrameTerminated { session_id: self.session_id(), parent_id, child_id: *frame_id });
        }

        Ok(())
    }

    /// Réagit à la terminaison d'un enfant de `frame_id` : d'abord sa
    /// politique d'échec ([`ChildFailurePolicy`]) — si assez d'enfants ont
    /// échoué, `frame_id` échoue à son tour sans attendre les autres —
    /// puis sa politique d'agrégation ([`ParentPolicy::Sequential`]
    /// progresse dès ce seul enfant terminé ; [`ParentPolicy::FanIn`]
    /// attend que tous aient terminé avant de committer un cliché joignant
    /// leurs contributions, voir
    /// [`Self::drain_pending_accumulators`]/[`Self::commit_snapshot`]).
    pub async fn on_child_frame_terminated(&self, frame_id: &FrameId, child_id: &FrameId) -> Result<(), SessionError> {
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
                    self.mark_failed_frame(format!("au moins {count} enfants ont échoué"), frame_id).await;
                    return Ok(());
                },

                DontFail | FailIfAtLeastHasFailed(_) => {},
            }
        }

        match parent_policy.parent_policy {
            Sequential => {
                // par défaut un sequential sans enfants resume (c'est pas supposé arrivé)
                let Some(last_child) = self.frames.last_child_of(frame_id).await else {
                    self.mark_ready_frame(frame_id).await;
                    return Ok(());
                };

                let child_status = self.frames.status_of(&last_child).await;
                // commit_snapshot
                if let FrameStatus::Completed = child_status {
                    let generation = self.snapshots.latest(frame_id).await?.lock().r#ref().superstep;
                    let child_snapshot_ref = self.snapshots.latest(frame_id).await?.lock().r#ref();
                    let updates = self.drain_pending_accumulators(frame_id, &[last_child]).await?;
                    self.commit_snapshot(
                        frame_id,
                        generation,
                        updates,
                        vec![child_snapshot_ref]
                    ).await?;
                    self.resolve_pending_log(frame_id, &[last_child]).await?;
                }
                self.resume_frame(frame_id).await;
            },
            FanIn if all_children_have_terminated(&self.frames, frame_id).await  => {
                // commmit
                let generation = self.snapshots.latest(frame_id).await?.lock().r#ref().superstep;
                let relevant_children: Vec<_> = stream::iter(self.frames.iter_children_of(&frame_id).await)
                    .filter(|child_id| {
                        let child_id = *child_id;
                        async move { self.frames.status_of(&child_id).await.has_completed() }
                    })
                    .collect()
                    .await;

                let mut join_sources = Vec::with_capacity(relevant_children.len());
                for relevant_frame_id in relevant_children.iter().copied() {
                    let r#ref = self.snapshots.latest(&relevant_frame_id).await?.lock().r#ref();
                    join_sources.push(r#ref);
                }

                let updates = self.drain_pending_accumulators(frame_id, &relevant_children).await?;
                self.commit_snapshot(
                    frame_id,
                    generation,
                    updates,
                    join_sources
                ).await?;
                self.resolve_pending_log(frame_id, &relevant_children).await?;

                self.resume_frame(frame_id).await;
            }
            FanIn => {}
        }

        Ok(())
    }

    /// Fait progresser `frame_id` une fois ses enfants pertinents terminés
    /// (voir [`Self::on_child_frame_terminated`]) : le remet en
    /// [`FrameStatus::Ready`] (donc rejoué par [`Self::run_frame`]) s'il
    /// s'agit d'un des trois genres de frame que `run_frame` sait exécuter
    /// (`AskExpert`/`ToolCall`/`GraphNode`) ; sinon (`Void` — agrégateur —,
    /// `Graph`/`GraphThread`/`GraphSpan`, `Hitl`) le complète directement, ce
    /// sont de purs conteneurs structurels sans rien à ré-exécuter, qui
    /// n'ont qu'à notifier leur propre parent (voir
    /// [`Self::mark_completed_frame`]).
    async fn resume_frame(&self, frame_id: &FrameId) {
        let replayable = matches!(
            self.frames.data_of(frame_id).await,
            FrameData::GraphNode { .. } 
            | FrameData::AskExpert { .. } 
            | FrameData::ToolCall { .. }
        );
        
        if replayable {
            self.mark_ready_frame(frame_id).await;
        } else {
            self.mark_completed_frame(frame_id).await;
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

/// Comme [`all_have_terminated`], appliquée directement aux enfants de
/// `parent_id` — utilisée par [`SessionHandler::on_child_frame_terminated`]
/// pour décider si un parent [`ParentPolicy::FanIn`] peut committer son
/// cliché joint.
async fn all_children_have_terminated(tree: &FrameTree, parent_id: &FrameId) -> bool {
    all_have_terminated(tree, tree.iter_children_of(parent_id).await.into_iter()).await
}

/// Nombre d'enfants de `parent_id` en [`FrameStatus::Failed`] — utilisée par
/// [`SessionHandler::on_child_frame_terminated`] pour évaluer
/// [`ChildFailurePolicy::FailIfAtLeastHasFailed`] sans attendre que tous les
/// enfants aient terminé.
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

