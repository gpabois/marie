use std::collections::HashMap;
use std::sync::Arc;

use futures::{SinkExt as _, StreamExt as _, channel::mpsc};
use libp2p::rendezvous::Namespace;
use parking_lot::Mutex;
use serde_json::{Value, json};
use tokio::{select, sync::oneshot};
use tracing::error;
use typed_builder::TypedBuilder;

use crate::{
    agent::{AgentId, context::ContextEntry, frame::AgentFrame, role::Role, status::{AgentResponse, AgentStatus, YieldStatus}}, hitl::{Answer, Question}, layer::Layer, network::{bootstrap::BootstrapClient, worker::client::WorkerClient}, rpc::{RemoteProcedureCall, RpcServer}, session::{
        NS_SESSION, Session, SessionEvent, SessionId, SessionLog, SessionLogId,
        catalog::SessionCatalog,
        rpc::{AppendLog, GetSession, InsertInLog, InsertSession, ListSession, PatchVars, PushGraph, PushHitl, PushOrchestration, QueryVars, RemoveSession, ReportAgentRun, ReportGraphDispatch, ReportGraphRun, ReportToolDispatch, ReportToolExecution, ReportUserInput, UpdateGraphStep, UpdateSession},
        state::{
            StateGraph,
            executable::{OrchestrationStrategy, ResolvedChildTask},
            frame::{GraphFrame, GraphFrameId, GraphOwner, GraphResponse, GraphStackFrame},
            hitl::{HitlFrame, HitlFrameId, HitlFrameStatus},
            orchestration::{ChildRef, OrchestrationFrame, OrchestrationFrameId, Waiter},
        },
        worker::RunAgent,
    }, sink::SinkBoxExt as _,
    tools::{ToolCallId, ToolCallResult},
};
use crate::session::state::worker::RunGraphStep;

#[derive(TypedBuilder)]
pub struct SessionServerArgs {
    rpc_server: RpcServer,
    bootstrap: BootstrapClient,
    /// Utilisé pour resoumettre un job `RunAgent`/`RunGraphStep` quand un
    /// frame en [`YieldStatus::WaitingAgents`]/[`YieldStatus::WaitingGraph`]/
    /// [`YieldStatus::WaitingOrchestration`] se retrouve entièrement
    /// débloqué (voir [`report_agent_run`]/[`report_graph_run`]) —
    /// soumission directe (`WorkerClient::spawn`, sélection décentralisée
    /// via `bootstrap.select_peer`), sans passer par un control plane.
    worker: WorkerClient,
}

/// Frame à resoumettre comme nouveau Job une fois débloqué — soit un
/// [`AgentFrame`] (`RunAgent`), soit un [`GraphFrame`] entier (`RunGraphStep`,
/// même discipline "un pas par Job" que `RunAgent` "un tour par Job").
pub(crate) enum Resumed {
    Agent(AgentFrame),
    Graph(GraphFrame),
}

/// Commandes mutant le [`SessionCatalog`], consommées exclusivement par
/// [`SessionServerActor`] — voir sa doc pour la raison d'être de cette
/// indirection (RPC -> Command -> mutation + évènement) plutôt qu'une
/// mutation directe comme le fait encore [`crate::model::server::ModelServer`].
pub(crate) enum SessionCommand {
    Insert { session: Session, reply: oneshot::Sender<()> },
    Update { session: Session, reply: oneshot::Sender<()> },
    Remove { id: SessionId, reply: oneshot::Sender<()> },
    ReportAgentRun { agent_id: AgentId, response: AgentResponse, reply: oneshot::Sender<Result<(), String>> },
    ReportToolDispatch { agent_id: AgentId, tools_calls: Vec<ToolCallId>, reply: oneshot::Sender<Result<(), String>> },
    ReportToolExecution { agent_id: AgentId, tool_call_id: ToolCallId, result: ToolCallResult, reply: oneshot::Sender<Result<(), String>> },
    AppendLog { session_id: SessionId, line: String, reply: oneshot::Sender<Result<(), String>> },
    InsertInLog { session_id: SessionId, log_id: SessionLogId, text: String, reply: oneshot::Sender<Result<(), String>> },
    PatchVars { session_id: SessionId, path: String, value: Value, reply: oneshot::Sender<Result<(), String>> },
    /// Pousse un nouveau [`GraphFrame`] et fait passer `agent_id` en
    /// [`YieldStatus::WaitingGraph`] — voir [`push_graph`].
    PushGraph { agent_id: AgentId, graph_id: GraphFrameId, graph: StateGraph, reply: oneshot::Sender<Result<(), String>> },
    /// Persiste la progression d'un [`GraphFrame`] après un pas qui n'a ni
    /// conclu ni yieldé (avancée normale, fork, join, entrée en sous-graphe)
    /// — voir [`update_graph_step`].
    UpdateGraphStep { graph_id: GraphFrameId, graph: GraphFrame, reply: oneshot::Sender<Result<(), String>> },
    /// Persiste l'attente d'un enfant `Agent` spawné par un curseur, avant
    /// de soumettre son Job `RunAgent` — voir [`report_graph_dispatch`].
    ReportGraphDispatch { graph_id: GraphFrameId, graph: GraphFrame, spawn_agent: AgentFrame, reply: oneshot::Sender<Result<(), String>> },
    /// Rapporte l'issue d'un `GraphFrame` (conclu ou en échec) — voir
    /// [`report_graph_run`].
    ReportGraphRun { graph_id: GraphFrameId, response: GraphResponse, reply: oneshot::Sender<Result<(), String>> },
    /// Crée une nouvelle [`OrchestrationFrame`] et ses enfants — voir
    /// [`push_orchestration`].
    PushOrchestration {
        orchestration_id: OrchestrationFrameId,
        owner: Waiter,
        owner_graph_update: Option<GraphFrame>,
        strategy: OrchestrationStrategy,
        children: Vec<ResolvedChildTask>,
        reply: oneshot::Sender<Result<(), String>>,
    },
    /// Crée un nouveau [`HitlFrame`] et fait passer son `owner` (un
    /// [`AgentFrame`] ou un curseur de [`GraphFrame`]) en
    /// [`YieldStatus::WaitingHitl`] — voir [`push_hitl`].
    PushHitl {
        hitl_id: HitlFrameId,
        owner: Waiter,
        questions: Vec<Question>,
        owner_graph_update: Option<GraphFrame>,
        reply: oneshot::Sender<Result<(), String>>,
    },
    /// Rapporte une réponse humaine pour le [`HitlFrame`] `hitl_id`, ou —
    /// s'il est `None` — pour l'unique `AgentFrame` de la session en
    /// [`YieldStatus::WaitingHitl`] (input spontané) — voir
    /// [`report_user_input`].
    ReportUserInput {
        session_id: SessionId,
        hitl_id: Option<HitlFrameId>,
        answers: HashMap<String, Answer>,
        reply: oneshot::Sender<Result<HitlFrameId, String>>,
    },
}

pub struct SessionServerActor;

impl SessionServerActor {
    /// Démarre l'acteur : une tâche unique possède la seule instance mutable
    /// du [`SessionCatalog`] et traite en série les [`SessionCommand`]
    /// reçues (mutation + émission de [`SessionEvent`] sur succès), pendant
    /// que les RPC de lecture (`GetSession`/`ListSession`/`QueryVars`)
    /// continuent d'accéder au catalogue directement via l'`Arc<Mutex<_>>`
    /// partagé — inutile de les faire transiter par l'acteur puisqu'elles ne
    /// mutent rien ni n'émettent d'évènement.
    pub fn new(
        layer: impl Layer<Send = SessionEvent, Received = SessionEvent>,
        mut args: SessionServerArgs,
    ) -> SessionServer {
        args.bootstrap.register_to_namespaces([Namespace::from_static(NS_SESSION)]);

        let (tx, rx) = layer.split();
        let mut tx = tx.boxed_sink();
        let _rx = rx.boxed();

        let (event_tx, mut event_rx) = mpsc::unbounded::<SessionEvent>();
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded::<SessionCommand>();

        let catalog: Arc<Mutex<SessionCatalog>> = Arc::new(Mutex::new(SessionCatalog::new()));
        let cat = catalog.clone();
        let evtx = event_tx.clone();
        let worker = args.worker.clone();

        tokio::spawn(async move {
            use SessionCommand::*;
            loop {
                select! {
                    Ok(event_to_send) = event_rx.recv() => {
                        let _ = tx.send(event_to_send).await;
                    }
                    Ok(cmd) = cmd_rx.recv() => {
                        match cmd {
                            Insert { session, reply } => {
                                let id = session.id;
                                cat.lock().insert(session);
                                let _ = evtx.unbounded_send(SessionEvent::Created { id });
                                let _ = reply.send(());
                            }
                            Update { session, reply } => {
                                let id = session.id;
                                cat.lock().insert(session);
                                let _ = evtx.unbounded_send(SessionEvent::Updated { id });
                                let _ = reply.send(());
                            }
                            Remove { id, reply } => {
                                cat.lock().remove(&id.to_string());
                                let _ = evtx.unbounded_send(SessionEvent::Removed { id });
                                let _ = reply.send(());
                            }
                            ReportAgentRun { agent_id, response, reply } => {
                                let session_id = agent_id.session_id();
                                match report_agent_run(&mut cat.lock(), agent_id, response) {
                                    Ok((status, resumed)) => {
                                        let _ = evtx.unbounded_send(SessionEvent::FrameStatusChanged { session_id, agent_id, status });
                                        spawn_resumed(&worker, resumed);
                                        let _ = reply.send(Ok(()));
                                    }
                                    Err(error) => {
                                        let _ = reply.send(Err(error));
                                    }
                                }
                            }
                            ReportToolDispatch { agent_id, tools_calls, reply } => {
                                let session_id = agent_id.session_id();
                                match report_tool_dispatch(&mut cat.lock(), agent_id, tools_calls) {
                                    Ok(status) => {
                                        let _ = evtx.unbounded_send(SessionEvent::FrameStatusChanged { session_id, agent_id, status });
                                        let _ = reply.send(Ok(()));
                                    }
                                    Err(error) => {
                                        let _ = reply.send(Err(error));
                                    }
                                }
                            }
                            ReportToolExecution { agent_id, tool_call_id, result, reply } => {
                                let session_id = agent_id.session_id();
                                match report_tool_execution(&mut cat.lock(), agent_id, tool_call_id, result) {
                                    Ok((status, resumed)) => {
                                        let _ = evtx.unbounded_send(SessionEvent::FrameStatusChanged { session_id, agent_id, status });

                                        if let Some(frame) = resumed {
                                            let _ = evtx.unbounded_send(SessionEvent::FrameStatusChanged {
                                                session_id,
                                                agent_id: frame.id,
                                                status: frame.status.clone(),
                                            });

                                            let worker = worker.clone();
                                            tokio::spawn(async move {
                                                if let Err(err) = worker.spawn::<RunAgent>(frame, None).await {
                                                    error!(%err, "impossible de soumettre le job de reprise pour l'agent débloqué");
                                                }
                                            });
                                        }

                                        let _ = reply.send(Ok(()));
                                    }
                                    Err(error) => {
                                        let _ = reply.send(Err(error));
                                    }
                                }
                            }
                            AppendLog { session_id, line, reply } => {
                                let outcome = append_log(&mut cat.lock(), session_id, line.clone());
                                if let Ok(log_id) = outcome {
                                    let _ = evtx.unbounded_send(SessionEvent::LogAppended { session_id, log_id, text: line });
                                }
                                let _ = reply.send(outcome.map(|_| ()));
                            }
                            InsertInLog { session_id, log_id, text, reply } => {
                                let outcome = insert_in_log(&mut cat.lock(), session_id, log_id, text.clone());
                                if outcome.is_ok() {
                                    let _ = evtx.unbounded_send(SessionEvent::LogAppended { session_id, log_id, text });
                                }
                                let _ = reply.send(outcome);
                            }
                            PatchVars { session_id, path, value, reply } => {
                                let outcome = patch_vars(&mut cat.lock(), session_id, &path, value);
                                if outcome.is_ok() {
                                    let _ = evtx.unbounded_send(SessionEvent::VarsPatched { session_id });
                                }
                                let _ = reply.send(outcome);
                            }
                            PushGraph { agent_id, graph_id, graph, reply } => {
                                let session_id = agent_id.session_id();
                                let outcome = push_graph(&mut cat.lock(), agent_id, graph_id, graph);
                                if let Ok(status) = &outcome {
                                    let _ = evtx.unbounded_send(SessionEvent::FrameStatusChanged { session_id, agent_id, status: status.clone() });
                                    let _ = evtx.unbounded_send(SessionEvent::GraphStatusChanged { session_id, graph_id, status: AgentStatus::Running, current_node: None });
                                }
                                let _ = reply.send(outcome.map(|_| ()));
                            }
                            UpdateGraphStep { graph_id, graph, reply } => {
                                let session_id = graph_id.session_id();
                                let status = graph.status();
                                let current_node = graph.top().graph.ready_cursor().map(|cursor| cursor.current.clone());
                                let outcome = update_graph_step(&mut cat.lock(), graph_id, graph);
                                if outcome.is_ok() {
                                    let _ = evtx.unbounded_send(SessionEvent::GraphStatusChanged { session_id, graph_id, status, current_node });
                                }
                                let _ = reply.send(outcome);
                            }
                            ReportGraphDispatch { graph_id, graph, spawn_agent, reply } => {
                                let session_id = graph_id.session_id();
                                let status = graph.status();
                                let outcome = report_graph_dispatch(&mut cat.lock(), graph_id, graph, spawn_agent.clone());
                                if outcome.is_ok() {
                                    let _ = evtx.unbounded_send(SessionEvent::GraphStatusChanged { session_id, graph_id, status, current_node: None });
                                    let worker = worker.clone();
                                    tokio::spawn(async move {
                                        if let Err(err) = worker.spawn::<RunAgent>(spawn_agent, None).await {
                                            error!(%err, "impossible de soumettre le job de l'agent spawné par un nœud de graphe");
                                        }
                                    });
                                }
                                let _ = reply.send(outcome);
                            }
                            ReportGraphRun { graph_id, response, reply } => {
                                let session_id = graph_id.session_id();
                                match report_graph_run(&mut cat.lock(), graph_id, response) {
                                    Ok((status, resumed)) => {
                                        let _ = evtx.unbounded_send(SessionEvent::GraphStatusChanged { session_id, graph_id, status, current_node: None });
                                        spawn_resumed(&worker, resumed);
                                        let _ = reply.send(Ok(()));
                                    }
                                    Err(error) => {
                                        let _ = reply.send(Err(error));
                                    }
                                }
                            }
                            PushOrchestration { orchestration_id, owner, owner_graph_update, strategy, children, reply } => {
                                let session_id = orchestration_id.session_id();
                                match push_orchestration(&mut cat.lock(), orchestration_id, owner, owner_graph_update, strategy, children) {
                                    Ok((spawned, pending)) => {
                                        let _ = evtx.unbounded_send(SessionEvent::OrchestrationStatusChanged {
                                            session_id,
                                            orchestration_id,
                                            status: AgentStatus::Running,
                                            pending,
                                        });
                                        spawn_resumed(&worker, spawned);
                                        let _ = reply.send(Ok(()));
                                    }
                                    Err(error) => {
                                        let _ = reply.send(Err(error));
                                    }
                                }
                            }
                            PushHitl { hitl_id, owner, questions, owner_graph_update, reply } => {
                                let session_id = hitl_id.session_id();
                                match push_hitl(&mut cat.lock(), hitl_id, owner, questions, owner_graph_update) {
                                    Ok(status) => {
                                        let _ = evtx.unbounded_send(SessionEvent::HitlStatusChanged { session_id, hitl_id, status: HitlFrameStatus::Pending });
                                        match owner {
                                            Waiter::Agent(agent_id) => {
                                                let _ = evtx.unbounded_send(SessionEvent::FrameStatusChanged { session_id, agent_id, status });
                                            }
                                            Waiter::Graph(graph_id) => {
                                                let _ = evtx.unbounded_send(SessionEvent::GraphStatusChanged { session_id, graph_id, status, current_node: None });
                                            }
                                        }
                                        let _ = reply.send(Ok(()));
                                    }
                                    Err(error) => {
                                        let _ = reply.send(Err(error));
                                    }
                                }
                            }
                            ReportUserInput { session_id, hitl_id, answers, reply } => {
                                match report_user_input(&mut cat.lock(), session_id, hitl_id, answers) {
                                    Ok((hitl_id, hitl_status, resumed)) => {
                                        let _ = evtx.unbounded_send(SessionEvent::HitlStatusChanged { session_id, hitl_id, status: hitl_status });

                                        if let Some(resumed) = resumed {
                                            match &resumed {
                                                Resumed::Agent(frame) => {
                                                    let _ = evtx.unbounded_send(SessionEvent::FrameStatusChanged { session_id, agent_id: frame.id, status: frame.status.clone() });
                                                }
                                                Resumed::Graph(frame) => {
                                                    let _ = evtx.unbounded_send(SessionEvent::GraphStatusChanged { session_id, graph_id: frame.id, status: frame.status(), current_node: None });
                                                }
                                            }
                                            spawn_resumed(&worker, vec![resumed]);
                                        }

                                        let _ = reply.send(Ok(hitl_id));
                                    }
                                    Err(error) => {
                                        let _ = reply.send(Err(error));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        GetSession(catalog.clone()).register(&mut args.rpc_server);
        ListSession(catalog.clone()).register(&mut args.rpc_server);
        QueryVars(catalog.clone()).register(&mut args.rpc_server);

        InsertSession(cmd_tx.clone()).register(&mut args.rpc_server);
        UpdateSession(cmd_tx.clone()).register(&mut args.rpc_server);
        RemoveSession(cmd_tx.clone()).register(&mut args.rpc_server);
        ReportAgentRun(cmd_tx.clone()).register(&mut args.rpc_server);
        ReportToolDispatch(cmd_tx.clone()).register(&mut args.rpc_server);
        ReportToolExecution(cmd_tx.clone()).register(&mut args.rpc_server);
        AppendLog(cmd_tx.clone()).register(&mut args.rpc_server);
        InsertInLog(cmd_tx.clone()).register(&mut args.rpc_server);
        PatchVars(cmd_tx.clone()).register(&mut args.rpc_server);
        PushGraph(cmd_tx.clone()).register(&mut args.rpc_server);
        UpdateGraphStep(cmd_tx.clone()).register(&mut args.rpc_server);
        ReportGraphDispatch(cmd_tx.clone()).register(&mut args.rpc_server);
        ReportGraphRun(cmd_tx.clone()).register(&mut args.rpc_server);
        PushOrchestration(cmd_tx.clone()).register(&mut args.rpc_server);
        PushHitl(cmd_tx.clone()).register(&mut args.rpc_server);
        ReportUserInput(cmd_tx.clone()).register(&mut args.rpc_server);

        SessionServer { catalog, cmd_tx }
    }
}

/// Soumet un nouveau Job pour chaque [`Resumed`] — `RunAgent` pour un
/// [`AgentFrame`], `RunGraphStep` pour un [`GraphFrame`] entier (même
/// discipline "un pas par Job" que `RunAgent`).
fn spawn_resumed(worker: &WorkerClient, resumed: Vec<Resumed>) {
    for entry in resumed {
        let worker = worker.clone();
        tokio::spawn(async move {
            let result = match entry {
                Resumed::Agent(frame) => worker.spawn::<RunAgent>(frame, None).await.map(|_| ()),
                Resumed::Graph(frame) => worker.spawn::<RunGraphStep>(frame, None).await.map(|_| ()),
            };
            if let Err(err) = result {
                error!(%err, "impossible de soumettre le job de reprise débloqué");
            }
        });
    }
}

#[derive(Clone)]
pub struct SessionServer {
    pub(crate) catalog: Arc<Mutex<SessionCatalog>>,
    pub(crate) cmd_tx: mpsc::UnboundedSender<SessionCommand>,
}

/// Récupère `session_id` dans `catalog`, ou une erreur lisible si elle n'est
/// pas (encore) connue de ce nœud — commun aux opérations ci-dessous, qui
/// mutent une session existante plutôt que d'en créer une (contrairement à
/// [`crate::session::rpc::InsertSession`], leur appelant est censé savoir
/// que la session existe déjà).
pub(crate) fn get_session(catalog: &SessionCatalog, session_id: SessionId) -> Result<Session, String> {
    catalog
        .get(&session_id.to_string())
        .ok_or_else(|| format!("session inconnue : {session_id}"))
}

/// Met à jour le [`AgentFrame`] de `agent_id` au vu de l'issue rapportée par
/// le worker qui vient de terminer son job `RunAgent` (voir
/// [`crate::session::rpc::ReportAgentRun`]) : un succès ajoute la réponse
/// au contexte du frame et le marque terminé, un échec le marque en échec
/// et consigne l'erreur dans sa sortie d'erreur.
///
/// Réveil des attendants en cascade : parcourt ensuite (1) les autres
/// [`AgentFrame`] de la session en [`YieldStatus::WaitingAgents`] (fan-out
/// direct entre agents), (2) les curseurs de [`GraphFrame`] en
/// [`YieldStatus::WaitingAgents`] (nœud [`Executable::Agent`](crate::session::state::executable::Executable::Agent)),
/// et (3) les [`OrchestrationFrame`] dont `agent_id` est un enfant attendu —
/// une orchestration entièrement résolue réveille à son tour son `owner`
/// (voir [`resolve_orchestration_owner`]). Renvoie le statut résultant du
/// frame de `agent_id`, ainsi que tout ce qui est désormais entièrement
/// débloqué (à resoumettre comme nouveau Job — voir [`spawn_resumed`]).
pub(crate) fn report_agent_run(
    catalog: &mut SessionCatalog,
    agent_id: AgentId,
    response: AgentResponse,
) -> Result<(AgentStatus, Vec<Resumed>), String> {
    let mut session = get_session(catalog, agent_id.session_id())?;

    let status = {
        let Some(frame) = session.frames.get_mut(&agent_id) else {
            return Err(format!("frame {agent_id:?} inconnu de la session {}", agent_id.session_id()));
        };

        apply_agent_response(frame, response.clone());
        frame.status.clone()
    };

    let mut resumed = Vec::new();

    for (waiting_id, waiting_frame) in session.frames.iter_mut() {
        if *waiting_id == agent_id {
            continue;
        }

        let AgentStatus::Yielding(YieldStatus::WaitingAgents { agents }) = &mut waiting_frame.status else {
            continue;
        };

        let Some(index) = agents.iter().position(|awaited| *awaited == agent_id) else {
            continue;
        };

        push_child_result_into_context(waiting_frame, agent_id, &response);

        let AgentStatus::Yielding(YieldStatus::WaitingAgents { agents }) = &mut waiting_frame.status else {
            unreachable!("le statut n'a pas pu changer entre le match ci-dessus et ici");
        };
        agents.remove(index);

        if agents.is_empty() {
            waiting_frame.status = AgentStatus::Running;
            resumed.push(Resumed::Agent(waiting_frame.clone()));
        }
    }

    for graph in session.graphs.values_mut() {
        let mut unblocked = false;
        for cursor in graph.top_mut().graph.cursors.iter_mut() {
            let AgentStatus::Yielding(YieldStatus::WaitingAgents { agents }) = &mut cursor.status else {
                continue;
            };
            let Some(index) = agents.iter().position(|awaited| *awaited == agent_id) else {
                continue;
            };

            cursor.last_output = agent_response_to_value(&response);

            let AgentStatus::Yielding(YieldStatus::WaitingAgents { agents }) = &mut cursor.status else {
                unreachable!("le statut n'a pas pu changer entre le match ci-dessus et ici");
            };
            agents.remove(index);

            if agents.is_empty() {
                cursor.status = AgentStatus::Running;
                unblocked = true;
            }
        }
        if unblocked {
            resumed.push(Resumed::Graph(graph.clone()));
        }
    }

    let resolved_orchestrations = resolve_orchestration_child(&mut session, ChildRef::Agent(agent_id), agent_response_to_value(&response));
    for orchestration_id in resolved_orchestrations {
        resolve_orchestration_owner(&mut session, orchestration_id, &mut resumed);
    }

    catalog.insert(session);
    Ok((status, resumed))
}

/// Ajoute au contexte de `frame` le résultat rapporté par l'agent enfant
/// `child_id` — préfixé par son [`AgentId`] (voir son `Display`) pour que le
/// parent, qui peut attendre plusieurs enfants à la fois, sache lequel a
/// répondu quoi. Un échec de l'enfant est injecté au même titre qu'un succès
/// (voir la sémantique "tous les enfants" de [`YieldStatus::WaitingAgents`]) :
/// il compte comme terminé, à charge pour le parent d'en tenir compte à sa
/// reprise.
fn push_child_result_into_context(frame: &mut AgentFrame, child_id: AgentId, response: &AgentResponse) {
    let content = match response {
        AgentResponse::Finished { text } => text.clone().unwrap_or_default(),
        AgentResponse::Failed { error } => format!("erreur : {error}"),
    };

    frame.context.push(ContextEntry { role: Role::Tool, content: format!("[agent {child_id}] {content}") });
}

fn apply_agent_response(frame: &mut AgentFrame, response: AgentResponse) {
    match response {
        AgentResponse::Finished { text } => {
            if let Some(text) = text {
                frame.context.push(ContextEntry { role: Role::Assistant, content: text });
            }
            frame.status = AgentStatus::Finished;
        }
        AgentResponse::Failed { error } => {
            frame.stderr.push_str(&error);
            frame.status = AgentStatus::Failed;
        }
    }
}

fn agent_response_to_value(response: &AgentResponse) -> Value {
    match response {
        AgentResponse::Finished { text } => json!({ "text": text }),
        AgentResponse::Failed { error } => json!({ "error": error }),
    }
}

fn graph_response_to_value(response: &GraphResponse) -> Value {
    match response {
        GraphResponse::Finished { output } => output.clone(),
        GraphResponse::Failed { error } => json!({ "error": error }),
    }
}

/// Retire `child_ref` de la liste `pending` de toute [`OrchestrationFrame`]
/// qui l'attend, y consigne `value` dans `results` — renvoie les
/// orchestrations désormais entièrement résolues (`pending` vide et tous les
/// enfants prévus déjà spawnés), à réveiller via
/// [`resolve_orchestration_owner`].
fn resolve_orchestration_child(session: &mut Session, child_ref: ChildRef, value: Value) -> Vec<OrchestrationFrameId> {
    let mut resolved = Vec::new();

    for orchestration in session.orchestrations.values_mut() {
        let Some(index) = orchestration.pending.iter().position(|awaited| *awaited == child_ref) else {
            continue;
        };

        orchestration.results.insert(child_ref, value.clone());
        orchestration.pending.remove(index);

        if orchestration.pending.is_empty() && orchestration.spawned.len() == orchestration.children.len() {
            orchestration.status = AgentStatus::Finished;
            resolved.push(orchestration.id);
        }
    }

    resolved
}

/// Réveille l'`owner` de l'[`OrchestrationFrame`] `orchestration_id`
/// (désormais entièrement résolue, voir [`resolve_orchestration_child`]) —
/// injecte les résultats agrégés (`Value::Array`, dans l'ordre de
/// [`OrchestrationFrame::children`]) dans son contexte (agent) ou son
/// `last_output` (curseur de graphe), le fait repasser `Running`, et
/// l'ajoute à `resumed`.
fn resolve_orchestration_owner(session: &mut Session, orchestration_id: OrchestrationFrameId, resumed: &mut Vec<Resumed>) {
    let Some(orchestration) = session.orchestrations.get(&orchestration_id).cloned() else {
        return;
    };

    let aggregated = Value::Array(orchestration.children.iter().filter_map(|child| orchestration.results.get(child).cloned()).collect());

    match orchestration.owner {
        Waiter::Agent(owner_id) => {
            if let Some(frame) = session.frames.get_mut(&owner_id)
                && matches!(&frame.status, AgentStatus::Yielding(YieldStatus::WaitingOrchestration { orchestration: o }) if *o == orchestration_id)
            {
                frame.context.push(ContextEntry { role: Role::Tool, content: format!("[orchestration {orchestration_id}] {aggregated}") });
                frame.status = AgentStatus::Running;
                resumed.push(Resumed::Agent(frame.clone()));
            }
        }
        Waiter::Graph(owner_graph_id) => {
            if let Some(graph) = session.graphs.get_mut(&owner_graph_id) {
                let mut unblocked = false;
                for cursor in graph.top_mut().graph.cursors.iter_mut() {
                    if matches!(&cursor.status, AgentStatus::Yielding(YieldStatus::WaitingOrchestration { orchestration: o }) if *o == orchestration_id) {
                        cursor.last_output = aggregated.clone();
                        cursor.status = AgentStatus::Running;
                        unblocked = true;
                    }
                }
                if unblocked {
                    resumed.push(Resumed::Graph(graph.clone()));
                }
            }
        }
    }
}

/// Fait passer le frame de `agent_id` en [`AgentStatus::Yielding`]`(`[`YieldStatus::WaitingToolReply`]`{ tools_calls, .. })` —
/// appelée par `session::worker::run_turns` *avant* de déclencher les jobs
/// `ToolExecution` correspondants (voir [`crate::session::rpc::ReportToolDispatch`]
/// pour la raison de cet ordre : sans lui, un job très rapide pourrait
/// rapporter son résultat via [`report_tool_execution`] avant même que ce
/// statut n'existe, et rester bloqué indéfiniment dans `tools_calls`).
pub(crate) fn report_tool_dispatch(
    catalog: &mut SessionCatalog,
    agent_id: AgentId,
    tools_calls: Vec<ToolCallId>,
) -> Result<AgentStatus, String> {
    let mut session = get_session(catalog, agent_id.session_id())?;

    let status = {
        let Some(frame) = session.frames.get_mut(&agent_id) else {
            return Err(format!("frame {agent_id:?} inconnu de la session {}", agent_id.session_id()));
        };

        frame.status = AgentStatus::Yielding(YieldStatus::WaitingToolReply { tools_calls, tools_outputs: std::collections::HashMap::new() });
        frame.status.clone()
    };

    catalog.insert(session);
    Ok(status)
}

/// Rapporte le résultat de l'appel de tool `tool_call_id` pour le frame de
/// `agent_id` — voir [`crate::session::rpc::ReportToolExecution`]. Sur le
/// même modèle "tous les tools" que [`report_agent_run`] pour
/// [`YieldStatus::WaitingAgents`], mais ici le frame en attente *est* celui
/// de `agent_id` (contrairement à `WaitingAgents`, pas besoin de parcourir
/// les autres frames de la session pour trouver qui attend). Contrairement
/// à `push_child_result_into_context` (résultats consommés immédiatement),
/// les sorties de tool s'accumulent dans `tools_outputs` et ne sont
/// réinjectées dans le [`Context`](crate::agent::context::Context) qu'une
/// fois `tools_calls` vide — voir la doc de
/// [`YieldStatus::WaitingToolReply`] pour la raison de ce groupement.
///
/// Idempotente : si le frame n'est plus en [`YieldStatus::WaitingToolReply`]
/// ou que `tool_call_id` n'y est plus attendu (résultat déjà appliqué, ex.
/// job `ToolExecution` retenté après TTL), renvoie `Ok` sans rien muter
/// plutôt que d'échouer — même résilience au churn que le reste de ce
/// module, un job qui rejoue son rapport ne doit pas faire échouer/boucler
/// le worker qui l'exécute.
pub(crate) fn report_tool_execution(
    catalog: &mut SessionCatalog,
    agent_id: AgentId,
    tool_call_id: ToolCallId,
    result: ToolCallResult,
) -> Result<(AgentStatus, Option<AgentFrame>), String> {
    let mut session = get_session(catalog, agent_id.session_id())?;

    let Some(frame) = session.frames.get_mut(&agent_id) else {
        return Err(format!("frame {agent_id:?} inconnu de la session {}", agent_id.session_id()));
    };

    let current_status = frame.status.clone();

    let AgentStatus::Yielding(YieldStatus::WaitingToolReply { tools_calls, tools_outputs }) = &mut frame.status else {
        return Ok((current_status, None));
    };

    if !tools_calls.contains(&tool_call_id) {
        return Ok((current_status, None));
    }

    let content = match &result {
        ToolCallResult::Success(text) => text.clone().unwrap_or_default(),
        ToolCallResult::Failed(error) => format!("erreur : {error:?}"),
    };
    tools_outputs.insert(tool_call_id, Value::String(content));
    tools_calls.retain(|id| *id != tool_call_id);

    let (status, resumed) = if tools_calls.is_empty() {
        let outputs = std::mem::take(tools_outputs);
        frame.status = AgentStatus::Running;

        for (id, output) in outputs {
            let content = output.as_str().unwrap_or_default().to_string();
            frame.context.push(ContextEntry { role: Role::Tool, content: format!("[tool {id}] {content}") });
        }

        (frame.status.clone(), Some(frame.clone()))
    } else {
        (frame.status.clone(), None)
    };

    catalog.insert(session);
    Ok((status, resumed))
}

/// Crée toujours une nouvelle entrée de journal (contrairement à
/// [`insert_in_log`], qui accumule sur une entrée existante) — voir
/// [`crate::session::rpc::AppendLog`].
pub(crate) fn append_log(catalog: &mut SessionCatalog, session_id: SessionId, line: String) -> Result<SessionLogId, String> {
    let mut session = get_session(catalog, session_id)?;
    let log_id = SessionLogId::new(crate::id::generate_id());
    session.logs.push(SessionLog { id: log_id, content: line });
    catalog.insert(session);
    Ok(log_id)
}

/// Ajoute `text` à la suite du [`SessionLog`] identifié par `log_id`, ou crée
/// cette entrée si elle n'existe pas encore (premier fragment d'un flux) —
/// voir [`crate::session::rpc::InsertInLog`].
pub(crate) fn insert_in_log(catalog: &mut SessionCatalog, session_id: SessionId, log_id: SessionLogId, text: String) -> Result<(), String> {
    let mut session = get_session(catalog, session_id)?;
    match session.logs.iter_mut().find(|log| log.id == log_id) {
        Some(log) => log.content.push_str(&text),
        None => session.logs.push(SessionLog { id: log_id, content: text }),
    }
    catalog.insert(session);
    Ok(())
}

/// Évalue `path` (JSONPath) contre `Session::vars`, traité comme un unique
/// document JSON (voir [`crate::session::SessionVarsQueryRequest`]).
pub(crate) fn query_vars(catalog: &SessionCatalog, session_id: SessionId, path: &str) -> Result<Vec<Value>, String> {
    let session = get_session(catalog, session_id)?;
    let doc = serde_json::to_value(&session.vars).map_err(|error| error.to_string())?;
    let matches = jsonpath_lib::select(&doc, path).map_err(|error| error.to_string())?;
    Ok(matches.into_iter().cloned().collect())
}

/// Remplace, dans `Session::vars` traité comme un unique document JSON,
/// chaque nœud correspondant à `path` par `value` (voir
/// [`crate::session::SessionVarsPatchRequest`]).
pub(crate) fn patch_vars(catalog: &mut SessionCatalog, session_id: SessionId, path: &str, value: Value) -> Result<(), String> {
    let mut session = get_session(catalog, session_id)?;
    let doc = serde_json::to_value(&session.vars).map_err(|error| error.to_string())?;
    let patched = jsonpath_lib::replace_with(doc, path, &mut |_| Some(value.clone())).map_err(|error| error.to_string())?;
    session.vars = serde_json::from_value(patched).map_err(|error| error.to_string())?;

    catalog.insert(session);
    Ok(())
}

/// Insère un nouveau [`GraphFrame`] (un seul niveau de pile, `return_node: None`)
/// et fait passer `agent_id` en [`YieldStatus::WaitingGraph`] — voir
/// [`crate::session::rpc::PushGraph`], appelée quand un agent pousse un mode
/// `state_graph` (`system/push-mode`, non câblé encore côté dispatch de
/// tool) ou, plus généralement dès aujourd'hui, comme point d'entrée
/// programmatique pour démarrer un graphe sur une session.
pub(crate) fn push_graph(catalog: &mut SessionCatalog, agent_id: AgentId, graph_id: GraphFrameId, graph: StateGraph) -> Result<AgentStatus, String> {
    let mut session = get_session(catalog, agent_id.session_id())?;

    let status = {
        let Some(frame) = session.frames.get_mut(&agent_id) else {
            return Err(format!("frame {agent_id:?} inconnu de la session {}", agent_id.session_id()));
        };

        frame.status = AgentStatus::Yielding(YieldStatus::WaitingGraph { graph: graph_id });
        frame.status.clone()
    };

    session.graphs.insert(
        graph_id,
        GraphFrame { id: graph_id, owner: GraphOwner::Agent(agent_id), stack: vec![GraphStackFrame { graph, return_node: None }], error: String::new() },
    );

    catalog.insert(session);
    Ok(status)
}

/// Remplace l'entrée `session.graphs[graph_id]` par `graph` — cheval de
/// bataille appelé par le driver (`RunGraphStep`) après chaque pas qui n'a ni
/// conclu ni yieldé sur un enfant (avancée normale, fork, join, entrée en
/// sous-graphe) : sur le même modèle que [`Session::insert`]/[`Session::update`]
/// pour un [`AgentFrame`], remplacement complet plutôt que fusion de delta.
pub(crate) fn update_graph_step(catalog: &mut SessionCatalog, graph_id: GraphFrameId, graph: GraphFrame) -> Result<(), String> {
    let mut session = get_session(catalog, graph_id.session_id())?;

    if !session.graphs.contains_key(&graph_id) {
        return Err(format!("graphe {graph_id} inconnu de la session {}", graph_id.session_id()));
    }

    session.graphs.insert(graph_id, graph);
    catalog.insert(session);
    Ok(())
}

/// Persiste, en une seule mutation, l'insertion de l'enfant `spawn_agent`
/// dans `Session::frames` et la nouvelle version de `graph` (dont un curseur
/// vient de passer en `Yielding(WaitingAgents{agents:[spawn_agent.id]})`) —
/// appelée par le driver *avant* de soumettre le Job `RunAgent` de
/// `spawn_agent` (même ordre anti-course que [`report_tool_dispatch`] : sans
/// lui, un enfant particulièrement rapide pourrait rapporter son résultat
/// via [`report_agent_run`] avant même que ce statut d'attente n'existe côté
/// `SessionServer`).
pub(crate) fn report_graph_dispatch(catalog: &mut SessionCatalog, graph_id: GraphFrameId, graph: GraphFrame, spawn_agent: AgentFrame) -> Result<(), String> {
    let mut session = get_session(catalog, graph_id.session_id())?;
    session.graphs.insert(graph_id, graph);
    session.frames.insert(spawn_agent.id, spawn_agent);
    catalog.insert(session);
    Ok(())
}

/// Rapporte l'issue d'un `GraphFrame` (racine conclue ou en échec, voir
/// [`crate::session::state::worker::RunGraphStep`]) — même mécanique de
/// réveil en cascade que [`report_agent_run`] : les [`AgentFrame`] en
/// [`YieldStatus::WaitingGraph`] et les [`OrchestrationFrame`] dont ce graphe
/// est un enfant attendu sont débloqués.
pub(crate) fn report_graph_run(catalog: &mut SessionCatalog, graph_id: GraphFrameId, response: GraphResponse) -> Result<(AgentStatus, Vec<Resumed>), String> {
    let mut session = get_session(catalog, graph_id.session_id())?;

    let status = {
        let Some(graph) = session.graphs.get_mut(&graph_id) else {
            return Err(format!("graphe {graph_id} inconnu de la session {}", graph_id.session_id()));
        };

        if let GraphResponse::Failed { error } = &response {
            graph.error = error.clone();
        }

        graph.status()
    };

    let mut resumed = Vec::new();
    let output = graph_response_to_value(&response);

    for frame in session.frames.values_mut() {
        if matches!(&frame.status, AgentStatus::Yielding(YieldStatus::WaitingGraph { graph }) if *graph == graph_id) {
            frame.context.push(ContextEntry { role: Role::Tool, content: format!("[graph {graph_id}] {output}") });
            frame.status = AgentStatus::Running;
            resumed.push(Resumed::Agent(frame.clone()));
        }
    }

    let resolved_orchestrations = resolve_orchestration_child(&mut session, ChildRef::Graph(graph_id), output);
    for orchestration_id in resolved_orchestrations {
        resolve_orchestration_owner(&mut session, orchestration_id, &mut resumed);
    }

    catalog.insert(session);
    Ok((status, resumed))
}

/// Crée une nouvelle [`OrchestrationFrame`] et insère tous ses enfants
/// résolus (voir [`ResolvedChildTask`]) dans `Session::frames`/`Session::graphs` —
/// un enfant `Agent` devient un [`AgentFrame`], un enfant `Graph` un
/// [`GraphFrame`] frais (`owner: GraphOwner::Orchestration`, un seul niveau
/// de pile). `owner_graph_update`, si fourni, est la version déjà mise à
/// jour (curseur en `Yielding(WaitingOrchestration)`) du [`GraphFrame`]
/// appelant — persistée dans la même mutation pour la même raison
/// anti-course que [`report_graph_dispatch`] (absent quand `owner` est un
/// `AgentFrame`, pas encore câblé côté `system/push-mode`).
///
/// [`OrchestrationStrategy::Parallel`] spawn tous les enfants immédiatement ;
/// [`OrchestrationStrategy::Sequential`] n'en spawn que le premier (voir
/// [`OrchestrationFrame::spawned`]) — les suivants ne sont pas insérés ici et
/// nécessitent une resoumission déclenchée en cascade côté
/// `report_agent_run`/`report_graph_run` (non câblée dans cette passe, voir
/// leur doc).
///
/// Renvoie les enfants effectivement spawnés (à soumettre comme premier Job
/// chacun, voir [`Resumed`]/`spawn_resumed`) et le nombre total d'enfants
/// prévus (spawnés ou non — voir [`SessionEvent::OrchestrationStatusChanged::pending`]).
pub(crate) fn push_orchestration(
    catalog: &mut SessionCatalog,
    orchestration_id: OrchestrationFrameId,
    owner: Waiter,
    owner_graph_update: Option<GraphFrame>,
    strategy: OrchestrationStrategy,
    children: Vec<ResolvedChildTask>,
) -> Result<(Vec<Resumed>, usize), String> {
    let session_id = orchestration_id.session_id();
    let mut session = get_session(catalog, session_id)?;

    let mut refs = Vec::with_capacity(children.len());
    let mut spawned: Vec<Resumed> = Vec::new();

    let spawn_count = match strategy {
        OrchestrationStrategy::Parallel => children.len(),
        OrchestrationStrategy::Sequential => children.len().min(1),
    };

    for (index, child) in children.into_iter().enumerate() {
        match child {
            ResolvedChildTask::Agent(frame) => {
                let child_ref = ChildRef::Agent(frame.id);
                session.frames.insert(frame.id, frame.clone());
                refs.push(child_ref);
                if index < spawn_count {
                    spawned.push(Resumed::Agent(frame));
                }
            }
            ResolvedChildTask::Graph(graph) => {
                let graph_id = GraphFrameId::new(session_id, crate::id::generate_id());
                let child_ref = ChildRef::Graph(graph_id);
                let frame = GraphFrame {
                    id: graph_id,
                    owner: GraphOwner::Orchestration(orchestration_id),
                    stack: vec![GraphStackFrame { graph, return_node: None }],
                    error: String::new(),
                };
                session.graphs.insert(graph_id, frame.clone());
                refs.push(child_ref);
                if index < spawn_count {
                    spawned.push(Resumed::Graph(frame));
                }
            }
        }
    }

    let pending_count = refs.len();
    let spawned_refs: Vec<ChildRef> = refs.iter().take(spawn_count).cloned().collect();

    let frame = OrchestrationFrame {
        id: orchestration_id,
        owner,
        strategy,
        children: refs.clone(),
        spawned: spawned_refs,
        pending: refs,
        results: HashMap::new(),
        status: AgentStatus::Running,
    };
    session.orchestrations.insert(orchestration_id, frame);

    match (owner, owner_graph_update) {
        (Waiter::Graph(owner_graph_id), Some(updated)) => {
            session.graphs.insert(owner_graph_id, updated);
        }
        (Waiter::Agent(agent_id), _) => {
            if let Some(frame) = session.frames.get_mut(&agent_id) {
                frame.status = AgentStatus::Yielding(YieldStatus::WaitingOrchestration { orchestration: orchestration_id });
            }
        }
        _ => {}
    }

    catalog.insert(session);
    Ok((spawned, pending_count))
}

/// Convertit `answers` en la `Value` réinjectée comme `last_output` d'un
/// curseur de [`GraphFrame`] réveillé par [`report_user_input`] — même rôle
/// que [`agent_response_to_value`]/[`graph_response_to_value`] pour les
/// autres satellites.
fn hitl_answers_to_value(answers: &HashMap<String, Answer>) -> Value {
    serde_json::to_value(answers).unwrap_or(Value::Null)
}

/// Crée un nouveau [`HitlFrame`] `hitl_id` et fait passer son `owner` en
/// [`YieldStatus::WaitingHitl`] — appelée par le tool `system/ask-user-input`
/// (`owner: Waiter::Agent`, `owner_graph_update: None`, voir
/// `session::worker::run_turns`) ou par le driver `RunGraphStep` pour un nœud
/// [`crate::session::state::executable::Executable::AskUserInput`]
/// (`owner: Waiter::Graph`, `owner_graph_update: Some(_)`, sur le même modèle
/// anti-course que [`push_orchestration`] : le curseur a déjà été mis en
/// attente dans la copie locale du driver avant cet appel, il ne reste qu'à
/// la persister).
pub(crate) fn push_hitl(
    catalog: &mut SessionCatalog,
    hitl_id: HitlFrameId,
    owner: Waiter,
    questions: Vec<Question>,
    owner_graph_update: Option<GraphFrame>,
) -> Result<AgentStatus, String> {
    let session_id = hitl_id.session_id();
    let mut session = get_session(catalog, session_id)?;

    let status = match owner {
        Waiter::Agent(agent_id) => {
            let Some(frame) = session.frames.get_mut(&agent_id) else {
                return Err(format!("frame {agent_id:?} inconnu de la session {session_id}"));
            };

            frame.status = AgentStatus::Yielding(YieldStatus::WaitingHitl { hitl: hitl_id });
            frame.status.clone()
        }
        Waiter::Graph(graph_id) => {
            let graph = owner_graph_update.ok_or_else(|| format!("graphe {graph_id} : mise à jour du curseur manquante"))?;
            let status = graph.status();
            session.graphs.insert(graph_id, graph);
            status
        }
    };

    session.hitls.insert(hitl_id, HitlFrame { id: hitl_id, owner, questions, status: HitlFrameStatus::Pending });

    catalog.insert(session);
    Ok(status)
}

/// Rapporte une réponse humaine pour le [`HitlFrame`] `hitl_id`, ou — si
/// `None` — pour l'unique `AgentFrame` de `session_id` actuellement
/// [`YieldStatus::WaitingHitl`] (input spontané, voir
/// [`crate::session::rpc::ReportUserInput`]) : ne scanne volontairement que
/// [`Session::frames`], jamais [`Session::graphs`] — un input spontané ne
/// doit jamais résoudre silencieusement un formulaire qu'un curseur de graphe
/// attend précisément (voir la doc de [`YieldStatus::WaitingHitl`]).
///
/// Ne valide pas `answers` contre les questions d'origine (voir
/// [`crate::hitl::validate_answers`], à appeler côté appelant/passerelle
/// avant cet appel) — c'est ce qui permet à un input spontané, dont les clés
/// ne correspondent à aucun schéma connu, de partager cette même mutation
/// qu'une réponse structurée.
///
/// Idempotente : si le [`HitlFrame`] est déjà [`HitlFrameStatus::Answered`],
/// renvoie son état actuel sans rien muter ni réveiller personne à nouveau —
/// même résilience au rejeu que [`report_tool_execution`].
pub(crate) fn report_user_input(
    catalog: &mut SessionCatalog,
    session_id: SessionId,
    hitl_id: Option<HitlFrameId>,
    answers: HashMap<String, Answer>,
) -> Result<(HitlFrameId, HitlFrameStatus, Option<Resumed>), String> {
    let mut session = get_session(catalog, session_id)?;

    let hitl_id = match hitl_id {
        Some(id) => id,
        None => {
            let mut matches = session.frames.values().filter(|frame| matches!(&frame.status, AgentStatus::Yielding(YieldStatus::WaitingHitl { .. })));

            let Some(only) = matches.next() else {
                return Err("aucun agent en attente d'une réponse humaine dans cette session".to_string());
            };
            if matches.next().is_some() {
                return Err("plusieurs agents en attente d'une réponse humaine : précisez hitl_id".to_string());
            }

            let AgentStatus::Yielding(YieldStatus::WaitingHitl { hitl }) = &only.status else {
                unreachable!("le filtre ci-dessus garantit ce statut");
            };
            *hitl
        }
    };

    let Some(hitl) = session.hitls.get_mut(&hitl_id) else {
        return Err(format!("formulaire {hitl_id} inconnu de la session {session_id}"));
    };

    if let HitlFrameStatus::Answered { .. } = &hitl.status {
        return Ok((hitl_id, hitl.status.clone(), None));
    }

    hitl.status = HitlFrameStatus::Answered { answers: answers.clone() };
    let owner = hitl.owner;
    let hitl_status = hitl.status.clone();

    let resumed = match owner {
        Waiter::Agent(agent_id) => {
            let Some(frame) = session.frames.get_mut(&agent_id) else {
                return Err(format!("frame {agent_id:?} inconnu de la session {session_id}"));
            };

            frame.context.push(ContextEntry { role: Role::Tool, content: format!("[hitl {hitl_id}] {}", hitl_answers_to_value(&answers)) });
            frame.status = AgentStatus::Running;
            Resumed::Agent(frame.clone())
        }
        Waiter::Graph(graph_id) => {
            let Some(graph) = session.graphs.get_mut(&graph_id) else {
                return Err(format!("graphe {graph_id} inconnu de la session {session_id}"));
            };

            let Some(cursor) = graph.top_mut().graph.cursors.iter_mut().find(|cursor| matches!(&cursor.status, AgentStatus::Yielding(YieldStatus::WaitingHitl { hitl }) if *hitl == hitl_id))
            else {
                return Err(format!("aucun curseur du graphe {graph_id} n'attend le formulaire {hitl_id}"));
            };

            cursor.last_output = hitl_answers_to_value(&answers);
            cursor.status = AgentStatus::Running;
            Resumed::Graph(graph.clone())
        }
    };

    catalog.insert(session);
    Ok((hitl_id, hitl_status, Some(resumed)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::frame::AgentFrameArgs;
    use crate::session::state::Node;

    fn session_id() -> SessionId {
        SessionId::new(crate::id::generate_id())
    }

    fn agent_frame(session_id: SessionId, model: &str) -> AgentFrame {
        let id = AgentId::new(session_id, crate::id::generate_id());
        AgentFrame::new(AgentFrameArgs::builder().id(id).model(model.into()).context(Default::default()).build())
    }

    fn empty_session(id: SessionId) -> Session {
        Session { id, frames: HashMap::new(), graphs: HashMap::new(), orchestrations: HashMap::new(), hitls: HashMap::new(), logs: Vec::new(), vars: HashMap::new() }
    }

    fn catalog_with(session: Session) -> SessionCatalog {
        let mut catalog = SessionCatalog::new();
        catalog.insert(session);
        catalog
    }

    fn linear_graph() -> StateGraph {
        StateGraph::new(vec![Node::new("start", None)], vec![], "start").unwrap()
    }

    #[test]
    fn test_push_graph_sets_waiting_status_and_creates_graph_frame() {
        let sid = session_id();
        let agent = agent_frame(sid, "gpt");
        let agent_id = agent.id;
        let mut session = empty_session(sid);
        session.frames.insert(agent_id, agent);
        let mut catalog = catalog_with(session);

        let graph_id = GraphFrameId::new(sid, crate::id::generate_id());
        let status = push_graph(&mut catalog, agent_id, graph_id, linear_graph()).unwrap();

        assert!(matches!(&status, AgentStatus::Yielding(YieldStatus::WaitingGraph { graph }) if *graph == graph_id));

        let session = catalog.get(&sid.to_string()).unwrap();
        assert!(session.graphs.contains_key(&graph_id));
        assert_eq!(session.frames[&agent_id].status, status);
    }

    /// Un curseur de `GraphFrame` spawne un enfant `Agent` (nœud `Executable::Agent`) —
    /// quand celui-ci rapporte son résultat via `report_agent_run`, le
    /// curseur (et donc le `GraphFrame`) doit être débloqué.
    #[test]
    fn test_agent_child_completion_resumes_waiting_graph_cursor() {
        let sid = session_id();
        let owner = agent_frame(sid, "gpt");
        let owner_id = owner.id;
        let mut session = empty_session(sid);
        session.frames.insert(owner_id, owner);

        let mut graph = linear_graph();
        let child = agent_frame(sid, "gpt-child");
        let child_id = child.id;
        graph.cursors[0].status = AgentStatus::Yielding(YieldStatus::WaitingAgents { agents: vec![child_id] });

        let graph_id = GraphFrameId::new(sid, crate::id::generate_id());
        session.graphs.insert(
            graph_id,
            GraphFrame { id: graph_id, owner: GraphOwner::Agent(owner_id), stack: vec![GraphStackFrame { graph, return_node: None }], error: String::new() },
        );
        session.frames.insert(child_id, child);

        let mut catalog = catalog_with(session);

        let (status, resumed) = report_agent_run(&mut catalog, child_id, AgentResponse::Finished { text: Some("done".to_string()) }).unwrap();
        assert_eq!(status, AgentStatus::Finished);
        assert_eq!(resumed.len(), 1);
        match &resumed[0] {
            Resumed::Graph(frame) => {
                assert_eq!(frame.id, graph_id);
                assert_eq!(frame.top().graph.cursors[0].status, AgentStatus::Running);
            }
            Resumed::Agent(_) => panic!("attendu Resumed::Graph"),
        }
    }

    #[test]
    fn test_report_graph_run_resumes_waiting_agent() {
        let sid = session_id();
        let mut owner = agent_frame(sid, "gpt");
        let owner_id = owner.id;
        let graph_id = GraphFrameId::new(sid, crate::id::generate_id());
        owner.status = AgentStatus::Yielding(YieldStatus::WaitingGraph { graph: graph_id });

        let mut session = empty_session(sid);
        session.frames.insert(owner_id, owner);
        session.graphs.insert(
            graph_id,
            GraphFrame {
                id: graph_id,
                owner: GraphOwner::Agent(owner_id),
                stack: vec![GraphStackFrame { graph: linear_graph(), return_node: None }],
                error: String::new(),
            },
        );

        let mut catalog = catalog_with(session);

        let (status, resumed) = report_graph_run(&mut catalog, graph_id, GraphResponse::Finished { output: json!("ok") }).unwrap();
        assert_eq!(status, AgentStatus::Running);
        assert_eq!(resumed.len(), 1);
        match &resumed[0] {
            Resumed::Agent(frame) => {
                assert_eq!(frame.id, owner_id);
                assert_eq!(frame.status, AgentStatus::Running);
            }
            Resumed::Graph(_) => panic!("attendu Resumed::Agent"),
        }
    }

    /// Deux enfants `Agent` en parallèle : l'orchestration ne doit conclure
    /// (et réveiller son `owner`) qu'une fois les DEUX rapportés — AND-join,
    /// pas OR (même sémantique que `WaitingAgents`, mémoire projet "Agent
    /// resume semantics: AND not OR").
    #[test]
    fn test_push_orchestration_parallel_and_joins_on_both_children() {
        let sid = session_id();
        let owner = agent_frame(sid, "gpt");
        let owner_id = owner.id;
        let mut session = empty_session(sid);
        session.frames.insert(owner_id, owner);
        let mut catalog = catalog_with(session);

        let child_a = agent_frame(sid, "a");
        let child_b = agent_frame(sid, "b");
        let child_a_id = child_a.id;
        let child_b_id = child_b.id;

        let orchestration_id = OrchestrationFrameId::new(sid, crate::id::generate_id());
        let (spawned, pending) = push_orchestration(
            &mut catalog,
            orchestration_id,
            Waiter::Agent(owner_id),
            None,
            OrchestrationStrategy::Parallel,
            vec![ResolvedChildTask::Agent(child_a), ResolvedChildTask::Agent(child_b)],
        )
        .unwrap();
        assert_eq!(pending, 2);
        assert_eq!(spawned.len(), 2);

        let owner_status = catalog.get(&sid.to_string()).unwrap().frames[&owner_id].status.clone();
        assert!(matches!(owner_status, AgentStatus::Yielding(YieldStatus::WaitingOrchestration { orchestration }) if orchestration == orchestration_id));

        let (_, resumed) = report_agent_run(&mut catalog, child_a_id, AgentResponse::Finished { text: Some("a done".to_string()) }).unwrap();
        assert!(resumed.is_empty(), "l'orchestration ne doit pas conclure avant le second enfant");

        let (_, resumed) = report_agent_run(&mut catalog, child_b_id, AgentResponse::Finished { text: Some("b done".to_string()) }).unwrap();
        assert_eq!(resumed.len(), 1);
        match &resumed[0] {
            Resumed::Agent(frame) => {
                assert_eq!(frame.id, owner_id);
                assert_eq!(frame.status, AgentStatus::Running);
            }
            Resumed::Graph(_) => panic!("attendu Resumed::Agent"),
        }
    }

    /// Un enfant `Graph` d'une orchestration (voir `ChildTask::Graph`) —
    /// `report_graph_run` doit savoir le retrouver dans `pending` au même
    /// titre qu'un enfant `Agent`.
    #[test]
    fn test_push_orchestration_with_graph_child_resolves_via_report_graph_run() {
        let sid = session_id();
        let owner = agent_frame(sid, "gpt");
        let owner_id = owner.id;
        let mut session = empty_session(sid);
        session.frames.insert(owner_id, owner);
        let mut catalog = catalog_with(session);

        let orchestration_id = OrchestrationFrameId::new(sid, crate::id::generate_id());
        let (spawned, pending) =
            push_orchestration(&mut catalog, orchestration_id, Waiter::Agent(owner_id), None, OrchestrationStrategy::Parallel, vec![ResolvedChildTask::Graph(linear_graph())])
                .unwrap();
        assert_eq!(pending, 1);
        assert_eq!(spawned.len(), 1);

        let Resumed::Graph(child_frame) = &spawned[0] else { panic!("attendu Resumed::Graph") };
        let child_graph_id = child_frame.id;
        assert_eq!(child_frame.owner, GraphOwner::Orchestration(orchestration_id));

        let (_, resumed) = report_graph_run(&mut catalog, child_graph_id, GraphResponse::Finished { output: json!("done") }).unwrap();
        assert_eq!(resumed.len(), 1);
        match &resumed[0] {
            Resumed::Agent(frame) => assert_eq!(frame.id, owner_id),
            Resumed::Graph(_) => panic!("attendu Resumed::Agent"),
        }
    }

    fn graph_frame_waiting_on_hitl(sid: SessionId, graph_id: GraphFrameId, hitl_id: HitlFrameId) -> GraphFrame {
        let owner_id = AgentId::new(sid, crate::id::generate_id());
        let mut graph = linear_graph();
        graph.cursors[0].status = AgentStatus::Yielding(YieldStatus::WaitingHitl { hitl: hitl_id });
        GraphFrame { id: graph_id, owner: GraphOwner::Agent(owner_id), stack: vec![GraphStackFrame { graph, return_node: None }], error: String::new() }
    }

    #[test]
    fn test_push_hitl_agent_owner_sets_waiting_status() {
        let sid = session_id();
        let agent = agent_frame(sid, "gpt");
        let agent_id = agent.id;
        let mut session = empty_session(sid);
        session.frames.insert(agent_id, agent);
        let mut catalog = catalog_with(session);

        let hitl_id = HitlFrameId::new(sid, crate::id::generate_id());
        let status = push_hitl(&mut catalog, hitl_id, Waiter::Agent(agent_id), vec![Question::short_text("q", "Q ?")], None).unwrap();

        assert!(matches!(&status, AgentStatus::Yielding(YieldStatus::WaitingHitl { hitl }) if *hitl == hitl_id));

        let session = catalog.get(&sid.to_string()).unwrap();
        assert_eq!(session.frames[&agent_id].status, status);
        assert!(matches!(&session.hitls[&hitl_id].status, HitlFrameStatus::Pending));
    }

    #[test]
    fn test_push_hitl_graph_owner_persists_cursor_status() {
        let sid = session_id();
        let mut catalog = catalog_with(empty_session(sid));

        let graph_id = GraphFrameId::new(sid, crate::id::generate_id());
        let hitl_id = HitlFrameId::new(sid, crate::id::generate_id());
        let graph_frame = graph_frame_waiting_on_hitl(sid, graph_id, hitl_id);

        let status = push_hitl(&mut catalog, hitl_id, Waiter::Graph(graph_id), vec![Question::short_text("q", "Q ?")], Some(graph_frame)).unwrap();

        assert!(matches!(&status, AgentStatus::Yielding(YieldStatus::WaitingHitl { hitl }) if *hitl == hitl_id));

        let session = catalog.get(&sid.to_string()).unwrap();
        assert!(session.graphs.contains_key(&graph_id));
        assert!(matches!(&session.hitls[&hitl_id].status, HitlFrameStatus::Pending));
    }

    #[test]
    fn test_report_user_input_explicit_hitl_id_resumes_agent() {
        let sid = session_id();
        let agent = agent_frame(sid, "gpt");
        let agent_id = agent.id;
        let mut session = empty_session(sid);
        session.frames.insert(agent_id, agent);
        let mut catalog = catalog_with(session);

        let hitl_id = HitlFrameId::new(sid, crate::id::generate_id());
        push_hitl(&mut catalog, hitl_id, Waiter::Agent(agent_id), vec![Question::short_text("q", "Q ?")], None).unwrap();

        let answers = HashMap::from([("q".to_string(), Answer::Single("42".to_string()))]);
        let (resolved_id, status, resumed) = report_user_input(&mut catalog, sid, Some(hitl_id), answers).unwrap();

        assert_eq!(resolved_id, hitl_id);
        assert!(matches!(status, HitlFrameStatus::Answered { .. }));

        match resumed.expect("doit réveiller l'agent") {
            Resumed::Agent(frame) => {
                assert_eq!(frame.id, agent_id);
                assert_eq!(frame.status, AgentStatus::Running);
            }
            Resumed::Graph(_) => panic!("attendu Resumed::Agent"),
        }

        let session = catalog.get(&sid.to_string()).unwrap();
        assert_eq!(session.frames[&agent_id].status, AgentStatus::Running);
    }

    #[test]
    fn test_report_user_input_explicit_hitl_id_resumes_graph_cursor() {
        let sid = session_id();
        let mut catalog = catalog_with(empty_session(sid));

        let graph_id = GraphFrameId::new(sid, crate::id::generate_id());
        let hitl_id = HitlFrameId::new(sid, crate::id::generate_id());
        let graph_frame = graph_frame_waiting_on_hitl(sid, graph_id, hitl_id);
        push_hitl(&mut catalog, hitl_id, Waiter::Graph(graph_id), vec![Question::short_text("q", "Q ?")], Some(graph_frame)).unwrap();

        let answers = HashMap::from([("q".to_string(), Answer::Single("42".to_string()))]);
        let (resolved_id, status, resumed) = report_user_input(&mut catalog, sid, Some(hitl_id), answers).unwrap();

        assert_eq!(resolved_id, hitl_id);
        assert!(matches!(status, HitlFrameStatus::Answered { .. }));

        match resumed.expect("doit réveiller le graphe") {
            Resumed::Graph(frame) => {
                assert_eq!(frame.id, graph_id);
                assert_eq!(frame.top().graph.cursors[0].status, AgentStatus::Running);
            }
            Resumed::Agent(_) => panic!("attendu Resumed::Graph"),
        }
    }

    #[test]
    fn test_report_user_input_is_idempotent_when_already_answered() {
        let sid = session_id();
        let agent = agent_frame(sid, "gpt");
        let agent_id = agent.id;
        let mut session = empty_session(sid);
        session.frames.insert(agent_id, agent);
        let mut catalog = catalog_with(session);

        let hitl_id = HitlFrameId::new(sid, crate::id::generate_id());
        push_hitl(&mut catalog, hitl_id, Waiter::Agent(agent_id), vec![Question::short_text("q", "Q ?")], None).unwrap();

        let answers = HashMap::from([("q".to_string(), Answer::Single("42".to_string()))]);
        report_user_input(&mut catalog, sid, Some(hitl_id), answers.clone()).unwrap();

        let (_, _, resumed) = report_user_input(&mut catalog, sid, Some(hitl_id), answers).unwrap();
        assert!(resumed.is_none(), "un rejeu ne doit pas re-réveiller l'agent");
    }

    #[test]
    fn test_report_user_input_spontaneous_resolves_single_waiting_agent() {
        let sid = session_id();
        let agent = agent_frame(sid, "gpt");
        let agent_id = agent.id;
        let mut session = empty_session(sid);
        session.frames.insert(agent_id, agent);
        let mut catalog = catalog_with(session);

        let hitl_id = HitlFrameId::new(sid, crate::id::generate_id());
        push_hitl(&mut catalog, hitl_id, Waiter::Agent(agent_id), vec![Question::short_text("q", "Q ?")], None).unwrap();

        let answers = HashMap::from([("message".to_string(), Answer::Single("hello".to_string()))]);
        let (resolved_id, _, resumed) = report_user_input(&mut catalog, sid, None, answers).unwrap();

        assert_eq!(resolved_id, hitl_id);
        assert!(resumed.is_some());
    }

    #[test]
    fn test_report_user_input_spontaneous_errors_when_none_waiting() {
        let sid = session_id();
        let mut catalog = catalog_with(empty_session(sid));

        let answers = HashMap::from([("message".to_string(), Answer::Single("hi".to_string()))]);
        assert!(report_user_input(&mut catalog, sid, None, answers).is_err());
    }

    #[test]
    fn test_report_user_input_spontaneous_errors_when_multiple_waiting() {
        let sid = session_id();
        let agent_a = agent_frame(sid, "a");
        let agent_b = agent_frame(sid, "b");
        let agent_a_id = agent_a.id;
        let agent_b_id = agent_b.id;

        let mut session = empty_session(sid);
        session.frames.insert(agent_a_id, agent_a);
        session.frames.insert(agent_b_id, agent_b);
        let mut catalog = catalog_with(session);

        push_hitl(&mut catalog, HitlFrameId::new(sid, crate::id::generate_id()), Waiter::Agent(agent_a_id), vec![], None).unwrap();
        push_hitl(&mut catalog, HitlFrameId::new(sid, crate::id::generate_id()), Waiter::Agent(agent_b_id), vec![], None).unwrap();

        let answers = HashMap::from([("message".to_string(), Answer::Single("hi".to_string()))]);
        assert!(report_user_input(&mut catalog, sid, None, answers).is_err());
    }

    #[test]
    fn test_report_user_input_spontaneous_does_not_match_graph_owned_hitl() {
        let sid = session_id();
        let mut catalog = catalog_with(empty_session(sid));

        let graph_id = GraphFrameId::new(sid, crate::id::generate_id());
        let hitl_id = HitlFrameId::new(sid, crate::id::generate_id());
        let graph_frame = graph_frame_waiting_on_hitl(sid, graph_id, hitl_id);
        push_hitl(&mut catalog, hitl_id, Waiter::Graph(graph_id), vec![], Some(graph_frame)).unwrap();

        let answers = HashMap::from([("message".to_string(), Answer::Single("hi".to_string()))]);
        assert!(
            report_user_input(&mut catalog, sid, None, answers).is_err(),
            "un input spontané ne doit jamais résoudre un HitlFrame porté par un curseur de graphe"
        );
    }
}
