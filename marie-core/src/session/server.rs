use std::collections::HashMap;

use anyhow::anyhow;
use futures::{FutureExt, SinkExt as _, StreamExt as _, TryFutureExt as _, channel::mpsc::{self, UnboundedSender}};
use libp2p::rendezvous::Namespace;
use serde_json::{Value, json};
use tokio::{select, sync::oneshot};
use tracing::error;
use typed_builder::TypedBuilder;

use crate::{
    agent::{AgentId, context::ContextEntry, frame::AgentFrame, role::Role, status::{AgentResponse, AgentStatus, YieldStatus}}, hitl::{Answer, Question}, layer::Layer, network::{bootstrap::BootstrapClient, worker::client::WorkerClient}, rpc::{RemoteProcedureCall, RpcServer}, session::{
        NS_SESSION, Session, SessionEvent, SessionId, SessionLog, SessionLogId, rpc::{AppendLog, GetSession, InsertInLog, InsertSession, ListSession, PatchVars, PushGraph, PushHitl, PushOrchestration, QueryVars, RemoveSession, ReportAgentRun, ReportGraphDispatch, ReportGraphRun, ReportToolDispatch, ReportToolExecution, ReportUserInput, UpdateGraphStep, UpdateSession}, state::{
            StateGraph,
            executable::{OrchestrationStrategy, ResolvedChildTask},
            frame::{GraphFrame, GraphFrameId, GraphOwner, GraphResponse, GraphStackFrame},
            hitl::{HitlFrame, HitlFrameId, HitlFrameStatus},
            orchestration::{ChildRef, OrchestrationFrame, OrchestrationFrameId, Waiter},
        }, store::{SessionStore, SessionStoreClient}, worker::RunAgent,
    }, sink::SinkBoxExt as _, tools::{ToolCallId, ToolCallResult},
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
    store: SessionStoreClient
}

/// Frame à resoumettre comme nouveau Job une fois débloqué — soit un
/// [`AgentFrame`] (`RunAgent`), soit un [`GraphFrame`] entier (`RunGraphStep`,
/// même discipline "un pas par Job" que `RunAgent` "un tour par Job").
pub(crate) enum Resumed {
    Agent(AgentFrame),
    Graph(GraphFrame),
}

/// Commandes mutant l'état d'une session (persisté via [`SessionStoreClient`]),
/// consommées exclusivement par [`SessionServerActor`] — voir sa doc pour la
/// raison d'être de cette indirection (RPC -> Command -> mutation + évènement)
/// plutôt qu'une mutation directe comme le fait encore
/// [`crate::model::server::ModelServer`].
pub(crate) enum SessionCommand {
    Insert { session: Session, reply: oneshot::Sender<anyhow::Result<()>> },
    Replace { session: Session, reply: oneshot::Sender<anyhow::Result<()>> },
    Remove { id: SessionId, reply: oneshot::Sender<anyhow::Result<()>> },
    ReportAgentRun { agent_id: AgentId, response: AgentResponse, reply: oneshot::Sender<anyhow::Result<()>> },
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

type SessionServerEventEmitter = UnboundedSender<SessionEvent>;

pub struct SessionServerActor;

impl SessionServerActor {
    /// Démarre l'acteur : une tâche unique traite en série les
    /// [`SessionCommand`] reçues (mutation via [`SessionStoreClient`] +
    /// émission de [`SessionEvent`] sur succès, chacune déportée dans son
    /// propre `tokio::spawn` pour ne pas bloquer la réception des commandes
    /// suivantes), pendant que les RPC de lecture
    /// (`GetSession`/`ListSession`/`QueryVars`) accèdent directement au même
    /// [`SessionStoreClient`], partagé (cheap à cloner) — inutile de les
    /// faire transiter par l'acteur puisqu'elles ne mutent rien ni n'émettent
    /// d'évènement.
    pub fn create(
        layer: impl Layer<Send = SessionEvent, Received = SessionEvent>,
        mut args: SessionServerArgs,
    ) -> SessionServer {
        args.bootstrap.register_to_namespaces([Namespace::from_static(NS_SESSION)]);

        let (tx, rx) = layer.split();
        let mut tx = tx.boxed_sink();
        let _rx = rx.boxed();

        let (event_tx, mut event_rx) = mpsc::unbounded::<SessionEvent>();
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded::<SessionCommand>();

        let store = args.store;
        let worker = args.worker.clone();

        {
            let store = store.clone();
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
                                let store = store.clone();
                                let event_tx = event_tx.clone();
                                tokio::spawn(async move {
                                    let result = Self::insert(session, store, event_tx).await;
                                    let _ = reply.send(result);
                                });

                                continue;
                            }
                            Replace { session, reply } => {
                                let store = store.clone();
                                let event_tx = event_tx.clone();
                                tokio::spawn(async move {
                                    let result = Self::replace(session, store, event_tx).await;
                                    let _ = reply.send(result);
                                });

                                continue;
                            }
                            Remove { id, reply } => {
                                let store = store.clone();
                                let event_tx = event_tx.clone();
                                tokio::spawn(async move {
                                    let result = Self::remove(id, store, event_tx).await;
                                    let _ = reply.send(result);
                                });

                                continue;
                            }
                            ReportAgentRun { agent_id, response, reply } => {
                                tokio::spawn(Self::report_agent_run(
                                    agent_id,
                                    response,
                                    store.clone(),
                                    worker.clone(),
                                    event_tx.clone(),
                                    reply
                                ));

                                continue;
                            }
                            ReportToolDispatch { agent_id, tools_calls, reply } => {
                                tokio::spawn(Self::report_tool_dispatch(agent_id, tools_calls, store.clone(), event_tx.clone(), reply));
                                continue;
                            }
                            ReportToolExecution { agent_id, tool_call_id, result, reply } => {
                                tokio::spawn(Self::report_tool_execution(agent_id, tool_call_id, result, store.clone(), worker.clone(), event_tx.clone(), reply));
                                continue;
                            }
                            AppendLog { session_id, line, reply } => {
                                tokio::spawn(Self::append_log(session_id, line, store.clone(), event_tx.clone(), reply));
                                continue;
                            }
                            InsertInLog { session_id, log_id, text, reply } => {
                                tokio::spawn(Self::insert_in_log(session_id, log_id, text, store.clone(), event_tx.clone(), reply));
                                continue;
                            }
                            PatchVars { session_id, path, value, reply } => {
                                tokio::spawn(Self::patch_vars(session_id, path, value, store.clone(), event_tx.clone(), reply));
                                continue;
                            }
                            PushGraph { agent_id, graph_id, graph, reply } => {
                                tokio::spawn(Self::push_graph(agent_id, graph_id, graph, store.clone(), event_tx.clone(), reply));
                                continue;
                            }
                            UpdateGraphStep { graph_id, graph, reply } => {
                                tokio::spawn(Self::update_graph_step(graph_id, graph, store.clone(), event_tx.clone(), reply));
                                continue;
                            }
                            ReportGraphDispatch { graph_id, graph, spawn_agent, reply } => {
                                tokio::spawn(Self::report_graph_dispatch(graph_id, graph, spawn_agent, store.clone(), worker.clone(), event_tx.clone(), reply));
                                continue;
                            }
                            ReportGraphRun { graph_id, response, reply } => {
                                tokio::spawn(Self::report_graph_run(graph_id, response, store.clone(), worker.clone(), event_tx.clone(), reply));
                                continue;
                            }
                            PushOrchestration { orchestration_id, owner, owner_graph_update, strategy, children, reply } => {
                                tokio::spawn(Self::push_orchestration(orchestration_id, owner, owner_graph_update, strategy, children, store.clone(), worker.clone(), event_tx.clone(), reply));
                                continue;
                            }
                            PushHitl { hitl_id, owner, questions, owner_graph_update, reply } => {
                                tokio::spawn(Self::push_hitl(hitl_id, owner, questions, owner_graph_update, store.clone(), event_tx.clone(), reply));
                                continue;
                            }
                            ReportUserInput { session_id, hitl_id, answers, reply } => {
                                tokio::spawn(Self::report_user_input(session_id, hitl_id, answers, store.clone(), worker.clone(), event_tx.clone(), reply));
                                continue;
                            }
                        }
                    }
                }
            }
            });
        }

        {
            GetSession(store.clone()).register(&mut args.rpc_server);
            ListSession(store.clone()).register(&mut args.rpc_server);
            QueryVars(store.clone()).register(&mut args.rpc_server);

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
        }

        SessionServer { store, cmd_tx }
    }

    async fn insert(
        session: Session, 
        store: SessionStoreClient, 
        event_tx: SessionServerEventEmitter,
    ) -> Result<(), anyhow::Error> {
        let id = session.id;
        store.insert(session).await?;
        let _ = event_tx.unbounded_send(SessionEvent::Created { id });
        Ok(())
    }

    async fn replace(
        session: Session, 
        store: SessionStoreClient, 
        event_tx: SessionServerEventEmitter,
    ) -> Result<(), anyhow::Error> { 
        let id = session.id;
        store.replace(session).await?;
        let _ = event_tx.unbounded_send(SessionEvent::Updated { id });
        Ok(())
    }

    async fn remove(
        id: SessionId,
        store: SessionStoreClient, 
        event_tx: SessionServerEventEmitter,
    ) -> Result<(), anyhow::Error> {
        store.delete(id).await?;
        let _ = event_tx.unbounded_send(SessionEvent::Removed { id });
        Ok(())
    }

    async fn report_agent_run(
        agent_id: AgentId,
        response: AgentResponse,
        store: SessionStoreClient,
        worker: WorkerClient,
        event_tx: SessionServerEventEmitter,
        reply: oneshot::Sender<anyhow::Result<()>>
    ) -> anyhow::Result<()> {
        use SessionEvent::FrameStatusChanged;
        match report_agent_run(store.clone(), agent_id, response).await {
            Ok((status, resumed)) => {
                let _ = event_tx.unbounded_send(FrameStatusChanged { session_id: agent_id.session_id(), agent_id, status });
                spawn_resumed(&worker, resumed);
                let _ = reply.send(Ok(()));
            }
            Err(error) => {
                let _ = reply.send(Err(error));
            }
        }

        Ok(())
    }

    async fn report_tool_dispatch(
        agent_id: AgentId,
        tools_calls: Vec<ToolCallId>,
        store: SessionStoreClient,
        event_tx: SessionServerEventEmitter,
        reply: oneshot::Sender<Result<(), String>>,
    ) -> anyhow::Result<()> {
        let session_id = agent_id.session_id();
        match report_tool_dispatch(store, agent_id, tools_calls).await {
            Ok(status) => {
                let _ = event_tx.unbounded_send(SessionEvent::FrameStatusChanged { session_id, agent_id, status });
                let _ = reply.send(Ok(()));
            }
            Err(error) => {
                let _ = reply.send(Err(error.to_string()));
            }
        }

        Ok(())
    }

    async fn report_tool_execution(
        agent_id: AgentId,
        tool_call_id: ToolCallId,
        result: ToolCallResult,
        store: SessionStoreClient,
        worker: WorkerClient,
        event_tx: SessionServerEventEmitter,
        reply: oneshot::Sender<Result<(), String>>,
    ) -> anyhow::Result<()> {
        let session_id = agent_id.session_id();
        match report_tool_execution(store, agent_id, tool_call_id, result).await {
            Ok((status, resumed)) => {
                let _ = event_tx.unbounded_send(SessionEvent::FrameStatusChanged { session_id, agent_id, status });

                if let Some(frame) = resumed {
                    let _ = event_tx.unbounded_send(SessionEvent::FrameStatusChanged {
                        session_id,
                        agent_id: frame.id,
                        status: frame.status.clone(),
                    });

                    if let Err(err) = worker.spawn::<RunAgent>(frame, None).await {
                        error!(%err, "impossible de soumettre le job de reprise pour l'agent débloqué");
                    }
                }

                let _ = reply.send(Ok(()));
            }
            Err(error) => {
                let _ = reply.send(Err(error.to_string()));
            }
        }

        Ok(())
    }

    async fn append_log(
        session_id: SessionId,
        line: String,
        store: SessionStoreClient,
        event_tx: SessionServerEventEmitter,
        reply: oneshot::Sender<Result<(), String>>,
    ) -> anyhow::Result<()> {
        match append_log(store, session_id, line.clone()).await {
            Ok(log_id) => {
                let _ = event_tx.unbounded_send(SessionEvent::LogAppended { session_id, log_id, text: line });
                let _ = reply.send(Ok(()));
            }
            Err(error) => {
                let _ = reply.send(Err(error.to_string()));
            }
        }

        Ok(())
    }

    async fn insert_in_log(
        session_id: SessionId,
        log_id: SessionLogId,
        text: String,
        store: SessionStoreClient,
        event_tx: SessionServerEventEmitter,
        reply: oneshot::Sender<Result<(), String>>,
    ) -> anyhow::Result<()> {
        match insert_in_log(store, session_id, log_id, text.clone()).await {
            Ok(()) => {
                let _ = event_tx.unbounded_send(SessionEvent::LogAppended { session_id, log_id, text });
                let _ = reply.send(Ok(()));
            }
            Err(error) => {
                let _ = reply.send(Err(error.to_string()));
            }
        }

        Ok(())
    }

    async fn patch_vars(
        session_id: SessionId,
        path: String,
        value: Value,
        store: SessionStoreClient,
        event_tx: SessionServerEventEmitter,
        reply: oneshot::Sender<Result<(), String>>,
    ) -> anyhow::Result<()> {
        match patch_vars(store, session_id, &path, value).await {
            Ok(()) => {
                let _ = event_tx.unbounded_send(SessionEvent::VarsPatched { session_id });
                let _ = reply.send(Ok(()));
            }
            Err(error) => {
                let _ = reply.send(Err(error.to_string()));
            }
        }

        Ok(())
    }

    async fn push_graph(
        agent_id: AgentId,
        graph_id: GraphFrameId,
        graph: StateGraph,
        store: SessionStoreClient,
        event_tx: SessionServerEventEmitter,
        reply: oneshot::Sender<Result<(), String>>,
    ) -> anyhow::Result<()> {
        let session_id = agent_id.session_id();
        match push_graph(store, agent_id, graph_id, graph).await {
            Ok(status) => {
                let _ = event_tx.unbounded_send(SessionEvent::FrameStatusChanged { session_id, agent_id, status: status.clone() });
                let _ = event_tx.unbounded_send(SessionEvent::GraphStatusChanged { session_id, graph_id, status: AgentStatus::Running, current_node: None });
                let _ = reply.send(Ok(()));
            }
            Err(error) => {
                let _ = reply.send(Err(error.to_string()));
            }
        }

        Ok(())
    }

    async fn update_graph_step(
        graph_id: GraphFrameId,
        graph: GraphFrame,
        store: SessionStoreClient,
        event_tx: SessionServerEventEmitter,
        reply: oneshot::Sender<Result<(), String>>,
    ) -> anyhow::Result<()> {
        let session_id = graph_id.session_id();
        let status = graph.status();
        let current_node = graph.top().graph.ready_cursor().map(|cursor| cursor.current.clone());
        match update_graph_step(store, graph_id, graph).await {
            Ok(()) => {
                let _ = event_tx.unbounded_send(SessionEvent::GraphStatusChanged { session_id, graph_id, status, current_node });
                let _ = reply.send(Ok(()));
            }
            Err(error) => {
                let _ = reply.send(Err(error.to_string()));
            }
        }

        Ok(())
    }

    async fn report_graph_dispatch(
        graph_id: GraphFrameId,
        graph: GraphFrame,
        spawn_agent: AgentFrame,
        store: SessionStoreClient,
        worker: WorkerClient,
        event_tx: SessionServerEventEmitter,
        reply: oneshot::Sender<Result<(), String>>,
    ) -> anyhow::Result<()> {
        let session_id = graph_id.session_id();
        let status = graph.status();
        match report_graph_dispatch(store, graph_id, graph, spawn_agent.clone()).await {
            Ok(()) => {
                let _ = event_tx.unbounded_send(SessionEvent::GraphStatusChanged { session_id, graph_id, status, current_node: None });
                if let Err(err) = worker.spawn::<RunAgent>(spawn_agent, None).await {
                    error!(%err, "impossible de soumettre le job de l'agent spawné par un nœud de graphe");
                }
                let _ = reply.send(Ok(()));
            }
            Err(error) => {
                let _ = reply.send(Err(error.to_string()));
            }
        }

        Ok(())
    }

    async fn report_graph_run(
        graph_id: GraphFrameId,
        response: GraphResponse,
        store: SessionStoreClient,
        worker: WorkerClient,
        event_tx: SessionServerEventEmitter,
        reply: oneshot::Sender<Result<(), String>>,
    ) -> anyhow::Result<()> {
        let session_id = graph_id.session_id();
        match report_graph_run(store, graph_id, response).await {
            Ok((status, resumed)) => {
                let _ = event_tx.unbounded_send(SessionEvent::GraphStatusChanged { session_id, graph_id, status, current_node: None });
                spawn_resumed(&worker, resumed);
                let _ = reply.send(Ok(()));
            }
            Err(error) => {
                let _ = reply.send(Err(error.to_string()));
            }
        }

        Ok(())
    }

    async fn push_orchestration(
        orchestration_id: OrchestrationFrameId,
        owner: Waiter,
        owner_graph_update: Option<GraphFrame>,
        strategy: OrchestrationStrategy,
        children: Vec<ResolvedChildTask>,
        store: SessionStoreClient,
        worker: WorkerClient,
        event_tx: SessionServerEventEmitter,
        reply: oneshot::Sender<Result<(), String>>,
    ) -> anyhow::Result<()> {
        let session_id = orchestration_id.session_id();
        match push_orchestration(store, orchestration_id, owner, owner_graph_update, strategy, children).await {
            Ok((spawned, pending)) => {
                let _ = event_tx.unbounded_send(SessionEvent::OrchestrationStatusChanged {
                    session_id,
                    orchestration_id,
                    status: AgentStatus::Running,
                    pending,
                });
                spawn_resumed(&worker, spawned);
                let _ = reply.send(Ok(()));
            }
            Err(error) => {
                let _ = reply.send(Err(error.to_string()));
            }
        }

        Ok(())
    }

    async fn push_hitl(
        hitl_id: HitlFrameId,
        owner: Waiter,
        questions: Vec<Question>,
        owner_graph_update: Option<GraphFrame>,
        store: SessionStoreClient,
        event_tx: SessionServerEventEmitter,
        reply: oneshot::Sender<Result<(), String>>,
    ) -> anyhow::Result<()> {
        let session_id = hitl_id.session_id();
        match push_hitl(store, hitl_id, owner, questions, owner_graph_update).await {
            Ok(status) => {
                let _ = event_tx.unbounded_send(SessionEvent::HitlStatusChanged { session_id, hitl_id, status: HitlFrameStatus::Pending });
                match owner {
                    Waiter::Agent(agent_id) => {
                        let _ = event_tx.unbounded_send(SessionEvent::FrameStatusChanged { session_id, agent_id, status });
                    }
                    Waiter::Graph(graph_id) => {
                        let _ = event_tx.unbounded_send(SessionEvent::GraphStatusChanged { session_id, graph_id, status, current_node: None });
                    }
                }
                let _ = reply.send(Ok(()));
            }
            Err(error) => {
                let _ = reply.send(Err(error.to_string()));
            }
        }

        Ok(())
    }

    async fn report_user_input(
        session_id: SessionId,
        hitl_id: Option<HitlFrameId>,
        answers: HashMap<String, Answer>,
        store: SessionStoreClient,
        worker: WorkerClient,
        event_tx: SessionServerEventEmitter,
        reply: oneshot::Sender<Result<HitlFrameId, String>>,
    ) -> anyhow::Result<()> {
        match report_user_input(store, session_id, hitl_id, answers).await {
            Ok((hitl_id, hitl_status, resumed)) => {
                let _ = event_tx.unbounded_send(SessionEvent::HitlStatusChanged { session_id, hitl_id, status: hitl_status });

                if let Some(resumed) = resumed {
                    match &resumed {
                        Resumed::Agent(frame) => {
                            let _ = event_tx.unbounded_send(SessionEvent::FrameStatusChanged { session_id, agent_id: frame.id, status: frame.status.clone() });
                        }
                        Resumed::Graph(frame) => {
                            let _ = event_tx.unbounded_send(SessionEvent::GraphStatusChanged { session_id, graph_id: frame.id, status: frame.status(), current_node: None });
                        }
                    }
                    spawn_resumed(&worker, vec![resumed]);
                }

                let _ = reply.send(Ok(hitl_id));
            }
            Err(error) => {
                let _ = reply.send(Err(error.to_string()));
            }
        }

        Ok(())
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
    pub(crate) store: SessionStoreClient,
    pub(crate) cmd_tx: mpsc::UnboundedSender<SessionCommand>,
}

/// Récupère `session_id` dans `catalog`, ou une erreur lisible si elle n'est
/// pas (encore) connue de ce nœud — commun aux opérations ci-dessous, qui
/// mutent une session existante plutôt que d'en créer une (contrairement à
/// [`crate::session::rpc::InsertSession`], leur appelant est censé savoir
/// que la session existe déjà).
pub(crate) async fn get_session(store: SessionStoreClient, session_id: SessionId) -> Result<Session, anyhow::Error> 
{
    store
        .clone()
        .get(session_id)
        .await?
        .ok_or_else(|| anyhow!("session inconnue : {session_id}"))
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
pub(crate) async fn report_agent_run(
    store: SessionStoreClient,
    agent_id: AgentId,
    response: AgentResponse,
) -> Result<(AgentStatus, Vec<Resumed>), anyhow::Error> {
    let mut session = get_session(store.clone(), agent_id.session_id()).await?;

    let status = {
        let Some(frame) = session.frames.get_mut(&agent_id) else {
            return Err(anyhow!("frame {agent_id:?} inconnu de la session {}", agent_id.session_id()));
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

    store.replace(session).await?;
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
pub(crate) async fn report_tool_dispatch(
    store: SessionStoreClient,
    agent_id: AgentId,
    tools_calls: Vec<ToolCallId>,
) -> Result<AgentStatus, anyhow::Error> 
{
    let mut session = get_session(store.clone(), agent_id.session_id()).await?;

    let status = {
        let Some(frame) = session.frames.get_mut(&agent_id) else {
            return Err(anyhow!("frame {agent_id:?} inconnu de la session {}", agent_id.session_id()));
        };

        frame.status = AgentStatus::Yielding(YieldStatus::WaitingToolReply { tools_calls, tools_outputs: std::collections::HashMap::new() });
        frame.status.clone()
    };

    store.replace(session).await?;
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
pub(crate) async fn report_tool_execution(
    store: SessionStoreClient,
    agent_id: AgentId,
    tool_call_id: ToolCallId,
    result: ToolCallResult,
) -> Result<(AgentStatus, Option<AgentFrame>), anyhow::Error> 
{
    let mut session = get_session(store.clone(), agent_id.session_id()).await?;

    let Some(frame) = session.frames.get_mut(&agent_id) else {
        return Err(anyhow!("frame {agent_id:?} inconnu de la session {}", agent_id.session_id()));
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

    store.replace(session).await?;
    Ok((status, resumed))
}

/// Crée toujours une nouvelle entrée de journal (contrairement à
/// [`insert_in_log`], qui accumule sur une entrée existante) — voir
/// [`crate::session::rpc::AppendLog`].
pub(crate) async fn append_log(
    store: SessionStoreClient, 
    session_id: SessionId, 
    line: String
) -> Result<SessionLogId, anyhow::Error> {
    let mut session = get_session(store.clone(), session_id).await?;
    let log_id = SessionLogId::new(crate::id::generate_id());
    session.logs.push(SessionLog { id: log_id, content: line });
    store.replace(session).await?;
    
    Ok(log_id)
}

/// Ajoute `text` à la suite du [`SessionLog`] identifié par `log_id`, ou crée
/// cette entrée si elle n'existe pas encore (premier fragment d'un flux) —
/// voir [`crate::session::rpc::InsertInLog`].
pub(crate) async fn insert_in_log(
    store: SessionStoreClient, 
    session_id: SessionId, 
    log_id: SessionLogId, 
    text: String
) -> Result<(), anyhow::Error> {
    let mut session = get_session(store.clone(), session_id).await?;
    match session.logs.iter_mut().find(|log| log.id == log_id) {
        Some(log) => log.content.push_str(&text),
        None => session.logs.push(SessionLog { id: log_id, content: text }),
    }
    store.replace(session).await?;
    Ok(())
}

/// Évalue `path` (JSONPath) contre `Session::vars`, traité comme un unique
/// document JSON (voir [`crate::session::SessionVarsQueryRequest`]).
pub(crate) async fn query_vars(
    catalog: SessionStoreClient, 
    session_id: SessionId, 
    path: &str
) -> Result<Vec<Value>, anyhow::Error> {
    let session = get_session(catalog, session_id).await?;
    let doc = serde_json::to_value(&session.vars)?;
    let matches = jsonpath_lib::select(&doc, path)?;
    Ok(matches.into_iter().cloned().collect())
}

/// Remplace, dans `Session::vars` traité comme un unique document JSON,
/// chaque nœud correspondant à `path` par `value` (voir
/// [`crate::session::SessionVarsPatchRequest`]).
pub(crate) async fn patch_vars(
    store: SessionStoreClient, 
    session_id: SessionId, 
    path: &str, 
    value: Value
) -> Result<(), anyhow::Error> {
    let mut session = get_session(store.clone(), session_id).await?;
    let doc = serde_json::to_value(&session.vars)?;
    let patched = jsonpath_lib::replace_with(doc, path, &mut |_| Some(value.clone()))?;
    session.vars = serde_json::from_value(patched)?;

    store.replace(session).await?;
    Ok(())
}

/// Insère un nouveau [`GraphFrame`] (un seul niveau de pile, `return_node: None`)
/// et fait passer `agent_id` en [`YieldStatus::WaitingGraph`] — voir
/// [`crate::session::rpc::PushGraph`], appelée quand un agent pousse un mode
/// `state_graph` (`system/push-mode`, non câblé encore côté dispatch de
/// tool) ou, plus généralement dès aujourd'hui, comme point d'entrée
/// programmatique pour démarrer un graphe sur une session.
pub(crate) async fn push_graph(
    store: SessionStoreClient, 
    agent_id: AgentId, 
    graph_id: GraphFrameId, 
    graph: StateGraph
) -> Result<AgentStatus, anyhow::Error> {
    let mut session = get_session(store.clone(), agent_id.session_id()).await?;

    let status = {
        let Some(frame) = session.frames.get_mut(&agent_id) else {
            return Err(anyhow!("frame {agent_id:?} inconnu de la session {}", agent_id.session_id()));
        };

        frame.status = AgentStatus::Yielding(YieldStatus::WaitingGraph { graph: graph_id });
        frame.status.clone()
    };

    session.graphs.insert(
        graph_id,
        GraphFrame { id: graph_id, owner: GraphOwner::Agent(agent_id), stack: vec![GraphStackFrame { graph, return_node: None }], error: String::new() },
    );

    store.replace(session).await?;
    Ok(status)
}

/// Remplace l'entrée `session.graphs[graph_id]` par `graph` — cheval de
/// bataille appelé par le driver (`RunGraphStep`) après chaque pas qui n'a ni
/// conclu ni yieldé sur un enfant (avancée normale, fork, join, entrée en
/// sous-graphe) : sur le même modèle que [`Session::insert`]/[`Session::update`]
/// pour un [`AgentFrame`], remplacement complet plutôt que fusion de delta.
pub(crate) async fn update_graph_step(
    store: SessionStoreClient, 
    graph_id: GraphFrameId, 
    graph: GraphFrame
) -> Result<(), anyhow::Error> {
    let mut session = get_session(store.clone(), graph_id.session_id()).await?;

    if !session.graphs.contains_key(&graph_id) {
        return Err(anyhow!("graphe {graph_id} inconnu de la session {}", graph_id.session_id()));
    }

    session.graphs.insert(graph_id, graph);
    store.replace(session).await?;
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
pub(crate) async fn report_graph_dispatch(
    store: SessionStoreClient, 
    graph_id: GraphFrameId, 
    graph: GraphFrame, 
    spawn_agent: AgentFrame
) -> Result<(), anyhow::Error> {
    let mut session = get_session(store.clone(), graph_id.session_id()).await?;
    session.graphs.insert(graph_id, graph);
    session.frames.insert(spawn_agent.id, spawn_agent);
    store.replace(session).await?;
    Ok(())
}

/// Rapporte l'issue d'un `GraphFrame` (racine conclue ou en échec, voir
/// [`crate::session::state::worker::RunGraphStep`]) — même mécanique de
/// réveil en cascade que [`report_agent_run`] : les [`AgentFrame`] en
/// [`YieldStatus::WaitingGraph`] et les [`OrchestrationFrame`] dont ce graphe
/// est un enfant attendu sont débloqués.
pub(crate) async fn report_graph_run(
    store: SessionStoreClient, 
    graph_id: GraphFrameId,
    response: GraphResponse
) -> Result<(AgentStatus, Vec<Resumed>), anyhow::Error> {
    let mut session = get_session(store.clone(), graph_id.session_id()).await?;

    let status = {
        let Some(graph) = session.graphs.get_mut(&graph_id) else {
            return Err(anyhow!("graphe {graph_id} inconnu de la session {}", graph_id.session_id()));
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

    store.replace(session).await?;
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
pub(crate) async fn push_orchestration(
    store: SessionStoreClient,
    orchestration_id: OrchestrationFrameId,
    owner: Waiter,
    owner_graph_update: Option<GraphFrame>,
    strategy: OrchestrationStrategy,
    children: Vec<ResolvedChildTask>,
) -> Result<(Vec<Resumed>, usize), anyhow::Error> {
    let session_id = orchestration_id.session_id();
    let mut session = get_session(store.clone(), session_id).await?;

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

    store.replace(session).await?;
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
pub(crate) async fn push_hitl(
    store: SessionStoreClient,
    hitl_id: HitlFrameId,
    owner: Waiter,
    questions: Vec<Question>,
    owner_graph_update: Option<GraphFrame>,
) -> Result<AgentStatus, anyhow::Error> {
    let session_id = hitl_id.session_id();
    let mut session = get_session(store.clone(), session_id).await?;

    let status = match owner {
        Waiter::Agent(agent_id) => {
            let Some(frame) = session.frames.get_mut(&agent_id) else {
                return Err(anyhow!("frame {agent_id:?} inconnu de la session {session_id}"));
            };

            frame.status = AgentStatus::Yielding(YieldStatus::WaitingHitl { hitl: hitl_id });
            frame.status.clone()
        }
        Waiter::Graph(graph_id) => {
            let graph = owner_graph_update.ok_or_else(|| anyhow!("graphe {graph_id} : mise à jour du curseur manquante"))?;
            let status = graph.status();
            session.graphs.insert(graph_id, graph);
            status
        }
    };

    session.hitls.insert(hitl_id, HitlFrame { id: hitl_id, owner, questions, status: HitlFrameStatus::Pending });

    store.replace(session).await?;
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
pub(crate) async fn report_user_input(
    store: SessionStoreClient,
    session_id: SessionId,
    hitl_id: Option<HitlFrameId>,
    answers: HashMap<String, Answer>,
) -> Result<(HitlFrameId, HitlFrameStatus, Option<Resumed>), anyhow::Error> {
    use AgentStatus::Yielding;
    use YieldStatus::WaitingHitl;

    let mut session = get_session(store.clone(), session_id).await?;

    let hitl_id = match hitl_id {
        Some(id) => id,
        None => {
            let mut matches = session.frames.iter_waiting_hitl();

            let Some(only) = matches.next() else {
                return Err(anyhow!("aucun agent en attente d'une réponse humaine dans cette session"));
            };
            if matches.next().is_some() {
                return Err(anyhow!("plusieurs agents en attente d'une réponse humaine : précisez hitl_id"));
            }

            let Yielding(WaitingHitl { hitl }) = &only.status else {
                unreachable!("le filtre ci-dessus garantit ce statut");
            };
            *hitl
        }
    };

    let Some(hitl) = session.hitls.get_mut(&hitl_id) else {
        return Err(anyhow!("formulaire {hitl_id} inconnu de la session {session_id}"));
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
                return Err(anyhow!("frame {agent_id:?} inconnu de la session {session_id}"));
            };

            frame.context.push(ContextEntry { role: Role::Tool, content: format!("[hitl {hitl_id}] {}", hitl_answers_to_value(&answers)) });
            frame.status = AgentStatus::Running;
            Resumed::Agent(frame.clone())
        }
        Waiter::Graph(graph_id) => {
            let Some(graph) = session.graphs.get_mut(&graph_id) else {
                return Err(anyhow!("graphe {graph_id} inconnu de la session {session_id}"));
            };

            let Some(cursor) = graph.top_mut().graph.cursors.iter_mut().find(|cursor| matches!(&cursor.status, AgentStatus::Yielding(YieldStatus::WaitingHitl { hitl }) if *hitl == hitl_id))
            else {
                return Err(anyhow!("aucun curseur du graphe {graph_id} n'attend le formulaire {hitl_id}"));
            };

            cursor.last_output = hitl_answers_to_value(&answers);
            cursor.status = AgentStatus::Running;
            Resumed::Graph(graph.clone())
        }
    };

    store.replace(session).await?;
    Ok((hitl_id, hitl_status, Some(resumed)))
}

#[cfg(test)]
mod tests {}
