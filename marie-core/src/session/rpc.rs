use async_trait::async_trait;
use futures::channel::mpsc;
use libp2p::PeerId;
use serde_json::Value;
use tokio::sync::oneshot;

use crate::{
    rpc::{RemoteProcedureCall, Void}, session::{
        Session, SessionAppendLogRequest, SessionId, SessionInsertInLogRequest, SessionPushGraphRequest, SessionPushHitlRequest, SessionPushOrchestrationRequest, SessionReportAgentRunRequest, SessionReportGraphDispatchRequest, SessionReportGraphRunRequest, SessionReportToolDispatchRequest, SessionReportToolExecutionRequest, SessionReportUserInputRequest, SessionUpdateGraphStepRequest, SessionVarsPatchRequest, SessionVarsQueryRequest, SessionVarsRemoveRequest, server::{SessionCommand, query_vars}, store::{SessionStore, SessionStoreClient},
    },
    state_graph::hitl::HitlFrameId,
};

/// Récupère une session du catalogue, ou `None` si inconnue de ce nœud —
/// voir [`crate::session::client::SessionClient::get`].
#[derive(Clone)]
pub struct GetSession(pub(crate) SessionStoreClient);

#[async_trait]
impl RemoteProcedureCall for GetSession {
    const NAME: &'static str = "/marie/sessions/get";

    type Args = SessionId;
    type Return = Option<Session>;

    async fn execute(self, id: SessionId, _: PeerId) -> Option<Session> {
        self.0.get(id).await.ok().flatten()
    }
}

/// Liste tout le catalogue de sessions connu de ce nœud.
#[derive(Clone)]
pub struct ListSession(pub(crate) SessionStoreClient);

#[async_trait]
impl RemoteProcedureCall for ListSession {
    const NAME: &'static str = "/marie/sessions/list";

    type Args = Void;
    type Return = Vec<Session>;

    async fn execute(self, _: Void, _: PeerId) -> Vec<Session> {
        self.0.list().await.unwrap_or_default()
    }
}

/// Crée une session dans le catalogue — envoie une [`SessionCommand::Insert`]
/// à [`crate::session::server::SessionServerActor`] plutôt que de muter le
/// catalogue directement, pour que l'insertion émette
/// [`crate::session::SessionEvent::Created`].
#[derive(Clone)]
pub struct InsertSession(pub(crate) mpsc::UnboundedSender<SessionCommand>);

#[async_trait]
impl RemoteProcedureCall for InsertSession {
    const NAME: &'static str = "/marie/sessions/insert";

    type Args = Session;
    type Return = Void;

    async fn execute(self, session: Session, _: PeerId) -> Void {
        let (reply, rx) = oneshot::channel();
        let _ = self.0.unbounded_send(SessionCommand::Insert { session, reply });
        let _ = rx.await;
        Void
    }
}

/// Remplace l'état complet d'une session existante — voir [`InsertSession`]
/// pour la raison du passage par une commande.
#[derive(Clone)]
pub struct UpdateSession(pub(crate) mpsc::UnboundedSender<SessionCommand>);

#[async_trait]
impl RemoteProcedureCall for UpdateSession {
    const NAME: &'static str = "/marie/sessions/update";

    type Args = Session;
    type Return = Void;

    async fn execute(self, session: Session, _: PeerId) -> Void {
        let (reply, rx) = oneshot::channel();
        let _ = self.0.unbounded_send(SessionCommand::Replace { session, reply });
        let _ = rx.await;
        Void
    }
}

/// Retire une session du catalogue — voir [`InsertSession`] pour la raison
/// du passage par une commande.
#[derive(Clone)]
pub struct RemoveSession(pub(crate) mpsc::UnboundedSender<SessionCommand>);

#[async_trait]
impl RemoteProcedureCall for RemoveSession {
    const NAME: &'static str = "/marie/sessions/remove";

    type Args = SessionId;
    type Return = Void;

    async fn execute(self, id: SessionId, _: PeerId) -> Void {
        let (reply, rx) = oneshot::channel();
        let _ = self.0.unbounded_send(SessionCommand::Remove { id, reply });
        let _ = rx.await;
        Void
    }
}

/// Rapporte l'issue d'un run `RunAgent` pour le frame concerné — voir
/// [`SessionReportAgentRunRequest`] et
/// [`crate::session::server::report_agent_run`]. Appelée en direct par
/// `RunAgent` en toute fin de `Job` (RPC synchrone, pas un évènement gossip)
/// pour avoir la certitude que `SessionServer` a bien reçu et appliqué le
/// résultat avant que le `Job` ne se termine.
#[derive(Clone)]
pub struct ReportAgentRun(pub(crate) mpsc::UnboundedSender<SessionCommand>);

#[async_trait]
impl RemoteProcedureCall for ReportAgentRun {
    const NAME: &'static str = "/marie/sessions/report-agent-run";

    type Args = SessionReportAgentRunRequest;
    type Return = Result<(), String>;

    async fn execute(self, request: SessionReportAgentRunRequest, _: PeerId) -> Result<(), String> {
        let (reply, rx) = oneshot::channel();
        let _ = self.0.unbounded_send(SessionCommand::ReportAgentRun {
            agent_id: request.agent_id,
            response: request.response,
            reply,
        });
        match rx.await {
            Ok(result) => result.map_err(|e| e.to_string()),
            Err(_) => Err("le serveur de sessions s'est arrêté".to_string()),
        }
    }
}

/// Persiste l'attente d'une réponse de tool(s) pour le frame concerné —
/// voir [`SessionReportToolDispatchRequest`] et
/// [`crate::session::server::report_tool_dispatch`]. Appelée par
/// `session::worker::run_turns` *avant* de déclencher les jobs
/// `ToolExecution` correspondants (voir la doc de
/// [`SessionReportToolDispatchRequest`] pour la course qu'évite cet ordre).
#[derive(Clone)]
pub struct ReportToolDispatch(pub(crate) mpsc::UnboundedSender<SessionCommand>);

#[async_trait]
impl RemoteProcedureCall for ReportToolDispatch {
    const NAME: &'static str = "/marie/sessions/report-tool-dispatch";

    type Args = SessionReportToolDispatchRequest;
    type Return = Result<(), String>;

    async fn execute(self, request: SessionReportToolDispatchRequest, _: PeerId) -> Result<(), String> {
        let (reply, rx) = oneshot::channel();
        let _ = self.0.unbounded_send(SessionCommand::ReportToolDispatch {
            agent_id: request.agent_id,
            tools_calls: request.tools_calls,
            reply,
        });
        rx.await.unwrap_or_else(|_| Err("le serveur de sessions s'est arrêté".to_string()))
    }
}

/// Rapporte l'issue d'un appel de tool pour le frame appelant — voir
/// [`SessionReportToolExecutionRequest`] et
/// [`crate::session::server::report_tool_execution`]. Appelée en direct par
/// `tools::worker::ToolExecution` en toute fin de `Job`, même raison qu'une
/// RPC directe et synchrone que [`ReportAgentRun`].
#[derive(Clone)]
pub struct ReportToolExecution(pub(crate) mpsc::UnboundedSender<SessionCommand>);

#[async_trait]
impl RemoteProcedureCall for ReportToolExecution {
    const NAME: &'static str = "/marie/sessions/report-tool-execution";

    type Args = SessionReportToolExecutionRequest;
    type Return = Result<(), String>;

    async fn execute(self, request: SessionReportToolExecutionRequest, _: PeerId) -> Result<(), String> {
        let (reply, rx) = oneshot::channel();
        let _ = self.0.unbounded_send(SessionCommand::ReportToolExecution {
            agent_id: request.agent_id,
            tool_call_id: request.tool_call_id,
            result: request.result,
            reply,
        });
        rx.await.unwrap_or_else(|_| Err("le serveur de sessions s'est arrêté".to_string()))
    }
}

/// Ajoute une ligne au journal d'évènements de la session — voir
/// [`SessionAppendLogRequest`].
#[derive(Clone)]
pub struct AppendLog(pub(crate) mpsc::UnboundedSender<SessionCommand>);

#[async_trait]
impl RemoteProcedureCall for AppendLog {
    const NAME: &'static str = "/marie/sessions/append-log";

    type Args = SessionAppendLogRequest;
    type Return = Result<(), String>;

    async fn execute(self, request: SessionAppendLogRequest, _: PeerId) -> Result<(), String> {
        let (reply, rx) = oneshot::channel();
        let _ = self.0.unbounded_send(SessionCommand::AppendLog {
            session_id: request.session_id,
            line: request.line,
            reply,
        });
        rx.await.unwrap_or_else(|_| Err("le serveur de sessions s'est arrêté".to_string()))
    }
}

/// Ajoute du texte à la suite d'un log existant, ou le crée si `log_id` est
/// inconnu — voir [`SessionInsertInLogRequest`]. Contrairement à
/// [`AppendLog`], permet d'accumuler du texte reçu au fil d'un flux (voir
/// `model::ModelStreamEvent::TextDelta`) dans la même entrée plutôt que d'en
/// créer une par fragment.
#[derive(Clone)]
pub struct InsertInLog(pub(crate) mpsc::UnboundedSender<SessionCommand>);

#[async_trait]
impl RemoteProcedureCall for InsertInLog {
    const NAME: &'static str = "/marie/sessions/insert-in-log";

    type Args = SessionInsertInLogRequest;
    type Return = Result<(), String>;

    async fn execute(self, request: SessionInsertInLogRequest, _: PeerId) -> Result<(), String> {
        let (reply, rx) = oneshot::channel();
        let _ = self.0.unbounded_send(SessionCommand::InsertInLog {
            session_id: request.session_id,
            log_id: request.log_id,
            text: request.text,
            reply,
        });
        rx.await.unwrap_or_else(|_| Err("le serveur de sessions s'est arrêté".to_string()))
    }
}

/// Évalue une expression JSONPath contre `Session::vars` — voir
/// [`SessionVarsQueryRequest`]. Opération de lecture seule : ne passe pas
/// par [`crate::session::server::SessionCommand`], contrairement aux RPC
/// mutantes ci-dessus.
#[derive(Clone)]
pub struct QueryVars(pub(crate) SessionStoreClient);

#[async_trait]
impl RemoteProcedureCall for QueryVars {
    const NAME: &'static str = "/marie/sessions/vars/query";

    type Args = SessionVarsQueryRequest;
    type Return = Result<Vec<Value>, String>;

    async fn execute(self, request: SessionVarsQueryRequest, _: PeerId) -> Result<Vec<Value>, String> {
        query_vars(self.0, request.session_id, &request.path).await.map_err(|e| e.to_string())
    }
}

/// Remplace, dans `Session::vars`, chaque nœud trouvé par une expression
/// JSONPath — voir [`SessionVarsPatchRequest`].
#[derive(Clone)]
pub struct PatchVars(pub(crate) mpsc::UnboundedSender<SessionCommand>);

#[async_trait]
impl RemoteProcedureCall for PatchVars {
    const NAME: &'static str = "/marie/sessions/vars/patch";

    type Args = SessionVarsPatchRequest;
    type Return = Result<(), String>;

    async fn execute(self, request: SessionVarsPatchRequest, _: PeerId) -> Result<(), String> {
        let (reply, rx) = oneshot::channel();
        let _ = self.0.unbounded_send(SessionCommand::PatchVars {
            session_id: request.session_id,
            path: request.path,
            value: request.value,
            reply,
        });
        rx.await.unwrap_or_else(|_| Err("le serveur de sessions s'est arrêté".to_string()))
    }
}

/// Retire, dans `Session::vars`, chaque nœud trouvé par une expression
/// JSONPath — voir [`SessionVarsRemoveRequest`].
#[derive(Clone)]
pub struct RemoveVars(pub(crate) mpsc::UnboundedSender<SessionCommand>);

#[async_trait]
impl RemoteProcedureCall for RemoveVars {
    const NAME: &'static str = "/marie/sessions/vars/remove";

    type Args = SessionVarsRemoveRequest;
    type Return = Result<(), String>;

    async fn execute(self, request: SessionVarsRemoveRequest, _: PeerId) -> Result<(), String> {
        let (reply, rx) = oneshot::channel();
        let _ = self.0.unbounded_send(SessionCommand::RemoveVars {
            session_id: request.session_id,
            path: request.path,
            reply,
        });
        rx.await.unwrap_or_else(|_| Err("le serveur de sessions s'est arrêté".to_string()))
    }
}

/// Pousse un nouveau [`crate::state_graph::frame::GraphFrame`] — voir
/// [`SessionPushGraphRequest`]/[`crate::session::server::push_graph`].
#[derive(Clone)]
pub struct PushGraph(pub(crate) mpsc::UnboundedSender<SessionCommand>);

#[async_trait]
impl RemoteProcedureCall for PushGraph {
    const NAME: &'static str = "/marie/sessions/push-graph";

    type Args = SessionPushGraphRequest;
    type Return = Result<(), String>;

    async fn execute(self, request: SessionPushGraphRequest, _: PeerId) -> Result<(), String> {
        let (reply, rx) = oneshot::channel();
        let _ = self.0.unbounded_send(SessionCommand::PushGraph { agent_id: request.agent_id, graph_id: request.graph_id, graph: request.graph, reply });
        rx.await.unwrap_or_else(|_| Err("le serveur de sessions s'est arrêté".to_string()))
    }
}

/// Persiste la progression d'un [`crate::state_graph::frame::GraphFrame`]
/// après un pas qui n'a ni conclu ni yieldé — voir
/// [`SessionUpdateGraphStepRequest`]/[`crate::session::server::update_graph_step`].
#[derive(Clone)]
pub struct UpdateGraphStep(pub(crate) mpsc::UnboundedSender<SessionCommand>);

#[async_trait]
impl RemoteProcedureCall for UpdateGraphStep {
    const NAME: &'static str = "/marie/sessions/update-graph-step";

    type Args = SessionUpdateGraphStepRequest;
    type Return = Result<(), String>;

    async fn execute(self, request: SessionUpdateGraphStepRequest, _: PeerId) -> Result<(), String> {
        let (reply, rx) = oneshot::channel();
        let _ = self.0.unbounded_send(SessionCommand::UpdateGraphStep { graph_id: request.graph_id, graph: request.graph, reply });
        rx.await.unwrap_or_else(|_| Err("le serveur de sessions s'est arrêté".to_string()))
    }
}

/// Persiste l'attente d'un enfant `Agent` spawné par un curseur de graphe,
/// avant que son Job `RunAgent` ne soit soumis — voir
/// [`SessionReportGraphDispatchRequest`]/[`crate::session::server::report_graph_dispatch`].
#[derive(Clone)]
pub struct ReportGraphDispatch(pub(crate) mpsc::UnboundedSender<SessionCommand>);

#[async_trait]
impl RemoteProcedureCall for ReportGraphDispatch {
    const NAME: &'static str = "/marie/sessions/report-graph-dispatch";

    type Args = SessionReportGraphDispatchRequest;
    type Return = Result<(), String>;

    async fn execute(self, request: SessionReportGraphDispatchRequest, _: PeerId) -> Result<(), String> {
        let (reply, rx) = oneshot::channel();
        let _ = self.0.unbounded_send(SessionCommand::ReportGraphDispatch {
            graph_id: request.graph_id,
            graph: request.graph,
            spawn_agent: request.spawn_agent,
            reply,
        });
        rx.await.unwrap_or_else(|_| Err("le serveur de sessions s'est arrêté".to_string()))
    }
}

/// Rapporte l'issue finale d'un [`crate::state_graph::frame::GraphFrame`] —
/// voir [`SessionReportGraphRunRequest`]/[`crate::session::server::report_graph_run`].
#[derive(Clone)]
pub struct ReportGraphRun(pub(crate) mpsc::UnboundedSender<SessionCommand>);

#[async_trait]
impl RemoteProcedureCall for ReportGraphRun {
    const NAME: &'static str = "/marie/sessions/report-graph-run";

    type Args = SessionReportGraphRunRequest;
    type Return = Result<(), String>;

    async fn execute(self, request: SessionReportGraphRunRequest, _: PeerId) -> Result<(), String> {
        let (reply, rx) = oneshot::channel();
        let _ = self.0.unbounded_send(SessionCommand::ReportGraphRun { graph_id: request.graph_id, response: request.response, reply });
        rx.await.unwrap_or_else(|_| Err("le serveur de sessions s'est arrêté".to_string()))
    }
}

/// Crée une nouvelle [`crate::state_graph::orchestration::OrchestrationFrame`]
/// et ses enfants — voir
/// [`SessionPushOrchestrationRequest`]/[`crate::session::server::push_orchestration`].
#[derive(Clone)]
pub struct PushOrchestration(pub(crate) mpsc::UnboundedSender<SessionCommand>);

#[async_trait]
impl RemoteProcedureCall for PushOrchestration {
    const NAME: &'static str = "/marie/sessions/push-orchestration";

    type Args = SessionPushOrchestrationRequest;
    type Return = Result<(), String>;

    async fn execute(self, request: SessionPushOrchestrationRequest, _: PeerId) -> Result<(), String> {
        let (reply, rx) = oneshot::channel();
        let _ = self.0.unbounded_send(SessionCommand::PushOrchestration {
            orchestration_id: request.orchestration_id,
            owner: request.owner,
            owner_graph_update: request.owner_graph_update,
            strategy: request.strategy,
            children: request.children,
            reply,
        });
        rx.await.unwrap_or_else(|_| Err("le serveur de sessions s'est arrêté".to_string()))
    }
}

/// Crée un nouveau [`crate::state_graph::hitl::HitlFrame`] — voir
/// [`SessionPushHitlRequest`]/[`crate::session::server::push_hitl`].
#[derive(Clone)]
pub struct PushHitl(pub(crate) mpsc::UnboundedSender<SessionCommand>);

#[async_trait]
impl RemoteProcedureCall for PushHitl {
    const NAME: &'static str = "/marie/sessions/push-hitl";

    type Args = SessionPushHitlRequest;
    type Return = Result<(), String>;

    async fn execute(self, request: SessionPushHitlRequest, _: PeerId) -> Result<(), String> {
        let (reply, rx) = oneshot::channel();
        let _ = self.0.unbounded_send(SessionCommand::PushHitl {
            hitl_id: request.hitl_id,
            owner: request.owner,
            questions: request.questions,
            owner_graph_update: request.owner_graph_update,
            reply,
        });
        rx.await.unwrap_or_else(|_| Err("le serveur de sessions s'est arrêté".to_string()))
    }
}

/// Rapporte une réponse humaine — voir
/// [`SessionReportUserInputRequest`]/[`crate::session::server::report_user_input`].
/// `Return` renvoie le [`HitlFrameId`] effectivement résolu : utile
/// spécifiquement pour un input spontané (`hitl_id: None` en entrée),
/// l'appelant ne sachant pas d'avance sur quel formulaire il est tombé.
#[derive(Clone)]
pub struct ReportUserInput(pub(crate) mpsc::UnboundedSender<SessionCommand>);

#[async_trait]
impl RemoteProcedureCall for ReportUserInput {
    const NAME: &'static str = "/marie/sessions/report-user-input";

    type Args = SessionReportUserInputRequest;
    type Return = Result<HitlFrameId, String>;

    async fn execute(self, request: SessionReportUserInputRequest, _: PeerId) -> Result<HitlFrameId, String> {
        let (reply, rx) = oneshot::channel();
        let _ = self.0.unbounded_send(SessionCommand::ReportUserInput {
            session_id: request.session_id,
            hitl_id: request.hitl_id,
            answers: request.answers,
            reply,
        });
        rx.await.unwrap_or_else(|_| Err("le serveur de sessions s'est arrêté".to_string()))
    }
}
