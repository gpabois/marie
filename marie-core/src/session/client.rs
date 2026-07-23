use libp2p::PeerId;
use serde_json::Value;
use thiserror::Error;

use std::collections::HashMap;

use crate::{
    agent::{AgentId, frame::AgentFrame, status::AgentResponse}, di::{Factory, Get, Resolve}, hitl::{Answer, Question}, network::{LocalPeerId, bootstrap::BootstrapClient}, rpc::{RpcClient, RpcError, Void}, session::{
        NS_SESSION, Session, SessionAppendLogRequest, SessionId, SessionInsertInLogRequest, SessionLogId, SessionPushGraphRequest, SessionPushHitlRequest, SessionPushOrchestrationRequest, SessionReportAgentRunRequest, SessionReportGraphDispatchRequest, SessionReportGraphRunRequest, SessionReportToolDispatchRequest, SessionReportToolExecutionRequest, SessionReportUserInputRequest, SessionUpdateGraphStepRequest, SessionVarsPatchRequest, SessionVarsQueryRequest, SessionVarsRemoveRequest,
        rpc::{AppendLog, GetSession, InsertInLog, InsertSession, ListSession, PatchVars, PushGraph, PushHitl, PushOrchestration, QueryVars, RemoveSession, RemoveVars, ReportAgentRun, ReportGraphDispatch, ReportGraphRun, ReportToolDispatch, ReportToolExecution, ReportUserInput, UpdateGraphStep, UpdateSession},
    }, state_graph::{
        StateGraph,
        executable::{OrchestrationStrategy, ResolvedChildTask},
        frame::{GraphFrame, GraphFrameId, GraphResponse},
        hitl::HitlFrameId,
        orchestration::{OrchestrationFrameId, Waiter},
    }, tools::{ToolCallId, ToolCallResult},
};

#[derive(Debug, Error)]
pub enum SessionError {
    #[error("aucun catalogue de sessions n'est disponible")]
    NoCatalogAvailable,
    #[error("session inconnue : {0}")]
    UnknownSession(SessionId),
    #[error("[Session] échec de l'appel distant : {0}")]
    RpcError(#[from] RpcError),
    #[error("échec côté serveur de sessions : {0}")]
    Server(String),
}

/// Point d'entrée pour le CRUD du catalogue de sessions, sur le même modèle
/// que [`crate::expert::client::ExpertClient`]/[`crate::model::client::ModelClient`] :
/// chaque opération sélectionne de manière déterministe le pair qui héberge
/// le catalogue (voir [`Self::select_catalog`]) plutôt que de s'appuyer sur
/// une réplication Raft.
#[derive(Clone)]
pub struct SessionClient {
    local_peer_id: LocalPeerId,
    rpc: RpcClient,
    bootstrap: BootstrapClient,
}

impl<D> Factory<D> for SessionClient 
    where 
        D: Get<LocalPeerId> 
            + Get<RpcClient> 
            + Get<BootstrapClient>
{
    fn create(container: &D) -> Self {
        Self {
            local_peer_id: container.get(),
            rpc: container.resolve(),
            bootstrap: container.resolve()
        }
    }
}

impl SessionClient {
    /// Récupère une session auprès du nœud qui la sert.
    pub async fn get(&self, id: impl Into<SessionId>) -> Result<Session, SessionError> {
        let id = id.into();
        let catalog = self.select_catalog(&id)?;

        self.rpc
            .invoke::<GetSession>(id, [catalog])
            .await?
            .ok_or(SessionError::UnknownSession(id))
    }

    /// Liste tout le catalogue de sessions connu du nœud sélectionné.
    pub async fn list(&self) -> Result<Vec<Session>, SessionError> {
        let catalog = self.select_catalog(self.local_peer_id.to_bytes())?;

        self.rpc.invoke::<ListSession>(Void, [catalog]).await.map_err(SessionError::from)
    }

    /// Crée une session dans le catalogue.
    pub async fn insert(&self, session: Session) -> Result<(), SessionError> {
        let catalog = self.select_catalog(&session.id)?;

        self.rpc.invoke::<InsertSession>(session, [catalog]).await?;

        Ok(())
    }

    /// Remplace l'état complet d'une session existante.
    pub async fn update(&self, session: Session) -> Result<(), SessionError> {
        let catalog = self.select_catalog(&session.id)?;

        self.rpc.invoke::<UpdateSession>(session, [catalog]).await?;

        Ok(())
    }

    /// Retire une session du catalogue.
    pub async fn remove(&self, id: impl Into<SessionId>) -> Result<(), SessionError> {
        let id = id.into();
        let catalog = self.select_catalog(&id)?;

        self.rpc.invoke::<RemoveSession>(id, [catalog]).await?;

        Ok(())
    }

    /// Rapporte l'issue d'un run `RunAgent` pour `agent_id` — voir
    /// [`crate::session::server::SessionServer`], qui met à jour le frame
    /// concerné (statut, contexte, sortie d'erreur) en conséquence. Appelée
    /// en direct par `RunAgent` en toute fin de `Job`, pas via un évènement.
    pub async fn report_agent_run(&self, agent_id: AgentId, response: AgentResponse) -> Result<(), SessionError> {
        let catalog = self.select_catalog(agent_id.session_id())?;
        let request = SessionReportAgentRunRequest { agent_id, response };

        self.rpc.invoke::<ReportAgentRun>(request, [catalog]).await?.map_err(SessionError::Server)
    }

    /// Persiste l'attente d'une réponse pour chacun de `tools_calls` sur le
    /// frame de `agent_id` — voir [`crate::session::server::report_tool_dispatch`].
    /// Appelée par `session::worker::run_turns` *avant* de déclencher les
    /// jobs `ToolExecution` correspondants.
    pub async fn report_tool_dispatch(&self, agent_id: AgentId, tools_calls: Vec<ToolCallId>) -> Result<(), SessionError> {
        let catalog = self.select_catalog(agent_id.session_id())?;
        let request = SessionReportToolDispatchRequest { agent_id, tools_calls };

        self.rpc.invoke::<ReportToolDispatch>(request, [catalog]).await?.map_err(SessionError::Server)
    }

    /// Rapporte le résultat de l'appel de tool `tool_call_id` pour le frame
    /// de `agent_id` — voir [`crate::session::server::report_tool_execution`],
    /// qui l'injecte dans le contexte du frame appelant une fois tous les
    /// tools attendus répondus. Appelée en direct par
    /// `tools::worker::ToolExecution` en toute fin de `Job`.
    pub async fn report_tool_execution(&self, agent_id: AgentId, tool_call_id: ToolCallId, result: ToolCallResult) -> Result<(), SessionError> {
        let catalog = self.select_catalog(agent_id.session_id())?;
        let request = SessionReportToolExecutionRequest { agent_id, tool_call_id, result };

        self.rpc.invoke::<ReportToolExecution>(request, [catalog]).await?.map_err(SessionError::Server)
    }

    /// Ajoute une ligne au journal d'évènements de `session_id`.
    pub async fn append_log(&self, session_id: SessionId, line: impl Into<String>) -> Result<(), SessionError> {
        let catalog = self.select_catalog(session_id)?;
        let request = SessionAppendLogRequest { session_id, line: line.into() };

        self.rpc.invoke::<AppendLog>(request, [catalog]).await?.map_err(SessionError::Server)
    }

    /// Ajoute `text` à la suite du [`SessionLog`](crate::session::SessionLog)
    /// identifié par `log_id` dans `session_id`, ou le crée s'il n'existe pas
    /// encore — voir [`crate::session::server::insert_in_log`].
    pub async fn insert_in_log(&self, session_id: SessionId, log_id: SessionLogId, text: impl Into<String>) -> Result<(), SessionError> {
        let catalog = self.select_catalog(session_id)?;
        let request = SessionInsertInLogRequest { session_id, log_id, text: text.into() };

        self.rpc.invoke::<InsertInLog>(request, [catalog]).await?.map_err(SessionError::Server)
    }

    /// Évalue `path` (JSONPath) contre `Session::vars` de `session_id` et
    /// renvoie les valeurs trouvées — voir [`SessionVarsQueryRequest`].
    pub async fn query_vars(&self, session_id: SessionId, path: impl Into<String>) -> Result<Vec<Value>, SessionError> {
        let catalog = self.select_catalog(session_id)?;
        let request = SessionVarsQueryRequest { session_id, path: path.into() };

        self.rpc.invoke::<QueryVars>(request, [catalog]).await?.map_err(SessionError::Server)
    }

    /// Remplace, dans `Session::vars` de `session_id`, chaque nœud
    /// correspondant à `path` (JSONPath) par `value` — voir
    /// [`SessionVarsPatchRequest`].
    pub async fn patch_vars(&self, session_id: SessionId, path: impl Into<String>, value: Value) -> Result<(), SessionError> {
        let catalog = self.select_catalog(session_id)?;
        let request = SessionVarsPatchRequest { session_id, path: path.into(), value };

        self.rpc.invoke::<PatchVars>(request, [catalog]).await?.map_err(SessionError::Server)
    }

    /// Retire, dans `Session::vars` de `session_id`, chaque nœud
    /// correspondant à `path` (JSONPath) — voir [`SessionVarsRemoveRequest`].
    pub async fn remove_vars(&self, session_id: SessionId, path: impl Into<String>) -> Result<(), SessionError> {
        let catalog = self.select_catalog(session_id)?;
        let request = SessionVarsRemoveRequest { session_id, path: path.into() };

        self.rpc.invoke::<RemoveVars>(request, [catalog]).await?.map_err(SessionError::Server)
    }

    /// Pousse un nouveau [`GraphFrame`] et fait passer `agent_id` en
    /// [`crate::agent::status::YieldStatus::WaitingGraph`] — voir
    /// [`crate::session::server::push_graph`].
    pub async fn push_graph(&self, agent_id: AgentId, graph_id: GraphFrameId, graph: StateGraph) -> Result<(), SessionError> {
        let catalog = self.select_catalog(agent_id.session_id())?;
        let request = SessionPushGraphRequest { agent_id, graph_id, graph };

        self.rpc.invoke::<PushGraph>(request, [catalog]).await?.map_err(SessionError::Server)
    }

    /// Persiste la progression d'un [`GraphFrame`] après un pas qui n'a ni
    /// conclu ni yieldé — voir [`crate::session::server::update_graph_step`].
    pub async fn update_graph_step(&self, graph_id: GraphFrameId, graph: GraphFrame) -> Result<(), SessionError> {
        let catalog = self.select_catalog(graph_id.session_id())?;
        let request = SessionUpdateGraphStepRequest { graph_id, graph };

        self.rpc.invoke::<UpdateGraphStep>(request, [catalog]).await?.map_err(SessionError::Server)
    }

    /// Persiste l'attente d'un enfant `Agent` spawné par un curseur de
    /// graphe, avant de soumettre son Job `RunAgent` — voir
    /// [`crate::session::server::report_graph_dispatch`].
    pub async fn report_graph_dispatch(&self, graph_id: GraphFrameId, graph: GraphFrame, spawn_agent: AgentFrame) -> Result<(), SessionError> {
        let catalog = self.select_catalog(graph_id.session_id())?;
        let request = SessionReportGraphDispatchRequest { graph_id, graph, spawn_agent };

        self.rpc.invoke::<ReportGraphDispatch>(request, [catalog]).await?.map_err(SessionError::Server)
    }

    /// Rapporte l'issue finale d'un [`GraphFrame`] — voir
    /// [`crate::session::server::report_graph_run`].
    pub async fn report_graph_run(&self, graph_id: GraphFrameId, response: GraphResponse) -> Result<(), SessionError> {
        let catalog = self.select_catalog(graph_id.session_id())?;
        let request = SessionReportGraphRunRequest { graph_id, response };

        self.rpc.invoke::<ReportGraphRun>(request, [catalog]).await?.map_err(SessionError::Server)
    }

    /// Crée une nouvelle [`crate::state_graph::orchestration::OrchestrationFrame`]
    /// et ses enfants — voir [`crate::session::server::push_orchestration`].
    pub async fn push_orchestration(
        &self,
        session_id: SessionId,
        orchestration_id: OrchestrationFrameId,
        owner: Waiter,
        owner_graph_update: Option<GraphFrame>,
        strategy: OrchestrationStrategy,
        children: Vec<ResolvedChildTask>,
    ) -> Result<(), SessionError> {
        let catalog = self.select_catalog(session_id)?;
        let request = SessionPushOrchestrationRequest { orchestration_id, owner, owner_graph_update, strategy, children };

        self.rpc.invoke::<PushOrchestration>(request, [catalog]).await?.map_err(SessionError::Server)
    }

    /// Pousse un nouveau [`crate::state_graph::hitl::HitlFrame`] et fait
    /// passer `owner` en [`crate::agent::status::YieldStatus::WaitingHitl`] —
    /// voir [`crate::session::server::push_hitl`].
    pub async fn push_hitl(&self, hitl_id: HitlFrameId, owner: Waiter, questions: Vec<Question>, owner_graph_update: Option<GraphFrame>) -> Result<(), SessionError> {
        let catalog = self.select_catalog(hitl_id.session_id())?;
        let request = SessionPushHitlRequest { hitl_id, owner, questions, owner_graph_update };

        self.rpc.invoke::<PushHitl>(request, [catalog]).await?.map_err(SessionError::Server)
    }

    /// Rapporte une réponse humaine pour le
    /// [`crate::state_graph::hitl::HitlFrame`] `hitl_id`, ou — si `None` —
    /// pour l'unique `AgentFrame` de `session_id` en attente (input spontané) —
    /// voir [`crate::session::server::report_user_input`]. Renvoie le
    /// [`HitlFrameId`] effectivement résolu.
    pub async fn report_user_input(&self, session_id: SessionId, hitl_id: Option<HitlFrameId>, answers: HashMap<String, Answer>) -> Result<HitlFrameId, SessionError> {
        let catalog = self.select_catalog(session_id)?;
        let request = SessionReportUserInputRequest { session_id, hitl_id, answers };

        self.rpc.invoke::<ReportUserInput>(request, [catalog]).await?.map_err(SessionError::Server)
    }

    /// Sélection déterministe d'un catalogue.
    fn select_catalog(&self, id: impl AsRef<[u8]>) -> Result<PeerId, SessionError> {
        use SessionError::NoCatalogAvailable;
        self.bootstrap.select_peer(NS_SESSION, &id).ok_or(NoCatalogAvailable)
    }
}
