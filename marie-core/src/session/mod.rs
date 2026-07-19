pub mod catalog;
pub mod client;
pub mod layers;
pub mod server;
pub mod model;
pub mod rpc;
pub mod state;
pub mod worker;

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::agent::AgentId;
use crate::agent::status::{AgentResponse, AgentStatus};
use crate::hitl::{Answer, Question};
use crate::layer::{IntoService as _, LayerExt as _};
use crate::network::actor::Network;
use crate::pubsub::{PubSubMessage, layers::PubSubLayer};
use crate::session::state::{
    StateGraph,
    executable::{OrchestrationStrategy, ResolvedChildTask},
    frame::{GraphFrame, GraphFrameId, GraphResponse},
    hitl::{HitlFrameId, HitlFrameStatus},
    orchestration::{OrchestrationFrameId, Waiter},
};
use crate::tools::{ToolCallId, ToolCallResult};

pub use model::{Session, SessionId, SessionLog, SessionLogId};
pub use rpc::{
    AppendLog, GetSession, InsertInLog, InsertSession, ListSession, PatchVars, PushGraph, PushHitl, PushOrchestration, QueryVars, RemoveSession,
    ReportAgentRun, ReportGraphDispatch, ReportGraphRun, ReportUserInput, UpdateGraphStep, UpdateSession,
};

pub const NS_SESSION: &str = "/marie/ns/sessions";

/// Évènements de cycle de vie d'une session, diffusés sur
/// [`SessionEvent::TOPIC_PREFIX`] — voir la doc de [`crate::network::worker::WorkerEvent`]
/// pour la justification du schéma (Layer/gossip plutôt qu'un canal en
/// mémoire), reproduit ici à l'identique pour `session::`. Seul
/// [`server::SessionServerActor`] en est l'émetteur : chaque mutation
/// réussie du catalogue (voir [`server::SessionCommand`]) produit
/// exactement l'évènement correspondant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SessionEvent {
    Created { id: SessionId },
    Updated { id: SessionId },
    Removed { id: SessionId },
    FrameStatusChanged { session_id: SessionId, agent_id: AgentId, status: AgentStatus },
    /// Progression d'un [`crate::session::state::frame::GraphFrame`] —
    /// `current_node` (le nœud du curseur prêt à avancer, s'il y en a un)
    /// est inclus directement pour observer la progression du graphe sans
    /// refaire un `GetSession` à chaque évènement.
    GraphStatusChanged { session_id: SessionId, graph_id: GraphFrameId, status: AgentStatus, current_node: Option<String> },
    /// Progression d'une [`crate::session::state::orchestration::OrchestrationFrame`] —
    /// `pending` : nombre d'enfants encore attendus.
    OrchestrationStatusChanged { session_id: SessionId, orchestration_id: OrchestrationFrameId, status: AgentStatus, pending: usize },
    /// Cycle de vie d'un [`crate::session::state::hitl::HitlFrame`] — émis à
    /// la fois par [`rpc::PushHitl`] (`status: Pending`) et
    /// [`rpc::ReportUserInput`] (`status: Answered`), observable
    /// indépendamment de l'`AgentFrame`/`GraphFrame` propriétaire (voir
    /// [`crate::session::state::hitl::HitlFrame::owner`]).
    HitlStatusChanged { session_id: SessionId, hitl_id: HitlFrameId, status: HitlFrameStatus },
    LogAppended { session_id: SessionId, log_id: SessionLogId, text: String },
    VarsPatched { session_id: SessionId },
}

#[derive(Debug, Error)]
pub enum SessionEventError {
    #[error("ce n'est pas un évènement de session")]
    NotSessionEvent,
}

impl SessionEvent {
    pub const TOPIC_PREFIX: &str = "marie/sessions/events";

    pub fn topic(&self) -> String {
        match self {
            SessionEvent::Created { .. } => format!("{0}/created", Self::TOPIC_PREFIX),
            SessionEvent::Updated { .. } => format!("{0}/updated", Self::TOPIC_PREFIX),
            SessionEvent::Removed { .. } => format!("{0}/removed", Self::TOPIC_PREFIX),
            SessionEvent::FrameStatusChanged { .. } => format!("{0}/frame-status-changed", Self::TOPIC_PREFIX),
            SessionEvent::GraphStatusChanged { .. } => format!("{0}/graph-status-changed", Self::TOPIC_PREFIX),
            SessionEvent::OrchestrationStatusChanged { .. } => format!("{0}/orchestration-status-changed", Self::TOPIC_PREFIX),
            SessionEvent::HitlStatusChanged { .. } => format!("{0}/hitl-status-changed", Self::TOPIC_PREFIX),
            SessionEvent::LogAppended { .. } => format!("{0}/log-appended", Self::TOPIC_PREFIX),
            SessionEvent::VarsPatched { .. } => format!("{0}/vars-patched", Self::TOPIC_PREFIX),
        }
    }

    pub fn is(msg: &PubSubMessage) -> bool {
        msg.topic.starts_with(Self::TOPIC_PREFIX)
    }
}

impl TryFrom<PubSubMessage> for SessionEvent {
    type Error = SessionEventError;

    fn try_from(value: PubSubMessage) -> Result<Self, Self::Error> {
        use SessionEventError::NotSessionEvent;

        if !Self::is(&value) { return Err(NotSessionEvent) };

        serde_json::from_slice(&value.payload).map_err(|_| NotSessionEvent)
    }
}

/// Construit un [`server::SessionServer`] en chaînant le transport réseau
/// brut (`NetworkCommand`/`NetworkEvent`) à travers `PubSubLayer` puis
/// [`layers::SessionEventLayer`] — mirroir de
/// [`crate::network::worker::build_server`].
pub fn build_server(net: &Network, args: server::SessionServerArgs) -> server::SessionServer {
    net.transport()
        .chain::<PubSubLayer, _>(())
        .chain::<layers::SessionEventLayer, _>(())
        .into_service(args)
}

/// Charge utile de [`rpc::ReportAgentRun`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionReportAgentRunRequest {
    pub agent_id: AgentId,
    pub response: AgentResponse,
}

/// Charge utile de [`rpc::ReportToolDispatch`] : persiste l'attente d'une
/// réponse pour chacun de `tools_calls` *avant* que ces appels ne soient
/// effectivement déclenchés (voir `session::worker::run_turns`) — sans ce
/// pré-enregistrement, un job `ToolExecution` particulièrement rapide
/// pourrait rapporter son résultat avant même que ce statut d'attente
/// n'existe, et son identifiant ne serait alors jamais retiré de la liste
/// (l'agent resterait bloqué indéfiniment).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionReportToolDispatchRequest {
    pub agent_id: AgentId,
    pub tools_calls: Vec<ToolCallId>,
}

/// Charge utile de [`rpc::ReportToolExecution`]. Appelée en direct par
/// `tools::worker::ToolExecution` en toute fin de `Job`, sur le même modèle
/// que [`SessionReportAgentRunRequest`] pour `RunAgent`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionReportToolExecutionRequest {
    pub agent_id: AgentId,
    pub tool_call_id: ToolCallId,
    pub result: ToolCallResult,
}

/// Charge utile de [`rpc::AppendLog`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionAppendLogRequest {
    pub session_id: SessionId,
    pub line: String,
}

/// Charge utile de [`rpc::InsertInLog`] : ajoute `text` à la suite du
/// [`SessionLog`] identifié par `log_id` (le crée s'il n'existe pas encore —
/// voir [`server::insert_in_log`]), contrairement à [`SessionAppendLogRequest`]
/// qui crée toujours une nouvelle entrée immuable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionInsertInLogRequest {
    pub session_id: SessionId,
    pub log_id: SessionLogId,
    pub text: String,
}

/// Charge utile de [`RPC_SESSION_VARS_QUERY`] : `path` est une expression
/// JSONPath (voir la crate `jsonpath_lib`), évaluée contre `Session::vars`
/// traité comme un unique document JSON (ses clés de premier niveau
/// devenant les champs de ce document, ex: `$.budget`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionVarsQueryRequest {
    pub session_id: SessionId,
    pub path: String,
}

/// Charge utile de [`RPC_SESSION_VARS_PATCH`] : remplace, dans
/// `Session::vars` traité comme un document JSON unique (voir
/// [`SessionVarsQueryRequest`]), chaque nœud correspondant à `path` par
/// `value`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionVarsPatchRequest {
    pub session_id: SessionId,
    pub path: String,
    pub value: Value,
}

/// Charge utile de [`rpc::PushGraph`] : `agent_id` pousse `graph`, un nouveau
/// [`GraphFrame`] identifié par `graph_id` — voir [`server::push_graph`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionPushGraphRequest {
    pub agent_id: AgentId,
    pub graph_id: GraphFrameId,
    pub graph: StateGraph,
}

/// Charge utile de [`rpc::UpdateGraphStep`] : remplace l'entrée
/// `session.graphs[graph_id]` par `graph` — voir [`server::update_graph_step`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionUpdateGraphStepRequest {
    pub graph_id: GraphFrameId,
    pub graph: GraphFrame,
}

/// Charge utile de [`rpc::ReportGraphDispatch`] : persiste `graph` (dont un
/// curseur vient de passer en attente de `spawn_agent`) et insère ce dernier
/// dans `Session::frames`, avant que son Job `RunAgent` ne soit soumis — voir
/// [`server::report_graph_dispatch`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionReportGraphDispatchRequest {
    pub graph_id: GraphFrameId,
    pub graph: GraphFrame,
    pub spawn_agent: crate::agent::frame::AgentFrame,
}

/// Charge utile de [`rpc::ReportGraphRun`] : rapporte l'issue finale d'un
/// [`GraphFrame`] — voir [`server::report_graph_run`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionReportGraphRunRequest {
    pub graph_id: GraphFrameId,
    pub response: GraphResponse,
}

/// Charge utile de [`rpc::PushOrchestration`] : crée une nouvelle
/// [`crate::session::state::orchestration::OrchestrationFrame`] et ses
/// enfants — voir [`server::push_orchestration`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionPushOrchestrationRequest {
    pub orchestration_id: OrchestrationFrameId,
    pub owner: Waiter,
    pub owner_graph_update: Option<GraphFrame>,
    pub strategy: OrchestrationStrategy,
    pub children: Vec<ResolvedChildTask>,
}

/// Charge utile de [`rpc::PushHitl`] : `owner` pousse un nouveau
/// [`crate::session::state::hitl::HitlFrame`] identifié par `hitl_id` — voir
/// [`server::push_hitl`]. `owner_graph_update`, si fourni, est la version
/// déjà mise à jour (curseur en `Yielding(WaitingHitl)`) du [`GraphFrame`]
/// appelant, sur le même modèle anti-course que
/// [`SessionPushOrchestrationRequest::owner_graph_update`] (absent quand
/// `owner` est un `AgentFrame`, la mutation se fait alors côté serveur).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionPushHitlRequest {
    pub hitl_id: HitlFrameId,
    pub owner: Waiter,
    pub questions: Vec<Question>,
    pub owner_graph_update: Option<GraphFrame>,
}

/// Charge utile de [`rpc::ReportUserInput`] : répond au
/// [`crate::session::state::hitl::HitlFrame`] `hitl_id`, ou — si `None` — au
/// seul `AgentFrame` de `session_id` actuellement `Yielding(WaitingHitl)`
/// (input spontané, voir [`server::report_user_input`] pour la résolution et
/// ses cas d'erreur).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionReportUserInputRequest {
    pub session_id: SessionId,
    pub hitl_id: Option<HitlFrameId>,
    pub answers: HashMap<String, Answer>,
}
