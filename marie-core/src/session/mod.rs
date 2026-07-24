#[cfg(feature = "catalog")]
pub mod catalog;
pub mod client;
#[cfg(feature = "catalog")]
pub mod layers;
// `server::SessionCommand` est référencé directement par les RPC mutantes de
// `rpc.rs` (voir ex. `InsertSession`), lui-même requis par `client::SessionClient`
// (base commune à toutes les features) — impossible de gater ce module
// derrière `catalog` sans casser un build client seul, même principe que
// `network::worker::server` (voir sa doc).
pub mod server;
pub mod model;
pub mod rpc;
pub mod worker;
pub mod store;

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::agent::AgentId;
use crate::agent::status::{AgentResponse, AgentStatus};
use crate::hitl::{Answer, Question};
use crate::pubsub::PubSubMessage;
use crate::state::StateLocation;
use crate::state_graph::{
    StateGraph,
    executable::{OrchestrationStrategy, ResolvedChildTask},
    frame::{GraphFrame, GraphFrameId, GraphResponse},
    hitl::{HitlFrameId, HitlFrameStatus},
    orchestration::{OrchestrationFrameId, Waiter},
};
use crate::tools::{ToolCallId, ToolCallResult};

pub use model::{Session, SessionId, SessionLog, SessionLogId};
pub use rpc::{
    AppendLog, GetSession, InsertInLog, InsertSession, ListSession, PatchVars, PushGraph, PushHitl, PushOrchestration, QueryState, RemoveSession,
    ReportAgentRun, ReportGraphDispatch, ReportGraphRun, ReportUserInput, UpdateGraphStep, UpdateSession,
};

pub const NS_SESSION: &str = "/marie/ns/sessions";

/// Évènements de cycle de vie d'une session — voir la doc de
/// [`crate::network::worker::WorkerEvent`] pour la justification du schéma
/// (Layer/gossip plutôt qu'un canal en mémoire), repris ici pour `session::`
/// à ceci près que le topic est dédié à chaque session (voir [`Self::topic`])
/// plutôt qu'unique et global : contrairement aux jobs (`WorkerEvent`) ou
/// aux tools (`ToolEvent`), un abonné n'est en général intéressé que par
/// UNE session précise (ex. une passerelle qui relaie les évènements d'une
/// session donnée à un client WebSocket) — un topic par session lui évite de
/// recevoir puis filtrer le bruit de toutes les autres. Seul
/// [`server::SessionServerActor`] en est l'émetteur : chaque mutation
/// réussie du catalogue (voir [`server::SessionCommand`]) produit
/// exactement l'évènement correspondant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SessionEvent {
    Created { id: SessionId },
    Updated { id: SessionId },
    Removed { id: SessionId },
    FrameStatusChanged { session_id: SessionId, agent_id: AgentId, status: AgentStatus },
    /// Progression d'un [`crate::state_graph::frame::GraphFrame`] —
    /// `current_node` (le nœud du curseur prêt à avancer, s'il y en a un)
    /// est inclus directement pour observer la progression du graphe sans
    /// refaire un `GetSession` à chaque évènement.
    GraphStatusChanged { session_id: SessionId, graph_id: GraphFrameId, status: AgentStatus, current_node: Option<String> },
    /// Progression d'une [`crate::state_graph::orchestration::OrchestrationFrame`] —
    /// `pending` : nombre d'enfants encore attendus.
    OrchestrationStatusChanged { session_id: SessionId, orchestration_id: OrchestrationFrameId, status: AgentStatus, pending: usize },
    /// Cycle de vie d'un [`crate::state_graph::hitl::HitlFrame`] — émis à
    /// la fois par [`rpc::PushHitl`] (`status: Pending`) et
    /// [`rpc::ReportUserInput`] (`status: Answered`), observable
    /// indépendamment de l'`AgentFrame`/`GraphFrame` propriétaire (voir
    /// [`crate::state_graph::hitl::HitlFrame::owner`]).
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
    /// Racine commune à tous les topics de session, dédiés comme global —
    /// voir [`Self::is`].
    pub const TOPIC_PREFIX: &str = "marie/sessions";

    /// Topic global, commun à toutes les sessions (voir [`Self::global_topic`])
    /// — conservé en plus du topic dédié (voir [`Self::topic_prefix`]) pour
    /// un abonné qui veut observer le cycle de vie de toutes les sessions
    /// sans connaître leurs identifiants à l'avance (ex. un tableau de bord).
    pub const GLOBAL_TOPIC_PREFIX: &str = "marie/sessions/events";

    /// Session concernée par cet évènement — sert à calculer le topic dédié
    /// (voir [`Self::topic_prefix`]/[`Self::topic`]).
    pub fn session_id(&self) -> SessionId {
        match self {
            SessionEvent::Created { id } | SessionEvent::Updated { id } | SessionEvent::Removed { id } => *id,
            SessionEvent::FrameStatusChanged { session_id, .. }
            | SessionEvent::GraphStatusChanged { session_id, .. }
            | SessionEvent::OrchestrationStatusChanged { session_id, .. }
            | SessionEvent::HitlStatusChanged { session_id, .. }
            | SessionEvent::LogAppended { session_id, .. }
            | SessionEvent::VarsPatched { session_id } => *session_id,
        }
    }

    /// Suffixe identifiant le type d'évènement, commun à [`Self::topic`] et
    /// [`Self::global_topic`].
    fn kind(&self) -> &'static str {
        match self {
            SessionEvent::Created { .. } => "created",
            SessionEvent::Updated { .. } => "updated",
            SessionEvent::Removed { .. } => "removed",
            SessionEvent::FrameStatusChanged { .. } => "frame-status-changed",
            SessionEvent::GraphStatusChanged { .. } => "graph-status-changed",
            SessionEvent::OrchestrationStatusChanged { .. } => "orchestration-status-changed",
            SessionEvent::HitlStatusChanged { .. } => "hitl-status-changed",
            SessionEvent::LogAppended { .. } => "log-appended",
            SessionEvent::VarsPatched { .. } => "vars-patched",
        }
    }

    /// Topic dédié à la session de cet évènement (`marie/sessions/{id}/`,
    /// suffixé par le type d'évènement dans [`Self::topic`]) — un abonné
    /// n'ayant besoin que d'une session précise s'abonne uniquement à ce
    /// préfixe-ci plutôt qu'au flux de toutes les sessions.
    pub fn topic_prefix(&self) -> String {
        format!("{0}/{1}", Self::TOPIC_PREFIX, self.session_id())
    }

    /// Topic effectivement publié pour cet évènement, dédié à sa session —
    /// voir [`Self::topic_prefix`]. Publié en plus de, et non à la place de,
    /// [`Self::global_topic`] (voir [`layers::SessionEventLayer`]).
    pub fn topic(&self) -> String {
        format!("{0}/{1}", self.topic_prefix(), self.kind())
    }

    /// Topic global (sans l'identifiant de session), sous
    /// [`Self::GLOBAL_TOPIC_PREFIX`] — voir [`Self::topic`] pour le pendant
    /// dédié à la session.
    pub fn global_topic(&self) -> String {
        format!("{0}/{1}", Self::GLOBAL_TOPIC_PREFIX, self.kind())
    }

    /// Reconnaît tout topic de session, dédié ou global — voir
    /// [`Self::topic_prefix`]/[`Self::GLOBAL_TOPIC_PREFIX`] pour filtrer plus
    /// précisément.
    pub fn is(msg: &PubSubMessage) -> bool {
        msg.topic.starts_with(Self::TOPIC_PREFIX)
    }

    /// Tous les suffixes de type d'évènement (voir [`Self::kind`]), dans le
    /// même ordre que les variantes de l'enum — permet à un abonné externe
    /// (ex. `marie_gateway::MarieGatewayActor`) de s'abonner à
    /// [`Self::global_topic`] pour chaque type d'évènement sans dupliquer à
    /// la main le match (privé) de [`Self::kind`]. À tenir à jour
    /// manuellement en même temps que [`Self::kind`] si une variante est
    /// ajoutée/retirée — rien ne garantit la synchronisation à la
    /// compilation, voir le test `all_global_topics_has_one_per_kind`.
    pub const KINDS: [&'static str; 9] = [
        "created",
        "updated",
        "removed",
        "frame-status-changed",
        "graph-status-changed",
        "orchestration-status-changed",
        "hitl-status-changed",
        "log-appended",
        "vars-patched",
    ];

    /// Tous les topics globaux (un par type d'évènement, voir
    /// [`Self::KINDS`]/[`Self::global_topic`]).
    pub fn all_global_topics() -> Vec<String> {
        Self::KINDS.iter().map(|kind| format!("{}/{kind}", Self::GLOBAL_TOPIC_PREFIX)).collect()
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
#[cfg(feature = "catalog")]
pub fn build_server(net: &crate::network::Network args: server::SessionServerArgs) -> server::SessionServer {
    use crate::layer::{IntoService as _, LayerExt as _};
    use crate::pubsub::layers::PubSubLayer;

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
pub struct SessionStateQueryRequest {
    pub location: StateLocation,
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

/// Charge utile de [`rpc::RemoveVars`] : retire, dans `Session::vars` traité
/// comme un document JSON unique (voir [`SessionVarsQueryRequest`]), chaque
/// nœud correspondant à `path` — même sémantique que
/// [`crate::workspace::WorkspaceVarsRemoveRequest`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionVarsRemoveRequest {
    pub session_id: SessionId,
    pub path: String,
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
/// [`crate::state_graph::orchestration::OrchestrationFrame`] et ses
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
/// [`crate::state_graph::hitl::HitlFrame`] identifié par `hitl_id` — voir
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
/// [`crate::state_graph::hitl::HitlFrame`] `hitl_id`, ou — si `None` — au
/// seul `AgentFrame` de `session_id` actuellement `Yielding(WaitingHitl)`
/// (input spontané, voir [`server::report_user_input`] pour la résolution et
/// ses cas d'erreur).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionReportUserInputRequest {
    pub session_id: SessionId,
    pub hitl_id: Option<HitlFrameId>,
    pub answers: HashMap<String, Answer>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_global_topics_has_one_per_kind() {
        let topics = SessionEvent::all_global_topics();
        assert_eq!(topics.len(), SessionEvent::KINDS.len());
        assert!(topics.iter().all(|t| t.starts_with(SessionEvent::GLOBAL_TOPIC_PREFIX)));
    }
}
