use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use crate::{agent::AgentId, state_graph::{frame::GraphFrameId, hitl::HitlFrameId, orchestration::OrchestrationFrameId}, tools::ToolCallId};


#[derive(Debug, Default, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum AgentStatus {
    #[default]
    Initial,
    Paused,
    Running,
    Failed,
    Yielding(YieldStatus),
    Finished
}

/// Raison pour laquelle un run d'agent s'est arrêté sans conclure (voir
/// [`AgentStatus::Yielding`] côté frame, et `job::JobState::Yielded` côté
/// job — même valeur, portée par les deux : le job se termine sur ce yield
/// (voir `network::worker::mod::RunOutcome`), le frame en garde la trace
/// pour qui observe la session).
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum YieldStatus {
    /// En attente de la réponse à un appel de tool en cours. Couvre aussi
    /// une question posée via [`crate::hitl::ASK_HUMAN_TOOL`] : `tool_call_id`
    /// est alors réutilisé comme `HumanInputRequest::id` (voir
    /// `crate::hitl::client::HitlClient::ask`), ce qui permet à
    /// `network::cp::mod` de retrouver l'agent concerné dès qu'une
    /// `HumanInputAnswer` correspondante est gossipée.
    WaitingToolReply {
        tools_calls: Vec<ToolCallId>,
        tools_outputs: HashMap<ToolCallId, serde_json::Value>
    },
    /// En attente que des agents enfants terminent — soit un fan-out ad-hoc
    /// direct entre deux [`AgentFrame`](crate::agent::frame::AgentFrame),
    /// soit un [`Cursor`](crate::state_graph::Cursor) de `GraphFrame` en
    /// attente de l'enfant qu'il a spawné pour un nœud
    /// [`Executable::Agent`](crate::state_graph::executable::Executable::Agent)
    /// (voir `state_graph::StateGraph::execute_cursor`) — voir
    /// `session::server::report_agent_run`, qui détecte leur complétion et
    /// resoumet un job une fois tous réunis. `agents` sert à
    /// la fois de liste d'attente et de compteur restant : chaque enfant qui
    /// termine est retiré de la liste (son résultat étant déjà injecté dans
    /// le `Context` du frame à ce moment-là, voir `push_child_result_into_context`),
    /// le frame ne repasse `Running` que lorsqu'elle est vide. Pas de champ
    /// séparé pour les résultats déjà reçus : contrairement à
    /// `WaitingToolReply::tools_outputs` (dont l'appelant a besoin de tous
    /// les résultats groupés pour reprendre en une fois), ici chaque
    /// résultat est consommé immédiatement à son arrivée.
    WaitingAgents {
        agents: Vec<AgentId>,
    },
    /// En attente qu'un [`GraphFrame`](crate::state_graph::frame::GraphFrame)
    /// poussé par cet agent (voir `system/push-mode`) conclue — voir
    /// `session::server::report_graph_run`, qui détecte sa complétion et
    /// resoumet un job pour cet agent une fois débloqué. Même sémantique de
    /// satellite que [`Self::WaitingAgents`], mais pour un `GraphFrame`
    /// plutôt qu'un autre [`AgentFrame`](crate::agent::frame::AgentFrame).
    WaitingGraph {
        graph: GraphFrameId,
    },
    /// En attente qu'une [`OrchestrationFrame`](crate::state_graph::orchestration::OrchestrationFrame)
    /// poussée par cet agent conclue — voir `session::server::report_agent_run`/
    /// `report_graph_run`, qui scannent aussi les orchestrations en
    /// attente lors du réveil d'un enfant.
    WaitingOrchestration {
        orchestration: OrchestrationFrameId,
    },
    /// Budget d'exécution (ex: nombre de tours) épuisé — contrairement aux
    /// autres variantes, n'attend rien d'externe : peut être repris dès que
    /// `session::server` observe ce yield.
    RunExhausted,
    /// En attente de la réponse à un [`crate::state_graph::hitl::HitlFrame`]
    /// (formulaire humain, voir `crate::tools::builtin::ASK_USER_INPUT_TOOL`
    /// et `session::server::report_user_input`) — même satellite que
    /// [`Self::WaitingGraph`]/[`Self::WaitingOrchestration`], mais porté soit
    /// par un [`AgentFrame`](crate::agent::frame::AgentFrame) (le tool a été
    /// appelé directement par un agent), soit par un curseur de
    /// [`GraphFrame`](crate::state_graph::frame::GraphFrame) (nœud
    /// [`Executable::AskUserInput`](crate::state_graph::executable::Executable::AskUserInput)) —
    /// voir `HitlFrame::owner`. Un input *spontané* (sans `hitl_id` explicite,
    /// voir `session::rpc::ReportUserInput`) ne résout jamais cette variante
    /// quand elle appartient à un curseur de graphe : seul un `AgentFrame` en
    /// `WaitingHitl` peut être ciblé implicitement, pour ne pas laisser un
    /// message libre satisfaire silencieusement un formulaire structuré que
    /// le graphe attend précisément.
    WaitingHitl { hitl: HitlFrameId },
}

/// Issue d'un run d'agent, rapportée par `session::worker::RunAgent` à
/// `SessionServer` via `session::rpc::ReportAgentRun` en toute fin de `Job`
/// — voir la doc de ce dernier pour la raison d'une RPC directe et
/// synchrone plutôt qu'un évènement gossip. Type propre à l'agent (par
/// opposition à `network::worker::JobResult`, générique à tout `Job`) : pas
/// de dépendance vers `model::ModelResponse` ici, la conversion se fait côté
/// appelant qui connaît déjà les deux types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AgentResponse {
    Finished { text: Option<String> },
    Failed { error: String },
}