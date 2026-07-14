use serde::{Deserialize, Serialize};
use crate::id::ID;

use crate::agent::GlobalAgentId;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum YieldStatus {
    /// En attente de la réponse à un appel de tool en cours. Couvre aussi
    /// une question posée via [`crate::hitl::ASK_HUMAN_TOOL`] : `tool_call_id`
    /// est alors réutilisé comme `HumanInputRequest::id` (voir
    /// `crate::hitl::client::HitlClient::ask`), ce qui permet à
    /// `network::cp::mod` de retrouver l'agent concerné dès qu'une
    /// `HumanInputAnswer` correspondante est gossipée.
    WaitingToolReply {
        tool_call_id: ID
    },
    /// En attente que les agents enfants d'une orchestration (voir
    /// `crate::mode::orchestration::Orchestration`) terminent — voir
    /// `network::cp::mod`, qui détecte leur complétion (`JobState::Completed`)
    /// et resoumet un job pour cet agent une fois tous réunis.
    WaitingChildren {
        children: Vec<GlobalAgentId>
    },
    /// Budget d'exécution (ex: nombre de tours) épuisé — contrairement aux
    /// deux autres variantes, n'attend rien d'externe : peut être repris dès
    /// que `network::cp::mod` observe ce yield (voir `on_job_terminated`).
    RunExhausted
}