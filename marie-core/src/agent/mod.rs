use serde::{Deserialize, Serialize};
use crate::id::ID;

use crate::{
    agent::{context::{Context, ContextEntry}, frame::AgentFrame, role::Role, status::{AgentStatus, YieldStatus}},
    hitl::{ASK_HUMAN_TOOL, Question, client::HitlClient},
    model::{self, ModelClient},
    session::client::SessionClient,
    tools::{ToolCall, client::ToolClient},
};

pub mod status;
pub mod frame;
pub mod context;
pub mod role;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GlobalAgentId(ID, ID);

impl GlobalAgentId {
    pub fn new(session_id: ID, local_id: ID) -> Self {
        Self(session_id, local_id)
    }

    pub fn session_id(&self) -> ID {
        self.0
    }

    pub fn local_id(&self) -> ID {
        self.1
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Warmup operations executed juste after spawn and before running.
pub enum AgentWarmup {
    WriteContext(Context),
    ExecuteTool(ToolCall)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentSpawnRequest {
    session_id: ID,
    agent_id: ID,
    warmup: Vec<AgentWarmup>,
}

/// Nombre maximal d'aller-retours modèle/tools qu'un run borné en mode
/// `mode::SessionMode::Simple` peut effectuer avant de céder la main — même
/// rôle que `network::worker::mod::MAX_STATE_GRAPH_STEPS_PER_RUN` pour un
/// `StateGraph` : sans cette borne, un agent qui enchaîne les tool calls
/// sans jamais conclure monopoliserait indéfiniment ce worker.
const MAX_TURNS_PER_RUN: u32 = 16;

/// Issue d'un run borné d'un agent en mode `mode::SessionMode::Simple` —
/// mêmes deux issues terminales que `network::worker::mod::RunOutcome` pour
/// un `StateGraph`, dupliquées ici plutôt que partagées (`agent` ne dépend
/// pas de `network::worker`) : soit le modèle a répondu sans plus rien
/// attendre, soit le run s'arrête sans conclure (voir [`YieldStatus`]).
#[derive(Debug)]
pub enum RunOutcome {
    Completed { text: Option<String> },
    Yielded { reason: YieldStatus },
}

/// Fait avancer l'agent `frame` d'un run borné modèle/tools, jusqu'à ce
/// qu'il conclue (réponse sans tool call), doive attendre (voir
/// [`ASK_HUMAN_TOOL`]) ou épuise son budget de tours ([`MAX_TURNS_PER_RUN`]) —
/// voir [`RunOutcome`]. `frame` est mis à jour au fil de l'eau (contexte,
/// statut) et chaque mutation est persistée via `sessions` (voir
/// [`SessionClient::push_context_entry`]/[`SessionClient::set_frame_status`])
/// pour qu'une réassignation à un autre worker en cours de run ne perde pas
/// la progression déjà accomplie — même logique que
/// `network::worker::mod::drive_state_graph` pour un `StateGraph`.
///
/// Cas particulier de [`ASK_HUMAN_TOOL`] : contrairement à un tool ordinaire
/// (relayé via [`ToolClient::call`], borné par le timeout RPC), la réponse
/// humaine n'a pas de limite de temps (voir le module [`crate::hitl`]) —
/// l'attendre ici bloquerait ce worker le temps de la réponse, potentiellement
/// des heures. Le formulaire est donc publié sans attendre (voir
/// [`HitlClient::ask_and_forget`]) et le run yielde immédiatement sur
/// [`YieldStatus::WaitingToolReply`] ; c'est au control plane de reprendre
/// l'agent une fois la réponse arrivée (voir
/// `network::cp::mod::resume_after_hitl_answer`).
///
/// Point ouvert, hors de la portée de cette fonction : comment le run repris
/// retrouve le contenu de cette réponse une fois arrivée — aucun stockage
/// des [`crate::hitl::HumanInputAnswer`] n'existe encore, seule leur
/// corrélation à l'agent en attente est câblée aujourd'hui.
pub async fn run(
    frame: &mut AgentFrame,
    model: &ModelClient,
    tools: &ToolClient,
    hitl: &HitlClient,
    sessions: &SessionClient,
) -> Result<RunOutcome, anyhow::Error> {
    let agent_id = GlobalAgentId::new(frame.session_id, frame.id);
    let declaration = model.get(frame.model_id.clone()).await?;

    let mut signatures = Vec::with_capacity(frame.allowed_tools.len());
    for name in &frame.allowed_tools {
        signatures.push(tools.get(name.as_str()).await?.signature);
    }

    set_status(frame, sessions, AgentStatus::Running).await?;

    for _ in 0..MAX_TURNS_PER_RUN {
        let input = frame.context.iter().map(|entry| format!("{}: {}", entry.role, entry.content)).collect::<Vec<_>>().join("\n");
        let response = model::execute(declaration.clone(), &signatures, input).await?;

        if let Some(text) = &response.text {
            push_context(frame, sessions, ContextEntry { role: Role::Assistant, content: text.clone() }).await?;
        }

        if response.tool_calls.is_empty() {
            let outcome = RunOutcome::Completed { text: response.text };
            set_status(frame, sessions, AgentStatus::Finished).await?;
            return Ok(outcome);
        }

        if let Some(index) = response.tool_calls.iter().position(|call| call.name == ASK_HUMAN_TOOL) {
            let ask_human = &response.tool_calls[index];
            let questions = parse_ask_human_questions(ask_human)?;
            let tool_call_id = ask_human.id;

            hitl.ask_and_forget(tool_call_id, agent_id, questions)?;

            let reason = YieldStatus::WaitingToolReply { tool_call_id };
            set_status(frame, sessions, AgentStatus::Yielding(reason.clone())).await?;
            return Ok(RunOutcome::Yielded { reason });
        }

        for call in response.tool_calls {
            let content = match tools.call(agent_id, call.clone()).await {
                Ok(output) => output.map(|value| value.to_string()).unwrap_or_default(),
                Err(error) => format!("erreur: {error}"),
            };
            push_context(frame, sessions, ContextEntry { role: Role::Tool, content }).await?;
        }
    }

    let reason = YieldStatus::RunExhausted;
    set_status(frame, sessions, AgentStatus::Yielding(reason.clone())).await?;
    Ok(RunOutcome::Yielded { reason })
}

/// Charge utile attendue des arguments d'un appel à [`ASK_HUMAN_TOOL`] (voir
/// `hitl::tool_declaration`) — seul `questions` nous intéresse ici, le reste
/// du formulaire (validation des réponses, etc.) est géré par
/// [`crate::hitl`].
#[derive(Debug, Deserialize)]
struct AskHumanArgs {
    questions: Vec<Question>,
}

fn parse_ask_human_questions(call: &ToolCall) -> Result<Vec<Question>, anyhow::Error> {
    let params = call.parameters.clone().ok_or_else(|| anyhow::anyhow!("appel de {ASK_HUMAN_TOOL} sans arguments"))?;
    let args: AskHumanArgs = serde_json::from_value(params)?;
    Ok(args.questions)
}

/// Ajoute `entry` au contexte de `frame`, en persistant d'abord le delta CRDT
/// (voir [`SessionClient::push_context_entry`]) avant de mettre à jour la
/// copie locale — pour qu'un échec réseau n'avance jamais la copie locale
/// sans que la persistance ait réussi.
async fn push_context(frame: &mut AgentFrame, sessions: &SessionClient, entry: ContextEntry) -> Result<(), anyhow::Error> {
    sessions.push_context_entry(frame.session_id, frame.id, entry.clone()).await?;
    frame.context.push(entry);
    Ok(())
}

/// Persiste le nouveau statut de `frame` (voir [`SessionClient::set_frame_status`])
/// avant de mettre à jour la copie locale, même ordre que [`push_context`].
async fn set_status(frame: &mut AgentFrame, sessions: &SessionClient, status: AgentStatus) -> Result<(), anyhow::Error> {
    sessions.set_frame_status(frame.session_id, frame.id, status.clone()).await?;
    frame.status = status;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ask_human_call(params: Option<serde_json::Value>) -> ToolCall {
        ToolCall { id: crate::id::generate_id(), name: ASK_HUMAN_TOOL.to_string(), parameters: params }
    }

    #[test]
    fn test_parse_ask_human_questions_extracts_questions() {
        let call = ask_human_call(Some(serde_json::json!({
            "questions": [
                { "key": "root_cause", "label": "Cause racine ?", "kind": "short_text" }
            ]
        })));

        let questions = parse_ask_human_questions(&call).unwrap();
        assert_eq!(questions, vec![Question::short_text("root_cause", "Cause racine ?")]);
    }

    #[test]
    fn test_parse_ask_human_questions_rejects_missing_arguments() {
        let call = ask_human_call(None);
        assert!(parse_ask_human_questions(&call).is_err());
    }

    #[test]
    fn test_parse_ask_human_questions_rejects_malformed_arguments() {
        let call = ask_human_call(Some(serde_json::json!({ "not_questions": [] })));
        assert!(parse_ask_human_questions(&call).is_err());
    }
}
