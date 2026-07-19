use async_trait::async_trait;
use futures::StreamExt as _;

use crate::{
    agent::{
        AgentId,
        frame::AgentFrame,
        context::{Context, ContextEntry},
        role::Role,
        status::AgentResponse,
    },
    hitl::Question,
    job::Job,
    model::{self, Model, ModelResponse, ModelStatus, client::ModelClient},
    network::{bootstrap::BootstrapClient, worker::JobContext},
    rpc::{RpcClient, Void},
    session::{SessionLogId, client::SessionClient, state::{hitl::HitlFrameId, orchestration::Waiter}},
    tools::{Tool, ToolCall, ToolCallId, builtin::ASK_USER_INPUT_TOOL, client::ToolClient},
};

pub struct RunAgent {
    rpc: RpcClient,
    bootstrap: BootstrapClient,
    sessions: SessionClient,
    models: ModelClient,
    tools: ToolClient
}

#[async_trait]
impl Job for RunAgent {
    const NAME: &'static str = "marie/sessions/run-agent";

    type Args = AgentFrame;
    type Return = Void;

    async fn execute(self, args: Self::Args, cx: JobContext) ->  Result<Self::Return, anyhow::Error>  {
        let tools: Vec<Tool> = self.tools
            .list()
            .await?
            .into_iter()
            .filter(|tool| args.allowed_tools.contains(&tool.name))
            .collect();

        let model = self.models.get(args.model).await?;

        let outcome = run_turns(args.id, model, &tools, args.context, &self.sessions, &self.tools).await;

        match outcome {
            Ok(TurnOutcome::Completed(model_response)) => {
                // RPC directe et synchrone (pas un évènement gossip) : le Job
                // ne se termine que lorsque SessionServer a effectivement
                // reçu et appliqué le résultat — résilience au churn, la
                // sélection déterministe du pair
                // (SessionClient::select_catalog, un hash ring) fait déjà le
                // travail de routage.
                self.sessions.report_agent_run(args.id, AgentResponse::Finished { text: model_response.text }).await?;
            }
            Ok(TurnOutcome::Yielded) => {
                // Rien à rapporter ici : `run_turns` a déjà persisté
                // l'attente via `SessionClient::report_tool_dispatch` avant
                // de déclencher les tools — le Job se termine normalement,
                // la reprise se fera via un nouveau Job `RunAgent` soumis
                // par `SessionServer` une fois tous les tools répondus (voir
                // `session::server::report_tool_execution`).
            }
            Err(error) => {
                self.sessions.report_agent_run(args.id, AgentResponse::Failed { error }).await?;
            }
        }

        Ok(Void)
    }
}

/// Issue d'un passage dans `run_turns` : soit le modèle a conclu sans appel
/// de tool ([`Self::Completed`]), soit le tour a déclenché des appels de
/// tool et le frame a été mis en attente de leurs réponses
/// ([`Self::Yielded`], voir [`crate::agent::status::YieldStatus::WaitingToolReply`]).
/// Contrairement à un simple `Result<ModelResponse, String>`, cette
/// distinction est nécessaire pour que [`RunAgent::execute`] sache si le
/// résultat a déjà été rapporté (cas `Yielded`, voir
/// [`SessionClient::report_tool_dispatch`]) ou reste à rapporter (cas
/// `Completed`/erreur, via [`SessionClient::report_agent_run`]).
enum TurnOutcome {
    Completed(ModelResponse),
    Yielded,
}

/// Fait avancer l'agent `agent_id` d'un unique tour modèle/tools. Contrairement
/// à une ancienne version qui enchaînait jusqu'à 100 tours dans le même Job
/// en attendant les tools en fire-and-forget (sans jamais réinjecter leurs
/// sorties dans `context`), un tour qui déclenche des appels de tool
/// persiste désormais l'attente ([`SessionClient::report_tool_dispatch`])
/// puis yielde immédiatement ([`TurnOutcome::Yielded`]) : la reprise (avec
/// les sorties de tool déjà injectées dans le contexte) se fait via un
/// nouveau Job `RunAgent`, resoumis par `SessionServer` dès que tous les
/// tools attendus ont répondu (voir `session::server::report_tool_execution`)
/// — même modèle de reprise que [`crate::agent::status::YieldStatus::WaitingAgents`].
/// Chaque tour consomme le flux SSE de [`model::execute`] : les
/// fragments de texte reçus sont relayés en direct vers
/// [`SessionClient::insert_in_log`] (une seule entrée par tour, voir
/// `log_id`), tandis que le [`ModelStatus`] du tour n'est connu qu'à la fin
/// du flux (`ModelStreamEvent::Completed`/`Failed`).
async fn run_turns(
    agent_id: AgentId,
    model: Model,
    tools: &[Tool],
    mut context: Context,
    sessions: &SessionClient,
    tools_client: &ToolClient,
) -> Result<TurnOutcome, String> {
    let session_id = agent_id.session_id();

    let log_id = SessionLogId::new(crate::id::generate_id());
    let stream = model::execute(agent_id, model.clone(), tools, context.clone())
        .await
        .map_err(|error| error.to_string())?;
    futures::pin_mut!(stream);

    let mut status = ModelStatus::Running;
    let mut turn_response = None;

    while let Some(event) = stream.next().await {
        match event {
            model::ModelStreamEvent::TextDelta(delta) => {
                sessions.insert_in_log(session_id, log_id, delta).await.map_err(|error| error.to_string())?;
            }
            model::ModelStreamEvent::Completed(response) => {
                if let Some(text) = &response.text {
                    context.push(ContextEntry { role: Role::Assistant, content: text.clone() });
                }
                status = if response.tool_calls.is_empty() {
                    ModelStatus::Completed
                } else {
                    ModelStatus::ToolCalls(response.tool_calls.clone())
                };
                turn_response = Some(response);
            }
            model::ModelStreamEvent::Failed(error) => return Err(error),
        }
    }

    match status {
        ModelStatus::Completed => {
            Ok(TurnOutcome::Completed(turn_response.expect("un tour complété porte toujours une réponse")))
        }
        ModelStatus::ToolCalls(calls) => {
            // `system/ask-user-input` est intercepté avant le dispatch
            // générique : sa résolution doit faire passer *ce* frame en
            // `Yielding(WaitingHitl)`, ce que la forme générique d'un
            // exécuteur de tool (voir `tools::builtin::register_builtins_tools_executors`,
            // qui ne connaît que `SessionId`, pas l'`AgentId` appelant) ne
            // peut pas exprimer — voir la doc de `ASK_USER_INPUT_TOOL`. Sur
            // le même modèle que l'ancienne (morte) interception de
            // `ASK_HUMAN_TOOL` dans `agent::run` : le premier appel trouvé
            // fait yielder le tour immédiatement, les autres appels
            // éventuellement groupés dans le même tour sont ignorés (un
            // modèle qui mélange une question humaine avec d'autres tools
            // dans le même tour les reverra au tour suivant).
            if let Some(call) = calls.iter().find(|call| call.name == ASK_USER_INPUT_TOOL) {
                let questions = parse_ask_user_input_questions(call)?;
                let hitl_id = HitlFrameId::new(session_id, crate::id::generate_id());

                sessions.push_hitl(hitl_id, Waiter::Agent(agent_id), questions, None).await.map_err(|error| error.to_string())?;

                return Ok(TurnOutcome::Yielded);
            }

            let tools_calls: Vec<ToolCallId> = calls.iter().map(|call| call.id).collect();

            // Persisté *avant* de déclencher les jobs `ToolExecution` : sans
            // cet ordre, un job particulièrement rapide pourrait rapporter
            // son résultat avant même que ce statut d'attente n'existe côté
            // `SessionServer`, et son identifiant ne serait jamais retiré de
            // `tools_calls` (l'agent resterait bloqué indéfiniment).
            sessions.report_tool_dispatch(agent_id, tools_calls).await.map_err(|error| error.to_string())?;

            for call in calls {
                tools_client.execute(call).await.map_err(|error| error.to_string())?;
            }

            Ok(TurnOutcome::Yielded)
        }
        ModelStatus::Running | ModelStatus::Failed => {
            Err("le flux du modèle s'est arrêté sans conclure".to_string())
        }
    }
}

/// Charge utile attendue des arguments d'un appel à
/// [`crate::tools::builtin::ASK_USER_INPUT_TOOL`] (voir sa déclaration) —
/// seul `questions` nous intéresse ici, la validation des réponses se fait
/// plus tard côté [`crate::hitl::validate_answers`].
#[derive(Debug, serde::Deserialize)]
struct AskUserInputArgs {
    questions: Vec<Question>,
}

fn parse_ask_user_input_questions(call: &ToolCall) -> Result<Vec<Question>, String> {
    let args: AskUserInputArgs = serde_json::from_value(call.parameters.clone())
        .map_err(|error| format!("arguments invalides pour {ASK_USER_INPUT_TOOL} : {error}"))?;
    Ok(args.questions)
}
