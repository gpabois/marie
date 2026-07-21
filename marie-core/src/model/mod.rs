
use async_openai::{
    Client,
    config::OpenAIConfig,
    error::OpenAIError,
    types::responses::{CreateResponseArgs, FunctionTool, OutputItem, Response, ResponseStreamEvent, Tool}
};
use futures::{Stream, StreamExt as _};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{agent::AgentId, rpc::RpcError, secret::SecretError, tools::{Tool as MarieTool, ToolCall, ToolCallId}};

pub mod catalog;
pub mod model;
pub mod client;
#[cfg(feature = "catalog")]
pub mod server;
pub mod rpc;

pub use model::{ModelId, Model, EncryptedModel};
pub use rpc::{GetModel, InsertModel, ListModel, RemoveModel, UpdateModel};

pub const NS_MODEL: &str = "/marie/ns/models";


#[derive(Debug, Error)]
pub enum ModelError {
    #[error("aucun catalogue de modèles n'est disponible")]
    NoCatalogAvailable,
    #[error("échec de la requête : {0}")]
    OpenAIError(#[from] OpenAIError),
    #[error("échec lors de la réponse: {message} (code: {code})")]
    ResponseError {
        code: String,
        message: String
    },
    #[error("modèle inconnu : {0}")]
    UnknownModel(ModelId),
    #[error("[Model] échec de l'appel distant : {0}")]
    RpcError(#[from] RpcError),
    #[error("erreur lors des opérations de chiffrement/déchiffrement: {0}")]
    SecretError(#[from] SecretError),
    #[error("{0}")]
    Custom(String)
}

#[derive(Debug)]
pub enum ModelStatus {
    Running,
    Failed,
    Completed,
    ToolCalls(Vec<ToolCall>)
}

/// Métriques d'un appel modèle (tokens, billing) — portées par
/// [`ModelResponse::usage`], directement extraites de la réponse de l'API
/// `responses` (`Response::usage`/`Response::billing`). Capturées mais pas
/// encore journalisées/exploitées : ce sera l'objet d'un travail séparé.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelUsage {
    pub input_tokens: u32,
    pub cached_tokens: u32,
    pub output_tokens: u32,
    pub reasoning_tokens: u32,
    pub total_tokens: u32,
    pub payer: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ModelResponse {
    pub text: Option<String>,
    pub tool_calls: Vec<ToolCall>,
    pub usage: Option<ModelUsage>,
}

/// Évènement produit au fil du flux SSE de l'API `responses` (voir
/// [`execute`]) — [`TextDelta`](Self::TextDelta) est yield à mesure que le
/// modèle écrit, `Completed`/`Failed` clôturent le flux (un seul des deux
/// est produit, en tout dernier).
#[derive(Debug)]
pub enum ModelStreamEvent {
    TextDelta(String),
    Completed(ModelResponse),
    Failed(String),
}

/// Exécute un modèle en flux : contrairement à l'ancienne version bloquante
/// (un unique appel `Responses::create`), consomme
/// `Responses::create_stream` (SSE) et yield un [`ModelStreamEvent::TextDelta`]
/// pour chaque fragment de texte reçu — voir `session::worker::RunAgent`,
/// seul consommateur, qui relaie ces fragments vers
/// `SessionClient::insert_in_log` au fil de l'eau plutôt que d'attendre la
/// réponse complète.
#[inline]
pub async fn execute(agent_id: AgentId, decl: Model, tools: &[MarieTool], input: impl ToString) -> Result<impl Stream<Item = ModelStreamEvent> + Send, ModelError> {
    let Model::OpenAICompatible { base_url, client_id, api_key, model, system_prompt, .. } = decl;

    let config = OpenAIConfig::new()
        .with_api_base(base_url)
        .with_api_key(api_key)
        .with_org_id(client_id);

    let client = Client::with_config(config);

    let mut request = CreateResponseArgs::default();

    request
        .model(model)
        .input(input.to_string())
        .tools(tools.iter().cloned().map(|tool| Tool::Function(FunctionTool {
            name: tool.name.to_string(),
            description: Some(tool.description),
            parameters: Some(tool.parameters_schema),
            ..Default::default()

        })).collect::<Vec<_>>());

    if let Some(system_prompt) = system_prompt {
        request.instructions(system_prompt);
    }

    let mut stream = client.responses().create_stream(request.build()?).await?;

    Ok(async_stream::stream! {
        // `stream` (renvoyé par `create_stream`) est déjà un
        // `Pin<Box<dyn Stream<...>>>`, donc `Unpin` — pas besoin de
        // `pin_mut!` ici (nécessaire en revanche côté appelant pour le flux
        // renvoyé par cette fonction, voir `session::worker::run_turns`).
        let mut final_response: Option<Response> = None;
        let mut failure: Option<String> = None;

        while let Some(event) = stream.next().await {
            match event {
                Ok(ResponseStreamEvent::ResponseOutputTextDelta(delta)) => {
                    yield ModelStreamEvent::TextDelta(delta.delta);
                }
                Ok(ResponseStreamEvent::ResponseCompleted(completed)) => {
                    final_response = Some(completed.response);
                }
                Ok(ResponseStreamEvent::ResponseFailed(failed)) => {
                    failure = Some(failed.response.error.map(|err| err.message).unwrap_or_else(|| "réponse échouée".to_string()));
                }
                Ok(ResponseStreamEvent::ResponseError(err)) => {
                    failure = Some(err.message);
                }
                Err(err) => {
                    failure = Some(err.to_string());
                }
                _ => {}
            }
        }

        match (failure, final_response) {
            (Some(message), _) => yield ModelStreamEvent::Failed(message),
            (None, Some(response)) => {
                let text = response.output_text();
                let tool_calls = response.output.into_iter().filter_map(|item| match item {
                    OutputItem::FunctionCall(call) => Some(ToolCall {
                        id: ToolCallId::new(agent_id.session_id(), crate::id::generate_id()),
                        agent_id,
                        name: call.name,
                        parameters: serde_json::from_str(&call.arguments).unwrap_or_default(),
                    }),
                    _ => None,
                }).collect();
                let usage = response.usage.map(|u| ModelUsage {
                    input_tokens: u.input_tokens,
                    cached_tokens: u.input_tokens_details.cached_tokens,
                    output_tokens: u.output_tokens,
                    reasoning_tokens: u.output_tokens_details.reasoning_tokens,
                    total_tokens: u.total_tokens,
                    payer: response.billing.as_ref().map(|b| b.payer.clone()),
                });

                yield ModelStreamEvent::Completed(ModelResponse { text, tool_calls, usage });
            }
            (None, None) => yield ModelStreamEvent::Failed("le flux s'est terminé sans réponse".to_string()),
        }
    })
}
