
use async_openai::{Client, config::OpenAIConfig, error::OpenAIError, types::responses::{CreateResponseArgs, FunctionTool, OutputItem, Tool}};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use typed_builder::TypedBuilder;

use crate::{agent::Context, rpc::RpcError, secret::SecretError, session::SessionId, tools::{Tool as MarieTool, ToolCall, ToolCallId}};

pub mod catalog;
pub mod model;
pub mod client;
pub mod server;

pub use model::{ModelId, Model, EncryptedModel};

pub const RPC_MODEL_INSERT: &str = "/marie/models/insert";
pub const RPC_MODEL_UPDATE: &str = "/marie/models/update";
pub const RPC_MODEL_REMOVE: &str = "/marie/models/remove";
pub const RPC_MODEL_GET: &str = "/marie/models/get";
pub const RPC_MODEL_LIST: &str = "/marie/models/list";
pub const RPC_MODEL_RUN: &str = "/marie/models/run";
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

#[derive(Debug, Serialize, Deserialize)]
pub struct ModelResponse {
    pub text: Option<String>,
    pub tool_calls: Vec<ToolCall>
}

#[derive(TypedBuilder, Serialize, Deserialize)]
pub struct RunModelArgs {
    pub session_id: SessionId,
    pub model_id: ModelId,
    pub tools: Vec<MarieTool>,
    pub context: Context
}

pub async fn execute(session_id: SessionId, decl: Model, tools: &[MarieTool], input: impl ToString) -> Result<ModelResponse, ModelError> {
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
        .tools(tools.iter().cloned().map(|sig| Tool::Function(FunctionTool {
            name: sig.name,
            description: Some(sig.description),
            parameters: Some(sig.parameters_schema),
            ..Default::default()

        })).collect::<Vec<_>>());

    if let Some(system_prompt) = system_prompt {
        request.instructions(system_prompt);
    }

    let response = client.responses().create(request.build()?).await?;

    if let Some(err) = response.error {
        return Err(ModelError::ResponseError { code: err.code, message: err.message })
    }

    let text = response.output_text();

    // `response.tools` n'est que l'écho des tools *disponibles* (ceux passés
    // en requête, voir `CreateResponse::tools`) — les appels que le modèle a
    // effectivement décidé de faire sont dans `response.output`, sous
    // `OutputItem::FunctionCall` (voir `Response::output_text`, qui suit la
    // même logique pour le texte).
    let tool_calls = response.output.into_iter().filter_map(|item| match item {
        OutputItem::FunctionCall(call) => Some(ToolCall {
            id: ToolCallId::new(session_id, crate::id::generate_id()),
            name: call.name,
            parameters: serde_json::from_str(&call.arguments).ok(),
        }),
        _ => None,
    });

    Ok(ModelResponse {
        text,
        tool_calls: tool_calls.collect()
    })

}
