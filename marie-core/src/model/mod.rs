
use async_openai::{Client, config::OpenAIConfig, error::OpenAIError, types::responses::{CreateResponseArgs, FunctionTool, OutputItem, Tool}};
use serde::Serialize;
use thiserror::Error;

use crate::{model::catalog::ModelId, rpc::RpcError, tools::{ToolCall, ToolSignature}};

pub mod catalog;
pub mod model;
pub mod client;

pub use model::{Model, EncryptedModel};

#[derive(Debug, Error)]
pub enum ModelError {
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
}

#[derive(Debug, Serialize)]
pub struct ModelResponse {
    pub text: Option<String>,
    pub tool_calls: Vec<ToolCall>
}

pub async fn execute(decl: Model, tools: &[ToolSignature], input: impl Into<String>) -> Result<ModelResponse, ModelError> {
    let Model::OpenAICompatible { base_url, client_id, api_key, model, system_prompt, .. } = decl;

    let config = OpenAIConfig::new()
        .with_api_base(base_url)
        .with_api_key(api_key)
        .with_org_id(client_id);

    let client = Client::with_config(config);

    let mut request = CreateResponseArgs::default();
    request
        .model(model)
        .input(input.into())
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
            id: crate::id::generate_id(),
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

#[cfg(test)]
mod tests {
    use serde_json::json;
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers::{method, path}};

    use crate::id;

    use super::*;

    /// Modèle pointant vers `base_url` — un serveur `wiremock` local dans les
    /// tests ci-dessous, pour exercer [`execute`] contre un vrai aller-retour
    /// HTTP (sérialisation de la requête, désérialisation de la réponse)
    /// sans dépendre d'une vraie API OpenAI.
    fn model(base_url: String) -> Model {
        Model::OpenAICompatible {
            id: id::generate_id().to_string(),
            base_url,
            client_id: "test-org".to_string(),
            api_key: "sk-test".to_string(),
            model: "gpt-test".to_string(),
            system_prompt: None,
        }
    }

    /// Réponse Responses API minimale mais complète (tous les champs requis
    /// par `async_openai::types::responses::Response`) portant `output` tel
    /// quel — les tests construisent `output` pour le scénario qui les
    /// intéresse (message texte, appel de fonction, etc.).
    fn response_body(output: serde_json::Value) -> serde_json::Value {
        json!({
            "id": "resp_test",
            "object": "response",
            "created_at": 1_700_000_000,
            "model": "gpt-test",
            "status": "completed",
            "output": output,
        })
    }

    #[tokio::test]
    async fn test_execute_returns_text_when_model_answers_without_tool_call() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/responses"))
            .respond_with(ResponseTemplate::new(200).set_body_json(response_body(json!([
                {
                    "type": "message",
                    "id": "msg_1",
                    "role": "assistant",
                    "status": "completed",
                    "content": [
                        { "type": "output_text", "text": "bonjour", "annotations": [] }
                    ]
                }
            ]))))
            .expect(1)
            .mount(&server)
            .await;

        let response = execute(model(server.uri()), &[], "salut").await.unwrap();

        assert_eq!(response.text.as_deref(), Some("bonjour"));
        assert!(response.tool_calls.is_empty());
    }

    #[tokio::test]
    async fn test_execute_extracts_tool_calls_from_output_not_from_available_tools() {
        let server = MockServer::start().await;
        // La réponse déclare deux tools *disponibles* (`tools`, écho de la
        // requête) mais un seul appel réel dans `output` : si `execute`
        // régresse vers l'ancien bug (lire `response.tools` au lieu de
        // `response.output`), ce test verrait deux tool_calls au lieu d'un,
        // et le mauvais nom pour celui-ci.
        let mut body = response_body(json!([
            {
                "type": "function_call",
                "call_id": "call_1",
                "name": "search",
                "arguments": "{\"query\":\"rust\"}"
            }
        ]));
        body["tools"] = json!([
            { "type": "function", "name": "search", "parameters": {} },
            { "type": "function", "name": "unused_tool", "parameters": {} }
        ]);

        Mock::given(method("POST"))
            .and(path("/responses"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .expect(1)
            .mount(&server)
            .await;

        let response = execute(model(server.uri()), &[], "cherche des infos sur rust").await.unwrap();

        assert_eq!(response.tool_calls.len(), 1);
        assert_eq!(response.tool_calls[0].name, "search");
        assert_eq!(response.tool_calls[0].parameters, Some(json!({"query": "rust"})));
    }

    #[tokio::test]
    async fn test_execute_sends_input_and_declared_tool_signatures() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/responses"))
            .and(wiremock::matchers::body_partial_json(json!({
                "model": "gpt-test",
                "input": "quelle heure est-il ?",
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(response_body(json!([]))))
            .expect(1)
            .mount(&server)
            .await;

        let signatures = vec![ToolSignature {
            name: "clock".to_string(),
            description: "donne l'heure".to_string(),
            parameters_schema: json!({ "type": "object", "properties": {} }),
        }];

        let response = execute(model(server.uri()), &signatures, "quelle heure est-il ?").await.unwrap();
        assert_eq!(response.text, None);
        assert!(response.tool_calls.is_empty());
    }

    #[tokio::test]
    async fn test_execute_surfaces_response_error() {
        let server = MockServer::start().await;
        let mut body = response_body(json!([]));
        body["error"] = json!({ "code": "rate_limit_exceeded", "message": "trop de requêtes" });

        Mock::given(method("POST")).and(path("/responses")).respond_with(ResponseTemplate::new(200).set_body_json(body)).mount(&server).await;

        let error = execute(model(server.uri()), &[], "salut").await.unwrap_err();
        assert!(matches!(error, ModelError::ResponseError { code, .. } if code == "rate_limit_exceeded"));
    }
}