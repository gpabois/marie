
use async_openai::{
    Client, config::OpenAIConfig, error::OpenAIError, types::responses::{CreateResponseArgs, FunctionTool, OutputItem, Response, ResponseStreamEvent, Tool}
};
use futures::{Stream, StreamExt as _};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    agent::{AgentFrameId, Context}, catalog::{Catalog, CatalogError, CatalogItemRef}, model::model::Model, secret::vault::{Vault, VaultError}, tools::{ModelToolCall, ToolCall, ToolCallId, ToolDefinition as MarieTool}
};

pub mod model;

pub use model::{ModelId, EncryptedModel};

pub const NS_MODEL: &str = "/marie/ns/models";

#[derive(Debug, Error)]
pub enum ModelError {
    #[error("erreur lors des opérations du coffre-fort: {0}")]
    VaultError(#[from] VaultError),
    #[error("erreur lors des opérations de cataloguage: {0}")]
    CatalogError(#[from] CatalogError),
    #[error("erreur lors des appels au service compatible OpenAI: {0}")]
    OpenAIError(#[from] OpenAIError)
}

#[derive(Clone)]
pub struct Models {
    vault: Vault,
    catalog: Catalog
}

pub enum CreateModel {
    OpenAICompatible {
        id: ModelId,
        base_url: String,
        client_id: String,
        api_key: String,
        model: String,
        system_prompt: Option<String>,
    }
}

pub struct ModelChangeset {
    id: String,
    changes: Vec<ModelChange>
}

pub enum ModelChange {
    SetBaseUrl(String),
    SetClientId(String),
    SetApiKey(String),
    SetModel(String),
    SetSystemPrompt(String)
}

#[derive(Clone)]
pub struct ModelRef(CatalogItemRef);

pub struct ExecuteModelArgs {
    model: ModelId,
    context: Context,
    tools: Vec<MarieTool>
}

impl Models {
    pub async fn insert(&self, model: CreateModel) -> Result<ModelRef, ModelError> {
        let model = match model {
            CreateModel::OpenAICompatible { id, base_url, client_id, api_key, model, system_prompt } => {
                let api_key = self.vault.store("/marie/secrets/models", api_key.as_bytes()).await?;
                EncryptedModel::OpenAICompatible { id, base_url, client_id, api_key, model, system_prompt }
            },
        };

        let item_ref = ModelRef(self.catalog.publish(model).await?);
        Ok(item_ref)
    }

    /// Applique un jeu de changements à la version active de `changeset.id`
    /// et publie le résultat comme nouvelle version — `SetApiKey` ne change
    /// pas la référence de secret (voir [`Model::OpenAICompatible::api_key`]),
    /// seul son contenu chiffré est remplacé en place (voir [`Vault::update`]).
    pub async fn change(&self, changeset: ModelChangeset) -> Result<ModelRef, ModelError> {
        let item = self.catalog.latest::<EncryptedModel>(&changeset.id).await?;
        let EncryptedModel::OpenAICompatible { id, mut base_url, mut client_id, api_key, mut model, mut system_prompt } = (*item).clone();

        for change in changeset.changes {
            match change {
                ModelChange::SetBaseUrl(value) => base_url = value,
                ModelChange::SetClientId(value) => client_id = value,
                ModelChange::SetApiKey(value) => self.vault.update(api_key, "/marie/secrets/models", value.as_bytes()).await?,
                ModelChange::SetModel(value) => model = value,
                ModelChange::SetSystemPrompt(value) => system_prompt = Some(value),
            }
        }

        let updated = EncryptedModel::OpenAICompatible { id, base_url, client_id, api_key, model, system_prompt };
        Ok(ModelRef(self.catalog.publish(updated).await?))
    }

    /// Supprime le secret associé puis soft-delete l'item de catalogue (voir
    /// [`Catalog::delete`]) — dans cet ordre : un item de catalogue sans
    /// secret déchiffrable est sans usage, l'inverse (secret orphelin après
    /// coup) ne l'est pas.
    pub async fn delete(&self, id: &ModelId) -> Result<(), ModelError> {
        let r = self.catalog.latest_ref::<EncryptedModel>(id).await?;
        let item = self.catalog.deref::<EncryptedModel>(&r).await?;
        let EncryptedModel::OpenAICompatible { api_key, .. } = &*item;
        self.vault.remove(*api_key).await?;
        self.catalog.delete(&r).await?;
        Ok(())
    }

    pub async fn latest(&self, id: &ModelId) -> Result<Option<ModelRef>, ModelError> {
        match self.catalog.latest_ref::<EncryptedModel>(id).await {
            Ok(r) => Ok(Some(ModelRef(r))),
            Err(CatalogError::NotFound { .. }) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    /// Vérifie qu'une référence déjà obtenue (voir [`Self::latest`]) désigne
    /// toujours un item lisible du catalogue — ne relit pas son contenu, qui
    /// n'a pas changé puisqu'une référence pointe une version immuable
    /// (seule sa disponibilité peut changer, via [`Self::delete`]).
    pub async fn get(&self, model_ref: &ModelRef) -> Result<Option<ModelRef>, ModelError>  {
        match self.catalog.deref::<EncryptedModel>(&model_ref.0).await {
            Ok(_) => Ok(Some(model_ref.clone())),
            Err(CatalogError::NotFound { .. }) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    pub async fn list(&self) -> Result<Vec<EncryptedModel>, ModelError> {
        let refs = self.catalog.list::<EncryptedModel>().await?;
        let mut models = Vec::with_capacity(refs.len());
        for r in refs {
            let item = self.catalog.deref::<EncryptedModel>(&r).await?;
            models.push((*item).clone());
        }
        Ok(models)
    }

    /// Résout `args.model` à sa version active, déchiffre son secret (voir
    /// [`EncryptedModel::decrypt`]) puis délègue à la fonction libre
    /// [`execute`] — c'est ici, pas dans [`execute`], que vit la traduction
    /// entre l'`EncryptedModel` du catalogue et le `Model` en clair qu'elle
    /// consomme, pour que `execute` reste utilisable indépendamment d'un
    /// `Catalog`/`Vault` (voir ses appelants de test).
    pub async fn execute(&self, args: ExecuteModelArgs) -> Result<impl Stream<Item = ModelStreamEvent> + Send, ModelError> {
        let item = self.catalog.latest::<EncryptedModel>(&args.model).await?;
        let model = (*item).clone().decrypt(&self.vault).await?;
        execute(model, args.tools, args.context).await
    }
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
    pub tool_calls: Vec<ModelToolCall>,
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
pub async fn execute(model: Model, tools: Vec<MarieTool>, input: impl ToString) -> Result<impl Stream<Item = ModelStreamEvent> + Send, ModelError> {
    let Model::OpenAICompatible { base_url, client_id, api_key, model, system_prompt, .. } = model;

    let config = OpenAIConfig::new()
        .with_api_base(base_url)
        .with_api_key(api_key)
        .with_org_id(client_id);

    let client = Client::with_config(config);

    let mut request = CreateResponseArgs::default();

    request
        .model(model)
        .input(input.to_string())
        .tools(tools.into_iter().map(|tool| Tool::Function(FunctionTool {
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
                    OutputItem::FunctionCall(call) => Some(ModelToolCall {
                        id: ToolCallId::new(crate::id::generate_id()),
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
