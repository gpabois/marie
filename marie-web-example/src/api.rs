//! Couche de données de cet exemple, sous forme de fonctions `#[server]`
//! (voir la doc de `leptos::server`) — remplace le duo `crate::api` (client
//! `gloo-net`) + `crate::server::{catalog,files,sessions,vars}` (handlers
//! `axum`) de l'ancien `marie-axum-leptos` : chaque fonction ici n'a qu'un
//! seul corps, qui s'exécute directement côté serveur (`expect_context`
//! remplace l'extracteur `State<GatewayState>` d'axum) et se change tout
//! seul, côté client (`hydrate`), en appel réseau vers un point d'entrée
//! généré automatiquement. Pas de conversion HTTP status/erreur à la main :
//! `ServerFnError` porte déjà l'erreur jusqu'au client (voir la doc de
//! `crate::dto`, dont les types restent inchangés).
//!
//! Exception délibérée : le téléversement HITL (contenu binaire brut) reste
//! une route `axum` classique (voir `crate::hitl_upload`) — une fonction
//! `#[server]` n'est pas un bon véhicule pour un corps de requête arbitraire,
//! comme la websocket `/ws` (voir `crate::ws_client`, `marie_axum::ws::router`)
//! n'en est pas un pour un flux bidirectionnel.

use leptos::prelude::*;

use crate::dto::{CreateSessionResponse, ExpertDto, FileContentDto, FileListDto, ModelDto, StateGraphDto, ToolDto, VarsDto};
#[cfg(feature = "ssr")]
use crate::dto::{EdgeDto, ExecutableDto, NodeDto};

#[cfg(feature = "ssr")]
use marie_axum::ws::GatewayState;
#[cfg(feature = "ssr")]
use marie_core::expert::catalog::ExpertId;
#[cfg(feature = "ssr")]
use marie_core::expert::declaration::ExpertDeclaration;
#[cfg(feature = "ssr")]
use marie_core::id::{ID, generate_id};
#[cfg(feature = "ssr")]
use marie_core::mode::executable::Executable;
#[cfg(feature = "ssr")]
use marie_core::mode::state_graph::catalog::StateGraphId;
#[cfg(feature = "ssr")]
use marie_core::mode::state_graph::declaration::StateGraphDeclaration;
#[cfg(feature = "ssr")]
use marie_core::mode::state_graph::{Edge, Node};
#[cfg(feature = "ssr")]
use marie_core::model::catalog::ModelId;
#[cfg(feature = "ssr")]
use marie_core::model::declaration::Model;
#[cfg(feature = "ssr")]
use marie_core::network::cp::rpc::RpcCall;
#[cfg(feature = "ssr")]
use marie_core::tools::ToolSignature;
#[cfg(feature = "ssr")]
use marie_core::tools::catalog::ToolId;
#[cfg(feature = "ssr")]
use marie_core::tools::declaration::{ToolDeclaration, ToolScope};
#[cfg(feature = "ssr")]
use marie_core::workspace::WorkspaceId;

/// Parse un identifiant en [`ID`] (voir `marie_core::id::ID::from_str`, forme
/// hexadécimale fixe de 32 caractères).
#[cfg(feature = "ssr")]
fn parse_id(raw: &str) -> Result<ID, ServerFnError> {
    raw.parse::<ID>().map_err(|error| ServerFnError::new(format!("identifiant invalide '{raw}' : {error}")))
}

#[cfg(feature = "ssr")]
fn to_server_fn_error(error: impl std::fmt::Display) -> ServerFnError {
    ServerFnError::new(error.to_string())
}

// ---------------------------------------------------------------------------
// Sessions
// ---------------------------------------------------------------------------

/// `workspace_id` absent crée un nouveau workspace, sinon rattache la
/// nouvelle session à un workspace existant (voir la doc de
/// `crate::dto::CreateSessionRequest`) — seul point d'entrée qui fabrique des
/// identifiants côté serveur (voir `marie_core::id::generate_id`) : le
/// frontend n'a jamais à en construire lui-même.
#[server]
pub async fn create_session(workspace_id: Option<String>) -> Result<CreateSessionResponse, ServerFnError> {
    let state = expect_context::<GatewayState>();
    let workspace_id = match workspace_id {
        Some(raw) => parse_id(&raw)?,
        None => generate_id(),
    };
    let session_id = state.gateway.workspace_client().create_session(workspace_id).await.map_err(to_server_fn_error)?;
    Ok(CreateSessionResponse { workspace_id: workspace_id.to_string(), session_id: session_id.to_string() })
}

/// Retrouve le workspace d'une session déjà existante (voir la doc de
/// `crate::dto::SessionWorkspaceResponse`) — lecture seule servie depuis
/// l'état Raft local du control plane, voir `RpcCall::SESSION_WORKSPACE`.
#[server]
pub async fn session_workspace(session_id: String) -> Result<Option<String>, ServerFnError> {
    let state = expect_context::<GatewayState>();
    let session_id = parse_id(&session_id)?;
    let workspace_id =
        state.gateway.network().rpc::<Option<WorkspaceId>>(RpcCall::new(RpcCall::SESSION_WORKSPACE, session_id)).await.map_err(to_server_fn_error)?;
    Ok(workspace_id.map(|id| id.to_string()))
}

/// Crée un workspace vierge (voir `WorkspaceClient::acquire`, qui ne fait
/// naître un workspace vide que lorsqu'aucune copie existante n'est
/// localisée — garanti ici puisque l'identifiant vient d'être généré). Ne
/// connaît aucune session tant qu'aucune n'y est créée (voir
/// [`create_session`]).
#[server]
pub async fn create_workspace() -> Result<String, ServerFnError> {
    let state = expect_context::<GatewayState>();
    let workspace_id = generate_id();
    state.gateway.workspace_client().acquire(workspace_id).await.map_err(to_server_fn_error)?;
    Ok(workspace_id.to_string())
}

/// Oublie le workspace localement (voir `WorkspaceClient::remove`) — purement
/// local à ce nœud passerelle, les autres détenteurs éventuels (voir la doc
/// de module de `workspace::client`) conservent leur copie.
#[server]
pub async fn delete_workspace(workspace_id: String) -> Result<(), ServerFnError> {
    let state = expect_context::<GatewayState>();
    let workspace_id = parse_id(&workspace_id)?;
    state.gateway.workspace_client().remove(workspace_id).await;
    Ok(())
}

// ---------------------------------------------------------------------------
// Vars (workspace/session)
// ---------------------------------------------------------------------------

#[server]
pub async fn session_vars(session_id: String) -> Result<VarsDto, ServerFnError> {
    let state = expect_context::<GatewayState>();
    let session_id = parse_id(&session_id)?;
    Ok(state.sessions.values(session_id).await)
}

#[server]
pub async fn set_session_var(session_id: String, key: String, value: serde_json::Value) -> Result<(), ServerFnError> {
    let state = expect_context::<GatewayState>();
    let session_id = parse_id(&session_id)?;
    state.sessions.set_value(session_id, key, value).await.map_err(to_server_fn_error)
}

#[server]
pub async fn delete_session_var(session_id: String, key: String) -> Result<(), ServerFnError> {
    let state = expect_context::<GatewayState>();
    let session_id = parse_id(&session_id)?;
    state.sessions.remove_value(session_id, key).await.map_err(to_server_fn_error)
}

#[server]
pub async fn workspace_vars(workspace_id: String) -> Result<VarsDto, ServerFnError> {
    let state = expect_context::<GatewayState>();
    let workspace_id = parse_id(&workspace_id)?;
    Ok(state.gateway.workspace_client().values(workspace_id).await)
}

#[server]
pub async fn set_workspace_var(workspace_id: String, key: String, value: serde_json::Value) -> Result<(), ServerFnError> {
    let state = expect_context::<GatewayState>();
    let workspace_id = parse_id(&workspace_id)?;
    state.gateway.workspace_client().set_value(workspace_id, key, value).await.map_err(to_server_fn_error)
}

#[server]
pub async fn delete_workspace_var(workspace_id: String, key: String) -> Result<(), ServerFnError> {
    let state = expect_context::<GatewayState>();
    let workspace_id = parse_id(&workspace_id)?;
    state.gateway.workspace_client().remove_value(workspace_id, key).await.map_err(to_server_fn_error)
}

// ---------------------------------------------------------------------------
// Fichiers de session (texte — voir crate::hitl_upload pour le binaire brut)
// ---------------------------------------------------------------------------

#[server]
pub async fn list_files(session_id: String) -> Result<FileListDto, ServerFnError> {
    let state = expect_context::<GatewayState>();
    let session_id = parse_id(&session_id)?;
    let paths = state.sessions.list_files(session_id).await.map_err(to_server_fn_error)?;
    Ok(FileListDto { paths })
}

/// `Ok(None)` si le fichier n'existe pas.
#[server]
pub async fn read_file(session_id: String, path: String) -> Result<Option<FileContentDto>, ServerFnError> {
    let state = expect_context::<GatewayState>();
    let session_id = parse_id(&session_id)?;
    match state.sessions.read_file(session_id, &path).await.map_err(to_server_fn_error)? {
        Some(bytes) => Ok(Some(FileContentDto { content: String::from_utf8_lossy(&bytes).into_owned() })),
        None => Ok(None),
    }
}

#[server]
pub async fn write_file(session_id: String, path: String, content: String) -> Result<(), ServerFnError> {
    let state = expect_context::<GatewayState>();
    let session_id = parse_id(&session_id)?;
    state.sessions.write_file(session_id, &path, content.into_bytes()).await.map_err(to_server_fn_error)
}

#[server]
pub async fn delete_file(session_id: String, path: String) -> Result<(), ServerFnError> {
    let state = expect_context::<GatewayState>();
    let session_id = parse_id(&session_id)?;
    state.sessions.delete_file(session_id, &path).await.map_err(to_server_fn_error)
}

// ---------------------------------------------------------------------------
// Catalogue : modèles
// ---------------------------------------------------------------------------

#[cfg(feature = "ssr")]
fn model_from_dto(dto: ModelDto) -> Model {
    Model::OpenAICompatible { base_url: dto.base_url, client_id: dto.client_id, api_key: dto.api_key, model: dto.model, system_prompt: dto.system_prompt }
}

#[cfg(feature = "ssr")]
fn model_to_dto(id: &ModelId, model: Model) -> ModelDto {
    let Model::OpenAICompatible { base_url, client_id, api_key, model, system_prompt } = model;
    ModelDto { id: id.to_string(), base_url, client_id, api_key, model, system_prompt }
}

#[server]
pub async fn list_models() -> Result<Vec<ModelDto>, ServerFnError> {
    let state = expect_context::<GatewayState>();
    let models = state.gateway.model_client().list().await.map_err(to_server_fn_error)?;
    Ok(models.into_iter().map(|(id, model)| model_to_dto(&id, model)).collect())
}

#[server]
pub async fn put_model(model: ModelDto) -> Result<(), ServerFnError> {
    let state = expect_context::<GatewayState>();
    let id = model.id.clone();
    state.gateway.model_client().set(id, model_from_dto(model)).await.map_err(to_server_fn_error)
}

#[server]
pub async fn delete_model(id: String) -> Result<(), ServerFnError> {
    let state = expect_context::<GatewayState>();
    state.gateway.model_client().remove(id).await.map_err(to_server_fn_error)
}

// ---------------------------------------------------------------------------
// Catalogue : tools
// ---------------------------------------------------------------------------

#[cfg(feature = "ssr")]
fn tool_from_dto(dto: ToolDto) -> Result<ToolDeclaration, ServerFnError> {
    let scope = match dto.scope.as_str() {
        "global" => ToolScope::Global,
        "session" => ToolScope::Session,
        other => return Err(ServerFnError::new(format!("portée de tool inconnue : '{other}' (attendu 'global' ou 'session')"))),
    };
    Ok(ToolDeclaration { signature: ToolSignature { name: dto.name, description: dto.description, parameters_schema: dto.parameters_schema }, scope })
}

#[cfg(feature = "ssr")]
fn tool_to_dto(id: &ToolId, decl: ToolDeclaration) -> ToolDto {
    let scope = match decl.scope {
        ToolScope::Global => "global",
        ToolScope::Session => "session",
    };
    ToolDto {
        id: id.to_string(),
        name: decl.signature.name,
        description: decl.signature.description,
        parameters_schema: decl.signature.parameters_schema,
        scope: scope.to_string(),
    }
}

#[server]
pub async fn list_tools() -> Result<Vec<ToolDto>, ServerFnError> {
    let state = expect_context::<GatewayState>();
    let tools = state.gateway.tool_client().list().await.map_err(to_server_fn_error)?;
    Ok(tools.into_iter().map(|(id, decl)| tool_to_dto(&id, decl)).collect())
}

#[server]
pub async fn put_tool(tool: ToolDto) -> Result<(), ServerFnError> {
    let state = expect_context::<GatewayState>();
    let id = tool.id.clone();
    let decl = tool_from_dto(tool)?;
    state.gateway.tool_client().set(id, decl).await.map_err(to_server_fn_error)
}

#[server]
pub async fn delete_tool(id: String) -> Result<(), ServerFnError> {
    let state = expect_context::<GatewayState>();
    state.gateway.tool_client().remove(id).await.map_err(to_server_fn_error)
}

// ---------------------------------------------------------------------------
// Catalogue : experts
// ---------------------------------------------------------------------------

#[cfg(feature = "ssr")]
fn expert_from_dto(dto: ExpertDto) -> ExpertDeclaration {
    ExpertDeclaration { prompt: dto.prompt, model_id: ModelId::new(dto.model_id), allowed_tools: dto.allowed_tools.into_iter().map(ToolId::new).collect() }
}

#[cfg(feature = "ssr")]
fn expert_to_dto(id: &ExpertId, decl: ExpertDeclaration) -> ExpertDto {
    ExpertDto {
        id: id.to_string(),
        prompt: decl.prompt,
        model_id: decl.model_id.to_string(),
        allowed_tools: decl.allowed_tools.into_iter().map(|tool_id| tool_id.to_string()).collect(),
    }
}

#[server]
pub async fn list_experts() -> Result<Vec<ExpertDto>, ServerFnError> {
    let state = expect_context::<GatewayState>();
    let experts = state.gateway.expert_client().list().await.map_err(to_server_fn_error)?;
    Ok(experts.into_iter().map(|(id, decl)| expert_to_dto(&id, decl)).collect())
}

#[server]
pub async fn put_expert(expert: ExpertDto) -> Result<(), ServerFnError> {
    let state = expect_context::<GatewayState>();
    let id = expert.id.clone();
    state.gateway.expert_client().set(id, expert_from_dto(expert)).await.map_err(to_server_fn_error)
}

#[server]
pub async fn delete_expert(id: String) -> Result<(), ServerFnError> {
    let state = expect_context::<GatewayState>();
    state.gateway.expert_client().remove(id).await.map_err(to_server_fn_error)
}

// ---------------------------------------------------------------------------
// Catalogue : graphes d'états
// ---------------------------------------------------------------------------

#[cfg(feature = "ssr")]
fn executable_from_dto(dto: ExecutableDto) -> Executable {
    match dto {
        ExecutableDto::Rust { id } => Executable::Rust { id },
        ExecutableDto::Python { source } => Executable::Python { source },
        ExecutableDto::Rune { source } => Executable::Rune { source },
        ExecutableDto::Agent { expert_id, task } => Executable::Agent { expert_id, task },
    }
}

#[cfg(feature = "ssr")]
fn executable_to_dto(executable: Executable) -> ExecutableDto {
    match executable {
        Executable::Rust { id } => ExecutableDto::Rust { id },
        Executable::Python { source } => ExecutableDto::Python { source },
        Executable::Rune { source } => ExecutableDto::Rune { source },
        Executable::Agent { expert_id, task } => ExecutableDto::Agent { expert_id, task },
    }
}

#[cfg(feature = "ssr")]
fn node_from_dto(dto: NodeDto) -> Node {
    Node::new(dto.id, dto.action.map(executable_from_dto))
}

#[cfg(feature = "ssr")]
fn node_to_dto(node: Node) -> NodeDto {
    NodeDto { id: node.id, action: node.action.map(executable_to_dto) }
}

#[cfg(feature = "ssr")]
fn edge_from_dto(dto: EdgeDto) -> Edge {
    Edge::new(dto.from, dto.to, dto.guard.map(executable_from_dto))
}

#[cfg(feature = "ssr")]
fn edge_to_dto(edge: Edge) -> EdgeDto {
    EdgeDto { from: edge.from, to: edge.to, guard: edge.guard.map(executable_to_dto) }
}

#[cfg(feature = "ssr")]
fn state_graph_from_dto(dto: StateGraphDto) -> StateGraphDeclaration {
    StateGraphDeclaration { nodes: dto.nodes.into_iter().map(node_from_dto).collect(), edges: dto.edges.into_iter().map(edge_from_dto).collect(), entry: dto.entry }
}

#[cfg(feature = "ssr")]
fn state_graph_to_dto(id: &StateGraphId, decl: StateGraphDeclaration) -> StateGraphDto {
    StateGraphDto {
        id: id.to_string(),
        entry: decl.entry,
        nodes: decl.nodes.into_iter().map(node_to_dto).collect(),
        edges: decl.edges.into_iter().map(edge_to_dto).collect(),
    }
}

#[server]
pub async fn list_state_graphs() -> Result<Vec<StateGraphDto>, ServerFnError> {
    let state = expect_context::<GatewayState>();
    let graphs = state.gateway.state_graph_client().list().await.map_err(to_server_fn_error)?;
    Ok(graphs.into_iter().map(|(id, decl)| state_graph_to_dto(&id, decl)).collect())
}

#[server]
pub async fn put_state_graph(graph: StateGraphDto) -> Result<(), ServerFnError> {
    let state = expect_context::<GatewayState>();
    let id = graph.id.clone();
    state.gateway.state_graph_client().set(id, state_graph_from_dto(graph)).await.map_err(to_server_fn_error)
}

#[server]
pub async fn delete_state_graph(id: String) -> Result<(), ServerFnError> {
    let state = expect_context::<GatewayState>();
    state.gateway.state_graph_client().remove(id).await.map_err(to_server_fn_error)
}
