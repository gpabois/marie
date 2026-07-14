//! DTOs échangés entre le frontend (feature `hydrate`, `wasm32-unknown-unknown`)
//! et le serveur (feature `ssr`, natif) de cet exemple — le seul module
//! compilé dans les deux cas (voir `lib.rs`).
//!
//! Ne dépend ni de `marie-core` (ne compile pas en wasm, voir
//! `marie-leptos::types`, dont ce module suit le même principe) ni de
//! `marie-axum`. Deux familles ici :
//!
//! - Le CRUD des catalogues et des vars/fichiers/sessions (`ModelDto`,
//!   `ToolDto`, ...) est un contrat propre à cet exemple, porté par des
//!   fonctions `#[server]` (voir `api.rs`) : sa forme JSON n'a pas besoin de
//!   correspondre à celle de `marie-core`, le serveur convertit dans les deux
//!   sens.
//! - Le sous-ensemble `*MessageDto`/`FrameSnapshotDto`/`SessionEventDto` en
//!   bas de fichier, à l'inverse, **doit** rester bit-à-bit compatible avec
//!   `marie_axum::protocol`/`marie_core::session::client::SessionEvent`/
//!   `marie_core::agent::status::AgentStatus` : le serveur monte
//!   `marie_axum::ws::router` tel quel (voir `main.rs`), sans traduction
//!   côté serveur — c'est ce module qui absorbe toute la compatibilité de
//!   forme JSON, pas une couche intermédiaire.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

// ---------------------------------------------------------------------------
// Catalogue : modèles
// ---------------------------------------------------------------------------

/// Reflet plat de `marie_core::model::declaration::Model::OpenAICompatible`
/// (seule variante existante) — `api_key` transite en clair sur ce canal
/// (appel de fonction serveur vers *son propre* serveur, servi en HTTPS en
/// pratique) ; c'est le serveur qui la fait ensuite chiffrer pour le control
/// plane (voir `NetworkClient::set_model`), jamais ce module.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelDto {
    pub id: String,
    pub base_url: String,
    pub client_id: String,
    pub api_key: String,
    pub model: String,
    pub system_prompt: Option<String>,
}

// ---------------------------------------------------------------------------
// Catalogue : tools
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolDto {
    pub id: String,
    pub name: String,
    pub description: String,
    pub parameters_schema: Value,
    /// `"global"` ou `"session"` — voir `marie_core::tools::declaration::ToolScope`.
    pub scope: String,
}

// ---------------------------------------------------------------------------
// Catalogue : experts
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpertDto {
    pub id: String,
    pub prompt: String,
    pub model_id: String,
    pub allowed_tools: Vec<String>,
}

// ---------------------------------------------------------------------------
// Catalogue : graphes d'états
// ---------------------------------------------------------------------------

/// Reflet de `marie_core::mode::executable::Executable` — même tag/casse
/// (`kind`, `snake_case`) pour que le serveur puisse convertir champ à champ
/// sans ambiguïté (voir `api::state_graph` côté conversion).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ExecutableDto {
    Rust { id: String },
    Python { source: String },
    Rune { source: String },
    Agent { expert_id: String, task: String },
}

/// Reflet de `marie_core::mode::state_graph::Node`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeDto {
    pub id: String,
    pub action: Option<ExecutableDto>,
}

/// Reflet de `marie_core::mode::state_graph::Edge`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeDto {
    pub from: String,
    pub to: String,
    pub guard: Option<ExecutableDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateGraphDto {
    pub id: String,
    pub entry: String,
    pub nodes: Vec<NodeDto>,
    pub edges: Vec<EdgeDto>,
}

// ---------------------------------------------------------------------------
// Vars (workspace/session) et fichiers de session
// ---------------------------------------------------------------------------

/// Snapshot complet d'un store clé-valeur (voir
/// `marie_core::session::client::SessionClient::values`/
/// `marie_core::workspace::client::WorkspaceClient::values`).
pub type VarsDto = HashMap<String, Value>;

/// Corps de `set_session_var`/`set_workspace_var` — une valeur JSON arbitraire.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetVarRequest {
    pub value: Value,
}

/// Chemins connus d'une session (voir `SessionClient::list_files`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileListDto {
    pub paths: Vec<String>,
}

/// Contenu d'un fichier de session — traité comme du texte UTF-8 pour cet
/// exemple (voir `api::files`) : suffisant pour visualiser/éditer les
/// fichiers qu'un agent produit typiquement (rapports, notes, code), pas
/// pensé pour du binaire.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileContentDto {
    pub content: String,
}

// ---------------------------------------------------------------------------
// Création de session
// ---------------------------------------------------------------------------

/// Argument de `create_session` — `workspace_id` absent crée un nouveau
/// workspace (voir `api::sessions::create_session`), sinon rattache la
/// nouvelle session à un workspace existant.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CreateSessionRequest {
    pub workspace_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSessionResponse {
    pub workspace_id: String,
    pub session_id: String,
}

/// Réponse de `session_workspace` — `None` si la session est inconnue du
/// control plane, ou pas encore rattachée à un workspace (voir
/// `RpcCall::SESSION_WORKSPACE`). Distinct de [`CreateSessionResponse`] : ce
/// point d'entrée sert à retrouver le workspace d'une session déjà existante
/// (tapée par l'utilisateur), pas seulement celle qu'on vient de créer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionWorkspaceResponse {
    pub workspace_id: Option<String>,
}

// ---------------------------------------------------------------------------
// Websocket — doit rester bit-compatible avec `marie_axum::protocol` (voir
// la doc de module).
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessageDto {
    SubscribeSession { session_id: String },
    UnsubscribeSession { session_id: String },
    GetFrame { session_id: String, local_id: String },
    /// Réponse à un [`HitlRequestDto`] reçu via [`ServerMessageDto::HitlRequest`]
    /// — le serveur (voir `marie_axum::ws::dispatch`) retrouve seul le
    /// formulaire d'origine depuis `request_id` (déjà mis en cache dès sa
    /// réception, voir `marie_axum::ws::HitlRegistry`) pour le valider ; pas
    /// besoin de le renvoyer ici.
    HitlAnswer { request_id: String, answers: HashMap<String, AnswerDto> },
    /// Injecte `text` comme nouveau message utilisateur pour démarrer un run
    /// d'agent (voir `marie_axum::protocol::ClientMessage::SendMessage`) —
    /// répond par [`ServerMessageDto::MessageSent`]. N'a de sens que pour un
    /// mode `simple` ou `orchestration` (voir [`ServerMessageDto::Mode`]),
    /// jamais `state_graph`.
    SendMessage { session_id: String, model_id: String, allowed_tools: Vec<String>, text: String },
    /// Récupère le mode actuellement au sommet de la pile de `session_id` —
    /// répond par [`ServerMessageDto::Mode`].
    GetMode { session_id: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessageDto {
    SessionEvent(SessionEventDto),
    HitlRequest(HitlRequestDto),
    Frame { session_id: String, local_id: String, frame: Option<FrameSnapshotDto> },
    Ack { in_reply_to: String },
    Error { in_reply_to: String, message: String },
    /// Réponse à [`ClientMessageDto::SendMessage`] — `local_id` est celui du
    /// frame nouvellement créé (son contenu arrive séparément, voir
    /// [`SessionEventDto::FrameStatusChanged`], déjà géré par
    /// `chat_view::ensure_socket`).
    MessageSent { session_id: String, local_id: String },
    /// Réponse à [`ClientMessageDto::GetMode`] — `mode` reste en `Value`
    /// brute pour la même raison que [`SessionEventDto::ModeChanged`] (voir
    /// sa doc) : seul un résumé textuel en est tiré côté `chat_view`
    /// (variante/stratégie/nombre d'enfants), pas une reconstruction fidèle
    /// de `marie_core::mode::SessionMode`.
    Mode { session_id: String, mode: Value },
}

/// Reflet de `marie_core::hitl::HumanInputRequest` — `agent_id` (un
/// `GlobalAgentId`, tuple struct à 2 champs côté `marie-core`) sérialise en
/// tableau JSON `[session_id, local_id]` par dérivation par défaut de serde ;
/// `(String, String)` reproduit exactement cette forme.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HitlRequestDto {
    pub id: String,
    pub agent_id: (String, String),
    pub questions: Vec<QuestionDto>,
}

impl HitlRequestDto {
    #[must_use]
    pub fn session_id(&self) -> &str {
        &self.agent_id.0
    }
}

/// Reflet de `marie_core::hitl::Question` (`#[serde(flatten)]` sur `kind`
/// côté `marie-core` — `key`/`label` restent donc à plat ici aussi, à côté
/// des champs aplatis de [`QuestionKindDto`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuestionDto {
    pub key: String,
    pub label: String,
    #[serde(flatten)]
    pub kind: QuestionKindDto,
}

/// Reflet de `marie_core::hitl::QuestionKind` — même tag/casse (`kind`,
/// `snake_case`) que côté `marie-core`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum QuestionKindDto {
    ShortText,
    LongText,
    Select { options: Vec<String> },
    Radio { options: Vec<String> },
    Checkboxes { options: Vec<String> },
    FileUpload { accept: Vec<String> },
}

/// Reflet de `marie_core::hitl::Answer` (`#[serde(untagged)]` côté
/// `marie-core`, reproduit ici à l'identique).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AnswerDto {
    Single(String),
    Multiple(Vec<String>),
}

/// Chemin, au sein de `/session/files`, où écrire le contenu d'un fichier
/// téléversé en réponse à une [`QuestionKindDto::FileUpload`] — reflet de
/// `marie_core::hitl::upload_path`/`sanitize_path_segment` : doit rester en
/// forme fidèle pour que l'agent (qui calcule ce même chemin de son côté)
/// retrouve le fichier au bon endroit.
#[must_use]
pub fn hitl_upload_path(request_id: &str, key: &str, filename: &str) -> String {
    fn sanitize(segment: &str) -> String {
        match segment.rsplit(['/', '\\']).next() {
            Some(candidate) if !candidate.is_empty() && candidate != "." && candidate != ".." => candidate.to_string(),
            _ => "_".to_string(),
        }
    }
    format!("hitl/{request_id}/{}/{}", sanitize(key), sanitize(filename))
}

/// Reflet de `marie_core::session::client::SessionEvent` — enum à balisage
/// externe *par défaut* (pas de `#[serde(tag = ...)]` côté `marie-core`),
/// donc les noms de variantes ci-dessous doivent rester en `PascalCase`
/// exact, sans `rename_all`. `status`/`log`/`mode` restent en `Value` brute
/// (voir la doc de module) : seul `Frame`/`FrameSnapshotDto` (récupéré à la
/// demande via `ClientMessageDto::GetFrame`) est interprété finement par ce
/// module, l'événement ne sert qu'à déclencher un rafraîchissement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SessionEventDto {
    Created { session_id: String },
    FrameStatusChanged { session_id: String, local_id: String, status: AgentStatusDto },
    LogAppended { session_id: String, log: SessionLogDto },
    /// `mode` reste en `Value` brute : cet exemple ne rend pas la pile de
    /// modes d'une session (voir `mode::SessionMode`, imbriquant
    /// `Orchestration`/`StateGraph`), seulement son statut/contexte/journal
    /// (voir [`FrameSnapshotDto`]/[`SessionLogDto`]) — inutile de mirrorer
    /// ici une hiérarchie que rien n'affiche.
    ModeChanged { session_id: String, mode: Value },
    Removed { session_id: String },
    ValueChanged { session_id: String, key: String, value: Value },
    ValueRemoved { session_id: String, key: String },
}

/// Reflet de `marie_core::session::SessionLog` (champs privés côté
/// `marie-core`, mais sérialisés par nom comme n'importe quel autre champ —
/// la visibilité Rust n'affecte pas la forme JSON dérivée).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionLogDto {
    pub id: String,
    pub data: SessionLogSpecDto,
}

/// Reflet de `marie_core::session::SessionLogSpec` — balisage externe par
/// défaut, comme [`SessionEventDto`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SessionLogSpecDto {
    AgentMessage { label: String, message: String },
    ToolCall(ToolCallDto),
}

/// Reflet de `marie_core::tools::ToolCall`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCallDto {
    pub id: String,
    pub name: String,
    pub parameters: Option<Value>,
}

/// Reflet de `marie_axum::protocol::FrameSnapshot`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrameSnapshotDto {
    pub session_id: String,
    pub id: String,
    pub model_id: String,
    pub status: AgentStatusDto,
    pub allowed_tools: Vec<String>,
    pub context: Vec<ContextEntryDto>,
    pub stdio: String,
    pub stderr: String,
}

/// Reflet de `marie_core::agent::status::AgentStatus` — mêmes contraintes de
/// balisage externe que [`SessionEventDto`] (pas de `rename_all`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AgentStatusDto {
    Initial,
    Paused,
    Running,
    Failed,
    Yielding(YieldStatusDto),
    Finished,
}

/// Reflet de `marie_core::agent::status::YieldStatus`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum YieldStatusDto {
    WaitingToolReply { tool_call_id: String },
    WaitingChildren { children: Vec<String> },
    RunExhausted,
}

/// Reflet de `marie_core::agent::context::ContextEntry`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextEntryDto {
    pub role: RoleDto,
    pub content: String,
}

/// Reflet de `marie_core::agent::role::Role` (`#[serde(rename_all = "lowercase")]`).
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RoleDto {
    System,
    User,
    Assistant,
    Tool,
}
