//! Types de vue consommés par les composants de ce crate.
//!
//! Ce ne sont pas les types de `marie-core` (`AgentFrame`, `SessionLog`,
//! `SessionEvent`, ...) : `marie-core` embarque `libp2p`/`tokio`/`openraft`
//! et ne compile pas en `wasm32-unknown-unknown`. Ces structures sont donc
//! des DTO autonomes, à la charge de la passerelle (HTTP/WebSocket) de
//! traduire les types réseau vers ceux-ci avant de les passer en props.
//!
//! Les méthodes `label`/`detail` ci-dessous sont les seules briques de
//! présentation que ce crate fournit — de simples chaînes, pas de markup ni
//! de classe CSS (voir `session_panel`, entièrement *headless* : la
//! structure et le style sont à la charge du consommateur, ce crate ne
//! prescrit que la donnée et sa mise en forme textuelle).

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionView {
    pub id: String,
    pub frames: Vec<FrameView>,
    pub logs: Vec<SessionLogView>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameView {
    pub id: String,
    pub model_id: String,
    pub status: AgentStatusView,
    pub allowed_tools: Vec<String>,
    pub context: Vec<ContextEntryView>,
    pub stdio: String,
    pub stderr: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentStatusView {
    Initial,
    Paused,
    Running,
    Failed,
    Yielding(YieldStatusView),
    Finished,
}

impl AgentStatusView {
    /// Étiquette courte et stable (`"running"`, `"yielding"`, ...) — un nom
    /// de variante lisible, pas une classe CSS : à composer vous-même en
    /// nom de classe/attribut si besoin (ex. `format!("status--{}", status.label())`).
    pub fn label(&self) -> &'static str {
        match self {
            Self::Initial => "initial",
            Self::Paused => "paused",
            Self::Running => "running",
            Self::Failed => "failed",
            Self::Finished => "finished",
            Self::Yielding(_) => "yielding",
        }
    }

    /// Détail additionnel pour un statut [`Self::Yielding`] — la raison
    /// précise de l'attente (réponse d'un tool, agents enfants, budget
    /// épuisé). `None` pour tout autre statut.
    pub fn detail(&self) -> Option<String> {
        let Self::Yielding(yield_status) = self else { return None };
        Some(match yield_status {
            YieldStatusView::WaitingToolReply { tool_call_id } => {
                format!("waiting tool reply: {tool_call_id}")
            }
            YieldStatusView::WaitingChildren { children } => {
                format!("waiting children: {}", children.join(", "))
            }
            YieldStatusView::RunExhausted => "run exhausted".to_string(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum YieldStatusView {
    WaitingToolReply { tool_call_id: String },
    WaitingChildren { children: Vec<String> },
    RunExhausted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoleView {
    System,
    User,
    Assistant,
    Tool,
}

impl RoleView {
    /// Étiquette courte et stable (`"system"`, `"user"`, ...) — même
    /// principe que [`AgentStatusView::label`].
    pub fn label(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::User => "user",
            Self::Assistant => "assistant",
            Self::Tool => "tool",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextEntryView {
    pub role: RoleView,
    pub content: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionLogView {
    pub id: String,
    pub data: SessionLogSpecView,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionLogSpecView {
    AgentMessage { label: String, message: String },
    ToolCall(ToolCallView),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCallView {
    pub name: String,
    pub parameters: Option<String>,
}

/// Vue éditable d'un nœud de `marie_core::mode::state_graph::StateGraph::nodes`
/// — porte en plus `x`/`y`, une position d'affichage sans équivalent côté
/// `marie-core` (le graphe du domaine ne connaît qu'une topologie, pas une
/// disposition) : uniquement pertinente pour
/// [`crate::state_graph_editor::StateGraphEditor`].
#[derive(Debug, Clone, PartialEq)]
pub struct NodeView {
    pub id: String,
    pub x: f64,
    pub y: f64,
    pub action: Option<ExecutableView>,
}

/// Vue d'une arête de `marie_core::mode::state_graph::StateGraph::edges` —
/// `from`/`to` référencent un [`NodeView::id`].
#[derive(Debug, Clone, PartialEq)]
pub struct EdgeView {
    pub from: String,
    pub to: String,
    pub guard: Option<ExecutableView>,
}

/// Reflet de `marie_core::mode::executable::Executable` — `marie-core` ne
/// compile pas en wasm (voir l'en-tête de ce fichier), cette copie autonome
/// sert de charge utile pour [`NodeView::action`]/[`EdgeView::guard`].
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutableView {
    Rust { id: String },
    Python { source: String },
    Rune { source: String },
    Agent { expert_id: String, task: String },
}

impl ExecutableView {
    /// Étiquette courte (`"rust"`, `"python"`, ...) — même principe que
    /// [`AgentStatusView::label`].
    pub fn label(&self) -> &'static str {
        match self {
            Self::Rust { .. } => "rust",
            Self::Python { .. } => "python",
            Self::Rune { .. } => "rune",
            Self::Agent { .. } => "agent",
        }
    }
}

/// Reflet de `marie_core::hitl::HumanInputRequest` — un formulaire soumis
/// par un agent, à présenter à un opérateur humain (voir
/// [`crate::hitl_form::HitlForm`]). `session_id`/`local_id` proviennent de
/// `GlobalAgentId` (aplati ici, pas de type dédié côté vue) : c'est ce qui
/// permet au consommateur de savoir où écrire le contenu d'une éventuelle
/// réponse [`QuestionKindView::FileUpload`] (voir
/// `marie_core::hitl::upload_path`) sans dépendre de la session actuellement
/// ouverte par ailleurs — un formulaire peut concerner n'importe quel agent
/// du cluster, pas seulement celui de la session affichée.
#[derive(Debug, Clone, PartialEq)]
pub struct HitlRequestView {
    pub id: String,
    pub session_id: String,
    pub local_id: String,
    pub questions: Vec<QuestionView>,
}

/// Reflet de `marie_core::hitl::Question`.
#[derive(Debug, Clone, PartialEq)]
pub struct QuestionView {
    pub key: String,
    pub label: String,
    pub kind: QuestionKindView,
}

/// Reflet de `marie_core::hitl::QuestionKind`.
#[derive(Debug, Clone, PartialEq)]
pub enum QuestionKindView {
    ShortText,
    LongText,
    Select { options: Vec<String> },
    Radio { options: Vec<String> },
    Checkboxes { options: Vec<String> },
    FileUpload { accept: Vec<String> },
}

/// Reflet de `marie_core::hitl::Answer` — `Single` pour
/// `ShortText`/`LongText`/`Select`/`Radio`/`FileUpload` (voir
/// [`QuestionKindView`]), `Multiple` pour `Checkboxes`.
#[derive(Debug, Clone, PartialEq)]
pub enum AnswerView {
    Single(String),
    Multiple(Vec<String>),
}
