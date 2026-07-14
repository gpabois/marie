pub mod executable;
pub mod orchestration;
pub mod state_graph;

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    mode::{orchestration::Orchestration, state_graph::StateGraph},
    tools::{
        ToolSignature,
        client::{ToolClient, ToolError},
        declaration::{ToolDeclaration, ToolScope},
    },
};

/// Mode de fonctionnement d'une session, au sommet de sa pile (voir
/// `session::crdt::YrsSession::push_mode`/`pop_mode`) — détermine comment
/// l'agent qui exécute cette session interprète son tour courant :
///
/// - [`SessionMode::Simple`] : conversation directe, un seul agent (voir
///   `agent::run`) — le mode implicite d'une pile vide, jamais empilé
///   explicitement (voir `Self::push_mode`, qui le rejette).
/// - [`SessionMode::Orchestration`] : un agent délègue à des agents enfants
///   (voir [`Orchestration`]).
/// - [`SessionMode::StateGraph`] : l'exécution suit un graphe d'états
///   explicite (voir [`StateGraph`]).
///
/// Empilable/dépilable en cours de session (voir [`PUSH_MODE_TOOL`]/
/// [`POP_MODE_TOOL`] ou directement `SessionClient::push_mode`/`pop_mode`
/// pour un pilotage humain) : un agent peut ainsi entrer temporairement en
/// orchestration ou suivre un graphe d'états pour une sous-tâche précise,
/// puis revenir (`pop`) à ce qu'il faisait avant, sans perdre le contexte du
/// mode englobant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum SessionMode {
    Simple,
    Orchestration(Orchestration),
    StateGraph(StateGraph),
}

/// Identifiant, dans le [`ToolCatalog`](crate::tools::catalog::ToolCatalog),
/// du tool qui empile un nouveau [`SessionMode`] sur la session courante
/// (voir [`push_mode_tool_declaration`]).
///
/// Contrairement à [`crate::hitl::ASK_HUMAN_TOOL`], ce tool s'exécute
/// entièrement localement : il mute la session que l'agent appelant est déjà
/// en train d'exécuter, sur le worker qui l'exécute déjà — pas de relais RPC
/// (`tools::client::ToolClient::call`) ni de gossip à traverser, juste un
/// appel direct à `SessionClient::push_mode` depuis la boucle qui dispatche
/// les tool calls de l'agent (voir `agent::run`, qui ne le fait pas encore).
pub const PUSH_MODE_TOOL: &str = "system/push-mode";

/// Identifiant du tool qui dépile le [`SessionMode`] courant de la session
/// (voir [`pop_mode_tool_declaration`]) — même modèle d'exécution locale que
/// [`PUSH_MODE_TOOL`].
pub const POP_MODE_TOOL: &str = "system/pop-mode";

/// Déclaration de [`PUSH_MODE_TOOL`] — portée [`ToolScope::Session`] plutôt
/// que `Global` (contrairement à [`crate::hitl::ASK_HUMAN_TOOL`]) :
/// changer le mode d'exécution de sa propre session est une capacité plus
/// engageante que solliciter un humain, réservée aux frames dont
/// `allowed_tools` la liste explicitement (voir
/// `agent::frame::AgentFrame::allowed_tools`) plutôt qu'ouverte par défaut à
/// n'importe quel agent simple.
#[must_use]
pub fn push_mode_tool_declaration() -> ToolDeclaration {
    ToolDeclaration {
        signature: ToolSignature {
            name: PUSH_MODE_TOOL.to_string(),
            description: "Empile un nouveau mode de fonctionnement sur la session courante (orchestration ou graphe d'états), \
                à dépiler avec system/pop-mode une fois la sous-tâche terminée pour revenir au mode précédent."
                .to_string(),
            parameters_schema: json!({
                "type": "object",
                "properties": {
                    "mode": {
                        "type": "string",
                        "enum": ["orchestration", "state_graph"],
                        "description": "Le mode à empiler. 'simple' ne peut pas être empilé : dépiler (system/pop-mode) suffit à y revenir."
                    },
                    "strategy": {
                        "type": "string",
                        "enum": ["sequential", "parallel"],
                        "description": "Requis pour mode = orchestration : séquentiel (un enfant après l'autre) ou parallèle."
                    },
                    "nodes": {
                        "type": "array",
                        "description": "Requis pour mode = state_graph : liste des nœuds, chacun avec un 'id' et une 'action' optionnelle.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": { "type": "string" },
                                "action": { "type": "object", "description": "Un Executable ({\"kind\": \"rust\", \"id\": \"...\"}, {\"kind\": \"agent\", \"expert_id\": \"...\", \"task\": \"...\"}, etc.), absent pour un nœud sans effet propre." }
                            },
                            "required": ["id"]
                        }
                    },
                    "edges": {
                        "type": "array",
                        "description": "Requis pour mode = state_graph : liste des transitions.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "from": { "type": "string" },
                                "to": { "type": "string" },
                                "guard": { "type": "object", "description": "Un Executable conditionnant le franchissement, absent pour une arête par défaut." }
                            },
                            "required": ["from", "to"]
                        }
                    },
                    "entry": {
                        "type": "string",
                        "description": "Requis pour mode = state_graph : id du nœud initial."
                    }
                },
                "required": ["mode"]
            }),
        },
        scope: ToolScope::Session,
    }
}

/// Déclaration de [`POP_MODE_TOOL`] — sans argument.
#[must_use]
pub fn pop_mode_tool_declaration() -> ToolDeclaration {
    ToolDeclaration {
        signature: ToolSignature {
            name: POP_MODE_TOOL.to_string(),
            description: "Dépile le mode de fonctionnement courant de la session et revient au précédent \
                (ou au mode 'simple' si la pile est désormais vide). Ne prend aucun argument."
                .to_string(),
            parameters_schema: json!({ "type": "object", "properties": {} }),
        },
        scope: ToolScope::Session,
    }
}

/// Enregistre (ou remplace) les déclarations de [`PUSH_MODE_TOOL`] et
/// [`POP_MODE_TOOL`] dans le catalogue de tools — idempotent, à appeler une
/// fois lors de la configuration du cluster (voir
/// `crate::hitl::client::HitlClient::ensure_declared` pour le même motif).
pub async fn ensure_tools_declared(tools: &ToolClient) -> Result<(), ToolError> {
    tools.set(PUSH_MODE_TOOL, push_mode_tool_declaration()).await?;
    tools.set(POP_MODE_TOOL, pop_mode_tool_declaration()).await?;
    Ok(())
}
