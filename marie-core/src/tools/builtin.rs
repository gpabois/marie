use serde::Deserialize;
use serde_json::{Value, json};

use crate::{
    session::{SessionId, client::SessionClient},
    tools::{Tool, server::ToolServer, worker::ToolWorkerArgs},
};

/// Lit une ou plusieurs variables de session via une expression JSONPath
/// (voir [`crate::session::client::SessionClient::query_vars`]).
pub const VARS_QUERY_TOOL: &str = "system/vars-query";
/// Remplace une ou plusieurs variables de session via une expression
/// JSONPath (voir [`crate::session::client::SessionClient::patch_vars`]).
pub const VARS_PATCH_TOOL: &str = "system/vars-patch";
/// Soumet un formulaire humain et fait yielder l'agent appelant en attendant
/// la réponse (voir [`crate::session::state::hitl::HitlFrame`],
/// [`crate::agent::status::YieldStatus::WaitingHitl`]) — contrairement à
/// [`VARS_QUERY_TOOL`]/[`VARS_PATCH_TOOL`], ce tool n'a pas d'exécuteur
/// générique enregistré via [`register_builtins_tools_executors`] : il est
/// intercepté par `session::worker::run_turns` avant le dispatch générique
/// des tools, car sa résolution mute le statut du frame appelant lui-même
/// (chose que la forme `Fn(SessionId, Args)` d'un exécuteur générique, qui
/// n'a pas accès à l'`AgentId` appelant, ne peut pas exprimer).
pub const ASK_USER_INPUT_TOOL: &str = "system/ask-user-input";

/// Déclaration de [`VARS_QUERY_TOOL`], à enregistrer dans le catalogue de
/// tools pour la rendre visible du modèle (voir [`crate::tools::client::ToolClient::insert`]) —
/// [`register_builtins_tools_executors`] ne fait qu'enregistrer l'exécuteur,
/// pas la déclaration.
#[must_use]
pub fn vars_query_tool_declaration() -> Tool {
    Tool {
        name: VARS_QUERY_TOOL.into(),
        description: "Lit une ou plusieurs variables de la session courante via une expression JSONPath (ex: \"$.budget\", \
            \"$.foo.bar[0]\") et renvoie la liste des valeurs trouvées (vide si aucune ne correspond)."
            .to_string(),
        parameters_schema: json!({
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Expression JSONPath à évaluer contre les variables de la session."
                }
            },
            "required": ["path"]
        }),
    }
}

/// Déclaration de [`VARS_PATCH_TOOL`] — voir [`vars_query_tool_declaration`].
#[must_use]
pub fn vars_patch_tool_declaration() -> Tool {
    Tool {
        name: VARS_PATCH_TOOL.into(),
        description: "Remplace, dans les variables de la session courante, chaque valeur correspondant à une expression \
            JSONPath (ex: \"$.budget\") par la valeur donnée. N'a aucun effet si `path` ne correspond à aucune variable \
            existante — ne crée pas de nouveau chemin."
            .to_string(),
        parameters_schema: json!({
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Expression JSONPath désignant la (ou les) valeur(s) à remplacer."
                },
                "value": {
                    "description": "Nouvelle valeur à écrire à cet emplacement."
                }
            },
            "required": ["path", "value"]
        }),
    }
}

/// Déclaration de [`ASK_USER_INPUT_TOOL`], à enregistrer dans le catalogue de
/// tools pour la rendre visible du modèle — même schéma `questions` que
/// l'ancien (mort) `hitl::tool_declaration` : chaque question porte une clé
/// stable (`key`), un libellé, un type (`kind`), et éventuellement des
/// options (`select`/`radio`/`checkboxes`) ou des extensions acceptées
/// (`file_upload`).
#[must_use]
pub fn ask_user_input_tool_declaration() -> Tool {
    Tool {
        name: ASK_USER_INPUT_TOOL.into(),
        description: "Soumet un formulaire d'une ou plusieurs questions à un opérateur humain et attend ses réponses avant de \
            poursuivre. A utiliser pour lever une ambiguïté, obtenir une validation, ou recueillir une information qu'aucune \
            donnée disponible ne permet de trancher seul."
            .to_string(),
        parameters_schema: json!({
            "type": "object",
            "properties": {
                "questions": {
                    "type": "array",
                    "minItems": 1,
                    "items": {
                        "type": "object",
                        "properties": {
                            "key": {
                                "type": "string",
                                "description": "Identifiant stable de cette question (ex: \"root_cause\"), utilisé pour retrouver sa réponse dans le résultat."
                            },
                            "label": {
                                "type": "string",
                                "description": "Le texte de la question, présenté tel quel à l'humain."
                            },
                            "kind": {
                                "type": "string",
                                "enum": ["short_text", "long_text", "select", "radio", "checkboxes", "file_upload"],
                                "description": "short_text/long_text : réponse libre (une ligne ou plusieurs). select/radio : un choix \
                                    unique parmi `options`. checkboxes : un ou plusieurs choix parmi `options`. file_upload : un \
                                    fichier à téléverser, éventuellement restreint par `accept`."
                            },
                            "options": {
                                "type": "array",
                                "items": { "type": "string" },
                                "description": "Requis (et non vide) pour kind = select, radio ou checkboxes ; absent sinon."
                            },
                            "accept": {
                                "type": "array",
                                "items": { "type": "string" },
                                "description": "Extensions de fichier acceptées (ex: [\".pdf\", \".png\"]), utilisé uniquement pour \
                                    kind = file_upload. Vide ou absent : tout fichier accepté."
                            }
                        },
                        "required": ["key", "label", "kind"]
                    }
                }
            },
            "required": ["questions"]
        }),
    }
}

#[derive(Debug, Deserialize)]
struct VarsQueryArgs {
    path: String,
}

#[derive(Debug, Deserialize)]
struct VarsPatchArgs {
    path: String,
    value: Value,
}

/// Enregistre les exécuteurs des tools builtins (voir [`VARS_QUERY_TOOL`]/
/// [`VARS_PATCH_TOOL`]) — chacun relaie vers le serveur de sessions qui
/// détient la session courante via `sessions` (voir
/// [`crate::session::client::SessionClient`]), sur le même modèle que tout
/// autre tool sauf qu'il s'exécute pour le compte de n'importe quelle
/// session sans déclaration séparée par session.
pub fn register_builtins_tools_executors(args: ToolWorkerArgs, sessions: SessionClient) -> ToolWorkerArgs {
    let query_sessions = sessions.clone();
    let args = args.add(VARS_QUERY_TOOL, move |session_id: SessionId, request: VarsQueryArgs| {
        let sessions = query_sessions.clone();
        async move { sessions.query_vars(session_id, request.path).await.map_err(anyhow::Error::from) }
    });

    args.add(VARS_PATCH_TOOL, move |session_id: SessionId, request: VarsPatchArgs| {
        let sessions = sessions.clone();
        async move { sessions.patch_vars(session_id, request.path, request.value).await.map_err(anyhow::Error::from) }
    })
}

/// Amorce le catalogue de `tool_server` avec les déclarations des tools
/// builtins (voir [`vars_query_tool_declaration`]/[`vars_patch_tool_declaration`]) —
/// appelé par [`ToolServer::new`] lui-même, directement sur son catalogue
/// (voir [`ToolServer::insert`]), donc sans passer par le réseau :
/// contrairement à [`register_builtins_tools_executors`] (côté worker, qui
/// exécute les appels), ceci s'exécute sur le nœud qui *sert* le catalogue
/// et n'a donc pas besoin de s'auto-découvrir via RPC. Idempotent (même
/// principe que `ToolCatalog::insert`) : sans effet de bord à rejouer.
pub fn register_builtins_tools(tool_server: ToolServer) {
    tool_server.insert(VARS_QUERY_TOOL, vars_query_tool_declaration());
    tool_server.insert(VARS_PATCH_TOOL, vars_patch_tool_declaration());
    tool_server.insert(ASK_USER_INPUT_TOOL, ask_user_input_tool_declaration());
}