pub mod catalog;
pub mod client;
pub mod declaration;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use crate::id::ID;

use crate::agent::GlobalAgentId;

pub type ToolName = String;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolSignature {
    pub name: ToolName,
    pub description: String,
    pub parameters_schema: Value
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ToolCallScope {
    Global,
    Session(ID)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: ID,
    pub name: ToolName,
    pub parameters: Option<Value>
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ToolCallError {
    TimeOut,
    Custom(String)
}

/// Requête d'exécution d'un tool relayée jusqu'à son exécuteur (voir
/// `tools::client::ToolClient::call`/`register_executor`) — `agent_id`
/// identifie l'appelant, pas seulement à titre informatif : un exécuteur
/// peut s'en servir pour retrouver le contexte de session (voir
/// `session::client::SessionClient`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCallRequest {
    pub agent_id: GlobalAgentId,
    pub call: ToolCall
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ToolCallResponse {
    Success {
        output: Option<Value>
    },
    Failed(ToolCallError),
}