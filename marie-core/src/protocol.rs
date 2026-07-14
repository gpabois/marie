use serde::{Deserialize, Serialize};

use crate::{agent::AgentSpawnRequest, tools::{ToolCallRequest, ToolCallResponse}};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AgentMessage {
    AgentSpawnRequest(AgentSpawnRequest),
    ToolCallRequest(ToolCallRequest),
    ToolCallResponse(ToolCallResponse)
}