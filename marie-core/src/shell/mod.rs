use serde::{Deserialize, Serialize};

use crate::{model::ModelId, tools::ToolId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShellMode {
    LoopModel {
        system_prompt: String,
        model_id: ModelId,
        allowed_tools: Vec<ToolId>
    }
}