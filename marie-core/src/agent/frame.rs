use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;
use crate::agent::AgentId;
use crate::id::ID;

use crate::agent::{context::Context, status::AgentStatus};
use crate::model::ModelId;
use crate::tools::ToolId;

#[derive(TypedBuilder)]
pub struct AgentFrameArgs {
    pub id: AgentId,
    pub model: ModelId,
    #[builder(default, setter(strip_option))]
    pub parent: Option<AgentId>,
    pub context: Context,
    #[builder(default)]
    pub allowed_tools: Vec<ToolId>,
    #[builder(default)]
    pub stdio: String
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentFrame {
    /// Agent id
    pub id: AgentId,
    /// Model of the agent
    pub model: ModelId,
    /// Agent spawner
    pub parent: Option<AgentId>,
    /// Current status of the agent
    pub status: AgentStatus,
    /// Allowed tools 
    pub allowed_tools: Vec<ToolId>,
    /// Context
    pub context: Context,
    /// Standard input/output
    pub stdio: String,
    /// Standard error
    pub stderr: String
}

impl AgentFrame {
    pub fn new(args: AgentFrameArgs) -> Self {
        Self {
            id: args.id,
            model: args.model,
            parent: args.parent,
            status: AgentStatus::Initial,
            allowed_tools: args.allowed_tools,
            context: args.context,
            stdio: args.stdio,
            stderr: String::default()
        }
    }
}

pub struct AgentState {
    pub frame: AgentFrame,
    pub lamport_clock: u64,      // ← ordre de causalité
    pub node_id: PeerId,          // ← briseur d'égalité
}

pub struct AgentFrameUpdate {
    pub id: ID,
    pub previous_version: u64,
    
}
