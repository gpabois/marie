use libp2p::PeerId;
use crate::id::ID;

use crate::agent::{context::Context, status::AgentStatus};

pub struct AgentFrame {
    /// The session id of the frame
    pub session_id: ID,
    /// The instance id
    pub id: ID,
    /// Model of the agent
    pub model_id: String,
    /// Current status of the agent
    pub status: AgentStatus,
    /// Allowed tools 
    pub allowed_tools: Vec<String>,
    /// Context
    pub context: Context,
    /// Standard input/output
    pub stdio: String,
    /// Standard error
    pub stderr: String
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
