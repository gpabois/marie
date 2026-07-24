pub mod node;
pub mod edge;
pub mod checkpoint;
pub mod graph;
pub mod server;

pub use node::NodeId;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{id::ID, session::SessionId, state::State};

pub enum Goto {
    Node(NodeId),
    FanOut(Vec<NodeId>)
}

pub enum Halt {
    Terminated,
    Failed(String)
}


pub type GraphName = String;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
pub struct GraphFrameId(SessionId, ID);

impl GraphFrameId {
    pub fn session_id(&self) -> SessionId {
        self.0
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct GraphFrame {
    /// The instance id of the Frame
    id: GraphFrameId,
    /// The graph id
    graph_id: GraphName,
    /// The state of the graph
    state: State
}

impl GraphFrame {
    pub fn state(&self) -> &State {
        &self.state
    }
}
