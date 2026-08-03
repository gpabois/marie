use std::collections::HashMap ;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use typed_builder::TypedBuilder;

use crate::{
    graph::graph::GraphRef, session::{
        SessionId, channel::ChannelName, snapshot::SnapshotRef, 
    }, 
};

pub mod store;
mod policy;
mod status;
mod id;
mod tree;
mod container;
mod node;
mod data;

use container::FrameNodeContainer;
pub use policy::{FramePolicy, ChildFailurePolicy, ParentPolicy};
pub use status::FrameStatus;
pub use id::FrameId;
pub use tree::{FrameTree, FrameTreeFactory};
pub use node::{FrameNode, NewFrameNodeArgs};
pub use data::FrameData;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FrameSpecRef {
    Graph(GraphRef),
    ToolAggregator,
    ToolCall,
    Expert,
    ExpertAggregator,
    Hitl
}

impl FrameSpecRef {
    pub fn into_graph_ref(self) -> GraphRef {
        if let Self::Graph(graph_ref) = self {
            graph_ref
        } else {
            panic!("not a graph ref")
        }
    }
}


