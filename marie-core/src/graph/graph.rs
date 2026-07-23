use std::{collections::HashMap, sync::Arc};

use crate::graph::{NodeId, checkpoint::Checkpointer, edge::Edge, node::{Nodable, NodeName}};

pub trait GraphState: Clone {}

pub struct NodeParams {
    name: NodeName,
    args: serde_json::Value
}

#[derive(Clone)]
pub struct Graph<S: GraphState> {
    nodes: HashMap<NodeId, NodeParams>,
    edges: HashMap<NodeId, Edge<S>>,
    entry: NodeId,
    max_steps: u32,
    checkpointer: Option<Arc<dyn Checkpointer<S>>>,
}
