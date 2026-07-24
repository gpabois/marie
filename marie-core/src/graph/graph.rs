use std::{collections::HashMap, sync::Arc};

use crate::graph::{
    NodeId, 
    checkpoint::Checkpointer, 
    edge::Edge, node::{NodeName}
};

pub trait GraphState: Clone {}

#[derive(Clone)]
pub struct NodeParams {
    name: NodeName,
    args: serde_json::Value,
}

#[derive(Clone)]
pub struct Graph {
    nodes: HashMap<NodeId, NodeParams>,
    edges: HashMap<NodeId, Edge>,
    entry: NodeId,
    max_steps: u32,
}
