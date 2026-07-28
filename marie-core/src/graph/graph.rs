use std::{borrow::Borrow, collections::HashMap, fmt};

use serde::{Deserialize, Serialize};

use crate::catalog::Catalogable;
use crate::graph::{NodeId, edge::Edge, node::NodeName};

pub trait GraphState: Clone {}

/// Identifiant unique d'une déclaration de [`Graph`] dans le catalogue (voir
/// [`crate::graph::Graphs`]) — même forme que
/// [`crate::expert::ExpertId`]/[`crate::model::ModelId`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct GraphId(String);

impl GraphId {
    pub fn new(id: impl ToString) -> Self {
        Self(id.to_string())
    }
}

impl fmt::Display for GraphId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for GraphId {
    fn from(id: String) -> Self {
        Self(id)
    }
}

impl From<&str> for GraphId {
    fn from(id: &str) -> Self {
        Self(id.to_owned())
    }
}

impl Borrow<str> for GraphId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl AsRef<[u8]> for GraphId {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct NodeParams {
    pub name: NodeName,
    pub args: serde_json::Value,
}

/// Déclaration d'un graphe (nodes/edges/point d'entrée), publiée dans le
/// catalogue via [`crate::graph::Graphs`] — sur le même modèle que
/// [`crate::expert::Expert`] : ne référence ses nodes que par [`NodeName`]
/// (résolues à l'exécution via [`crate::graph::server::GraphServer`]), donc
/// ne porte aucun secret, contrairement à
/// [`crate::model::EncryptedModel`].
#[derive(Clone, Serialize, Deserialize)]
pub struct Graph {
    pub id: GraphId,
    pub nodes: HashMap<NodeId, NodeParams>,
    pub edges: HashMap<NodeId, Edge>,
    pub entry: NodeId,
    pub max_steps: u32,
}

impl Graph {
    pub fn new(id: GraphId, entry: NodeId, max_steps: u32) -> Self {
        Self {
            id,
            nodes: HashMap::new(),
            edges: HashMap::new(),
            entry,
            max_steps,
        }
    }

    pub fn add_node(&mut self, id: NodeId, params: NodeParams) {
        self.nodes.insert(id, params);
    }

    pub fn add_edge(&mut self, from: NodeId, edge: Edge) {
        self.edges.insert(from, edge);
    }
}

impl Catalogable for Graph {
    const KIND: &str = "/marie/catalog/graphs";

    fn id(&self) -> &str {
        self.id.borrow()
    }
}
