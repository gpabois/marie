use std::hash::Hash;
use std::{borrow::Borrow, collections::HashMap, fmt};

use serde::{Deserialize, Serialize};

use crate::catalog::{CatalogItemRef, Catalogable};
use crate::graph::NodeId;
use crate::script::Javascript;
use crate::session::spec::CommonSpec;

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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GraphRef(pub(crate) CatalogItemRef);

impl GraphRef {
    /// Accès à la [`CatalogItemRef`] sous-jacente pour
    /// [`crate::graph::Graphs::get`] — `graph::mod` (module parent de
    /// celui-ci) n'a pas accès au champ privé de `GraphRef` par simple
    /// portée de module, contrairement à `GraphSpecRef` qui y est défini
    /// directement.
    pub(crate) fn catalog_ref(&self) -> &CatalogItemRef {
        &self.0
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub enum NodeKind {
    Native(String),
    Script {
        params: serde_json::Value,
        source: String
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct NodeSpec {
    pub kind: NodeKind,
    pub common: CommonSpec,
}

/// Déclaration d'un graphe (nodes/edges/point d'entrée), publiée dans le
/// catalogue via [`crate::graph::Graphs`] — sur le même modèle que
/// [`crate::expert::Expert`] : ne référence ses nodes que par [`NodeName`]
/// (résolues à l'exécution via [`crate::graph::server::GraphServer`]), donc
/// ne porte aucun secret, contrairement à
/// [`crate::model::EncryptedModel`].
#[derive(Clone, Serialize, Deserialize)]
pub struct GraphSpec {
    pub id: GraphId,
    pub nodes: HashMap<NodeId, NodeSpec>,
    pub edges: HashMap<NodeId, NodeId>,
    pub entry: NodeId,
    pub common: CommonSpec
}

impl GraphSpec {
    pub fn new(id: GraphId, entry: impl Into<NodeId>, common: CommonSpec) -> Self {
        Self {
            id,
            nodes: HashMap::new(),
            edges: HashMap::new(),
            entry: entry.into(),
            common
        }
    }

    pub fn add_node(&mut self, id: NodeId, params: NodeSpec) {
        self.nodes.insert(id, params);
    }

}

impl Catalogable for GraphSpec {
    const KIND: &str = "/marie/catalog/graphs";

    fn id(&self) -> &str {
        self.id.borrow()
    }
}
