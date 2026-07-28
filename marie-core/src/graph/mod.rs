pub mod node;
pub mod edge;
pub mod checkpoint;
pub mod graph;
pub mod server;

use std::borrow::Borrow;

use bytemuck::{Pod, Zeroable};
pub use graph::{Graph, GraphId};
pub use node::NodeId;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    catalog::{Catalog, CatalogError, CatalogItemRef}, id::ID, session::{SessionId, frames::Frame},
};

pub enum Goto {
    Node(NodeId),
    FanOut(Vec<NodeId>)
}

pub enum Halt {
    Terminated,
    Failed(String)
}


pub type GraphName = String;

/// Identifiant d'un [`GraphFrame`], scopé à sa session — même forme que
/// [`crate::agent::AgentFrameId`] (une session peut porter plusieurs graphes
/// en cours). À ne pas confondre avec [`GraphId`], qui identifie une
/// *déclaration* de graphe dans le catalogue (voir [`Graphs`]) plutôt qu'une
/// instance en cours d'exécution dans une session.
#[derive(Debug, Hash, Clone, Copy, PartialEq, Eq, Pod, Zeroable, JsonSchema)]
#[repr(C)]
pub struct GraphFrameId(SessionId, ID);

impl GraphFrameId {
    pub fn new(session_id: SessionId, id: ID) -> Self {
        Self(session_id, id)
    }

    pub fn session_id(&self) -> SessionId {
        self.0
    }

    pub fn local_id(&self) -> ID {
        self.1
    }
}

impl AsRef<[u8]> for GraphFrameId {
    fn as_ref(&self) -> &[u8] {
        bytemuck::bytes_of(self)
    }
}

impl std::fmt::Display for GraphFrameId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.0, self.1)
    }
}

impl std::str::FromStr for GraphFrameId {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (session_part, local_part) = s.split_once('/').ok_or_else(|| crate::err!("format de GraphFrameId invalide : {s}"))?;
        Ok(Self(session_part.parse()?, local_part.parse()?))
    }
}

/// Sérialisé/désérialisé comme une chaîne plutôt que via le `derive` par
/// défaut — même raison que [`crate::agent::AgentFrameId`] : sert de clé de
/// `HashMap` dans un état sérialisé en JSON.
impl Serialize for GraphFrameId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for GraphFrameId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let repr = String::deserialize(deserializer)?;
        repr.parse().map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphFrame {
    pub graph_id: GraphId,
    pub task: String
}


#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphThreadFrame {
    pub cursor: NodeId
}

impl GraphThreadFrame {
    pub fn new(start: NodeId) -> Self {
        Self::new(start)
    }
}

impl From<GraphThreadFrame> for Frame {
    fn from(value: GraphThreadFrame) -> Self {
        Frame::GraphThread(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// A frame holding a set of threads (between fork and join)
pub struct GraphSpanFrame {}

impl From<GraphSpanFrame> for Frame {
    fn from(value: GraphSpanFrame) -> Self {
        Frame::GraphSpan(value)
    }
}

/// Référence immuable vers une version publiée d'un [`Graph`] — même rôle
/// que [`crate::model::ModelRef`] : une poignée opaque à conserver côté
/// appelant plutôt que l'`id`/version bruts, pour ne pas coupler ce dernier
/// au détail de représentation de [`CatalogItemRef`].
#[derive(Clone)]
pub struct GraphRef(CatalogItemRef);

/// Catalogue des déclarations de [`Graph`] connues du cluster — sur le même
/// principe que [`crate::model::Models`]/[`crate::expert::Experts`] : un
/// [`Catalog`] Postgres versionné plutôt qu'un état CRDT local, puisqu'un
/// graphe n'a (contrairement à une session) pas besoin d'être écrit en
/// continu ni fusionné entre pairs. Contrairement à `Models`, aucun secret à
/// chiffrer (voir [`Graph`]), donc pas de `Vault` à porter.
#[derive(Clone)]
pub struct Graphs {
    catalog: Catalog
}

impl Graphs {
    pub fn new(catalog: Catalog) -> Self {
        Self { catalog }
    }

    /// Publie la première version d'un graphe.
    pub async fn insert(&self, graph: Graph) -> Result<GraphRef, CatalogError> {
        Ok(GraphRef(self.catalog.publish(graph).await?))
    }

    /// Publie une nouvelle version d'un graphe déjà connu — contrairement à
    /// [`Self::insert`], échoue avec [`CatalogError::NotFound`] si
    /// `graph.id` n'a encore aucune version active.
    pub async fn replace(&self, graph: Graph) -> Result<GraphRef, CatalogError> {
        self.catalog.latest_ref::<Graph>(graph.id.borrow()).await?;
        Ok(GraphRef(self.catalog.publish(graph).await?))
    }

    /// Soft-delete (voir [`Catalog::delete`]) la version active de `id`.
    pub async fn delete(&self, id: &GraphId) -> Result<(), CatalogError> {
        let r = self.catalog.latest_ref::<Graph>(id.borrow()).await?;
        self.catalog.delete(&r).await
    }

    /// Référence de la version active la plus récente de `id`, sans lire son
    /// contenu — voir [`Self::get`] pour la déclaration désérialisée.
    pub async fn latest(&self, id: &GraphId) -> Result<Option<GraphRef>, CatalogError> {
        match self.catalog.latest_ref::<Graph>(id.borrow()).await {
            Ok(r) => Ok(Some(GraphRef(r))),
            Err(CatalogError::NotFound { .. }) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Version active la plus récente de `id`, désérialisée.
    pub async fn get(&self, id: &GraphId) -> Result<Option<Graph>, CatalogError> {
        match self.catalog.latest::<Graph>(id.borrow()).await {
            Ok(item) => Ok(Some((*item).clone())),
            Err(CatalogError::NotFound { .. }) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub async fn list(&self) -> Result<Vec<Graph>, CatalogError> {
        let refs = self.catalog.list::<Graph>().await?;
        let mut graphs = Vec::with_capacity(refs.len());
        for r in refs {
            let item = self.catalog.deref::<Graph>(&r).await?;
            graphs.push((*item).clone());
        }
        Ok(graphs)
    }
}

