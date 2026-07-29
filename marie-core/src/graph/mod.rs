pub mod node;
pub mod edge;
pub mod checkpoint;
pub mod graph;
pub mod server;
pub mod reducer;
pub mod frames;

use std::borrow::Borrow;

use bytemuck::{Pod, Zeroable};
pub use graph::{GraphSpec, GraphId};
pub use node::NodeId;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    catalog::{Catalog, CatalogError, CatalogItemRef}, id::ID, session::{SessionId, frames::NewFrame},
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



/// Référence immuable vers une version publiée d'un [`Graph`] — même rôle
/// que [`crate::model::ModelRef`] : une poignée opaque à conserver côté
/// appelant plutôt que l'`id`/version bruts, pour ne pas coupler ce dernier
/// au détail de représentation de [`CatalogItemRef`].
#[derive(Clone)]
pub struct GraphSpecRef(CatalogItemRef);

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
    pub async fn insert(&self, graph: GraphSpec) -> Result<GraphSpecRef, CatalogError> {
        Ok(GraphSpecRef(self.catalog.publish(graph).await?))
    }

    /// Publie une nouvelle version d'un graphe déjà connu — contrairement à
    /// [`Self::insert`], échoue avec [`CatalogError::NotFound`] si
    /// `graph.id` n'a encore aucune version active.
    pub async fn replace(&self, graph: GraphSpec) -> Result<GraphSpecRef, CatalogError> {
        self.catalog.latest_ref::<GraphSpec>(graph.id.borrow()).await?;
        Ok(GraphSpecRef(self.catalog.publish(graph).await?))
    }

    /// Soft-delete (voir [`Catalog::delete`]) la version active de `id`.
    pub async fn delete(&self, id: &GraphId) -> Result<(), CatalogError> {
        let r = self.catalog.latest_ref::<GraphSpec>(id.borrow()).await?;
        self.catalog.delete(&r).await
    }

    /// Référence de la version active la plus récente de `id`, sans lire son
    /// contenu — voir [`Self::get`] pour la déclaration désérialisée.
    pub async fn latest(&self, id: &GraphId) -> Result<Option<GraphSpecRef>, CatalogError> {
        match self.catalog.latest_ref::<GraphSpec>(id.borrow()).await {
            Ok(r) => Ok(Some(GraphSpecRef(r))),
            Err(CatalogError::NotFound { .. }) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Version active la plus récente de `id`, désérialisée.
    pub async fn get(&self, id: &GraphId) -> Result<Option<GraphSpec>, CatalogError> {
        match self.catalog.latest::<GraphSpec>(id.borrow()).await {
            Ok(item) => Ok(Some((*item).clone())),
            Err(CatalogError::NotFound { .. }) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub async fn list(&self) -> Result<Vec<GraphSpec>, CatalogError> {
        let refs = self.catalog.list::<GraphSpec>().await?;
        let mut graphs = Vec::with_capacity(refs.len());
        for r in refs {
            let item = self.catalog.deref::<GraphSpec>(&r).await?;
            graphs.push((*item).clone());
        }
        Ok(graphs)
    }
}

