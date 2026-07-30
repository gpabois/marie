pub mod node;
pub mod edge;
pub mod checkpoint;
pub mod graph;
pub mod server;
pub mod reducer;
use std::borrow::Borrow;

pub use graph::{GraphSpec, GraphId, GraphRef};
pub use node::NodeId;

use crate::
    catalog::{Catalog, CatalogError} 
;

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
    pub async fn insert(&self, graph: GraphSpec) -> Result<GraphRef, CatalogError> {
        Ok(GraphRef(self.catalog.publish(graph).await?))
    }

    /// Publie une nouvelle version d'un graphe déjà connu — contrairement à
    /// [`Self::insert`], échoue avec [`CatalogError::NotFound`] si
    /// `graph.id` n'a encore aucune version active.
    pub async fn replace(&self, graph: GraphSpec) -> Result<GraphRef, CatalogError> {
        self.catalog.latest_ref::<GraphSpec>(graph.id.borrow()).await?;
        Ok(GraphRef(self.catalog.publish(graph).await?))
    }

    /// Soft-delete (voir [`Catalog::delete`]) la version active de `id`.
    pub async fn delete(&self, id: &GraphId) -> Result<(), CatalogError> {
        let r = self.catalog.latest_ref::<GraphSpec>(id.borrow()).await?;
        self.catalog.delete(&r).await
    }

    /// Référence de la version active la plus récente de `id`, sans lire son
    /// contenu — voir [`Self::get`] pour la déclaration désérialisée.
    pub async fn latest(&self, id: &GraphId) -> Result<Option<GraphRef>, CatalogError> {
        match self.catalog.latest_ref::<GraphSpec>(id.borrow()).await {
            Ok(r) => Ok(Some(GraphRef(r))),
            Err(CatalogError::NotFound { .. }) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Contenu désérialisé référencé par `r` — contrairement à l'ancienne
    /// version indexée par [`GraphId`] (qui résolvait implicitement la
    /// version active la plus récente via [`Catalog::latest`], d'où
    /// l'`Option` pour le cas où `id` n'a encore aucune version publiée),
    /// `r` désigne déjà une version précise (voir [`Catalog::deref`]) : un
    /// échec ici veut dire que cette version a été supprimée, pas qu'elle
    /// n'a jamais existé — donc plus d'`Option`, une [`CatalogError`]
    /// suffit (même convention que [`Catalog::deref`] lui-même).
    pub async fn get(&self, r: &GraphRef) -> Result<GraphSpec, CatalogError> {
        let item = self.catalog.deref::<GraphSpec>(r.catalog_ref()).await?;
        Ok((*item).clone())
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

