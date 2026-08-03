pub mod node;
pub mod graph;
pub mod reducer;
#[cfg(feature = "node-executor")]
pub mod executor;
pub mod builtin;
pub mod script;
pub mod run_log_macros;
use std::{borrow::Borrow, collections::HashMap, sync::Arc};

use parking_lot::Mutex;

pub use graph::{GraphSpec, GraphId, GraphRef};
pub use node::{NodeKindId, NodeId, NodeSpec};

use crate::
    catalog::{Catalog, CatalogError}
;
use crate::di::{Constructible, Resolve};
use crate::session::spec::CommonSpec;

/// Catalogue des déclarations de [`Graph`] connues du cluster — sur le même
/// principe que [`crate::model::Models`]/[`crate::expert::Experts`] : un
/// [`Catalog`] Postgres versionné plutôt qu'un état CRDT local, puisqu'un
/// graphe n'a (contrairement à une session) pas besoin d'être écrit en
/// continu ni fusionné entre pairs. Contrairement à `Models`, aucun secret à
/// chiffrer (voir [`Graph`]), donc pas de `Vault` à porter.
///
/// Porte aussi, sur le même principe que [`crate::tools::Tools`], le
/// registre des nodes natives ([`Nodable`](node::Nodable)) enregistrées en
/// mémoire par le process via [`node::Nodable::register`] : `native_nodes`
/// pour leur [`CommonSpec`] (toujours disponible, pour que la déclaration
/// d'une [`GraphSpec`] référençant cette node puisse être validée même sans
/// pouvoir l'exécuter), `executors` (derrière la feature `node-executor`)
/// pour l'exécution proprement dite.
#[derive(Clone)]
pub struct Graphs {
    catalog: Catalog,
    #[cfg(feature = "node-executor")]
    pub(crate) executors: executor::NodeExecutors,
    pub(crate) native_nodes: Arc<Mutex<HashMap<NodeKindId, CommonSpec>>>
}

impl<C> Constructible<C> for Graphs where C: Resolve<Catalog> {
    fn construct(container: &C, _: ()) -> Self {
        Self::new(container.resolve(()))
    }
}

impl Graphs {
    #[cfg(not(feature = "node-executor"))]
    pub fn new(catalog: Catalog) -> Self {
        Self { catalog, native_nodes: Arc::new(Mutex::new(HashMap::default())) }
    }

    #[cfg(feature = "node-executor")]
    pub fn new(catalog: Catalog) -> Self {
        Self {
            catalog,
            executors: executor::NodeExecutors::default(),
            native_nodes: Arc::new(Mutex::new(HashMap::default()))
        }
    }

    /// Enregistre une node native sans exécuteur — voir
    /// [`node::Nodable::register`] (variante sans la feature
    /// `node-executor`).
    #[cfg(not(feature = "node-executor"))]
    pub fn register(&self, id: NodeKindId, spec: CommonSpec) {
        self.native_nodes.lock().insert(id, spec);
    }

    /// Enregistre une node native et son exécuteur — voir
    /// [`node::Nodable::register`] (variante avec la feature
    /// `node-executor`).
    #[cfg(feature = "node-executor")]
    pub fn register<F, Fut>(&self, id: NodeKindId, spec: CommonSpec, executor: F)
        where
            F: Fn(node::NodeContext) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = crate::Result<(node::NodeContext, crate::session::protocol::FrameResult)>> + Send + 'static
    {
        self.native_nodes.lock().insert(id.clone(), spec);
        self.executors.add(id, executor);
    }

    /// Exécute la node native désignée par `spec.kind` — voir
    /// [`node::Nodable::register`]. Les surcharges de canaux propres à cette
    /// instance du node (`spec.common.overrides_channels`, voir
    /// [`CommonSpec::overrides_channels`]) sont déjà appliquées sur `ctx` par
    /// `SessionHandler` avant l'appel : `Graphs` reste indépendante de la
    /// notion de session/frame et n'a donc rien à y appliquer elle-même.
    #[cfg(feature = "node-executor")]
    pub async fn execute(
        &self,
        spec: &NodeSpec,
        ctx: node::NodeContext
    ) -> crate::Result<(node::NodeContext, crate::session::protocol::FrameResult)> {
        let executor = self.executors.get(&spec.kind)
            .ok_or_else(|| crate::err!("aucun exécuteur enregistré pour la node {:?}", spec.kind))?;

        executor(ctx).await
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

