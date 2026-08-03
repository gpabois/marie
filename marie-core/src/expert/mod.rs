pub mod model;
pub use model::{ExpertId, Expert, ExpertAskId};
use serde::{Deserialize, Serialize};

use std::borrow::Borrow;

use crate::{catalog::{Catalog, CatalogError, CatalogItemRef}, di::{Constructible, Resolve}};

#[derive(Clone)]
pub struct Experts {
    catalog: Catalog
}

impl<C> Constructible<C> for Experts where C: Resolve<Catalog> {
    fn construct(container: &C, _: ()) -> Self {
        Self::new(container.resolve(()))
    }
}

impl Experts {
    pub fn new(catalog: Catalog) -> Self {
        Self { catalog }
    }

    pub async fn create(&self, expert: Expert) -> Result<CatalogItemRef, CatalogError> {
        self.catalog.publish(expert).await
    }

    /// Version active la plus récente de `id`, désérialisée — sur le même
    /// modèle que [`crate::model::Models::latest`]/[`crate::tools::Tools::get`].
    pub async fn get(&self, id: &ExpertId) -> Result<Option<Expert>, CatalogError> {
        match self.catalog.latest::<Expert>(id.borrow()).await {
            Ok(item) => Ok(Some((*item).clone())),
            Err(CatalogError::NotFound { .. }) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Soft-delete (voir [`Catalog::delete`]) : retire l'expert `id` du
    /// catalogue actif, en le résolvant d'abord à sa version active la plus
    /// récente — échoue avec [`CatalogError::NotFound`] si `id` n'existe pas
    /// déjà.
    pub async fn delete(&self, id: &ExpertId) -> Result<(), CatalogError> {
        let r = self.catalog.latest_ref::<Expert>(id.borrow()).await?;
        self.catalog.delete(&r).await
    }

    pub async fn list(&self) -> Result<Vec<Expert>, CatalogError> {
        let refs = self.catalog.list::<Expert>().await?;
        let mut experts = Vec::with_capacity(refs.len());
        for r in refs {
            let item = self.catalog.deref::<Expert>(&r).await?;
            experts.push((*item).clone());
        }
        Ok(experts)
    }

    /// Publie une nouvelle version d'un expert déjà connu — contrairement à
    /// [`Self::create`], échoue avec [`CatalogError::NotFound`] si
    /// `expert.id` n'a encore aucune version active (pas de remplacement à
    /// faire).
    pub async fn replace(&self, expert: Expert) -> Result<CatalogItemRef, CatalogError> {
        self.catalog.latest_ref::<Expert>(expert.id.borrow()).await?;
        self.catalog.publish(expert).await
    }
}

/// Ce qu'un corps de node/script construit pour demander l'avis d'un expert
/// (voir `ask_experts!`/le Rune `ask_experts(...)`) — volontairement
/// dépourvu d'identifiant : le déterminisme du rejeu (voir
/// [`crate::session::run_log::RunLogs`]) interdit qu'un appelant en génère
/// un lui-même (aléatoire par nature, donc différent à chaque rejeu).
/// `SessionHandler::append_expert_asking` la transforme en [`AskExpert`] en
/// lui attachant un [`ExpertAskId`] fraîchement généré, côté serveur — même
/// principe que [`crate::hitl::Hitl`] transformé en
/// [`crate::hitl::protocol::HitlRequest`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequestAskExpert {
    pub task: String,
    pub expert_id: ExpertId
}

/// Version de [`RequestAskExpert`] portant l'[`ExpertAskId`] généré côté
/// `SessionHandler` — voir sa doc.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AskExpert {
    pub id: ExpertAskId,
    pub task: String,
    pub expert_id: ExpertId
}