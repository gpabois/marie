pub mod model;
pub use model::{ExpertId, Expert};

use std::borrow::Borrow;

use crate::catalog::{Catalog, CatalogError, CatalogItemRef};

pub struct Experts {
    catalog: Catalog
}

impl Experts {
    pub async fn create(&self, expert: Expert) -> Result<CatalogItemRef, CatalogError> {
        self.catalog.publish(expert).await
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



