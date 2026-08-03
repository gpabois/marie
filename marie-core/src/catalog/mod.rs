pub mod store;
pub mod in_memory;

use std::ops::Deref;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

use crate::{catalog::store::{CatalogStore, InsertCatalogItem, StoreCatalog}, di::{Constructible, Get}};

#[derive(Debug, Error)]
pub enum CatalogError {
    /// Pas de `#[from]` : [`crate::Error`] n'implémente volontairement pas
    /// `std::error::Error` (voir sa doc), donc `thiserror` ne peut pas en
    /// dériver `source()` automatiquement — conversion manuelle via
    /// `.map_err(CatalogError::StoreError)` sur chaque appel à
    /// [`StoreCatalog`], même idiome que `WorkerError::ExecutionError`.
    #[error("erreur lors des opérations de stockage du catalogue: {0}")]
    StoreError(crate::Error),
    #[error("élément de catalogue introuvable: {kind}/{id}")]
    NotFound { kind: String, id: String },
    #[error("erreur de (dé)sérialisation d'un élément de catalogue: {0}")]
    Deserialize(#[from] serde_json::Error),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CatalogItemRef {
    pub kind: String,
    pub id: String,
    pub version: u64,
}

pub struct Item<C> where C: Catalogable {
    version: u64,
    item: C
}

impl<C> Deref for Item<C> where C: Catalogable {
    type Target = C;
    
    fn deref(&self) -> &Self::Target {
        &self.item
    }
}

pub trait Catalogable: Serialize + DeserializeOwned + Clone + Send + 'static {
    const KIND: &str;

    fn id(&self) -> &str;
}

/// Façade typée du catalogue générique — `store` est le type opaque
/// [`CatalogStore`] plutôt qu'un `PgStore` concret, pour pouvoir le
/// construire en test avec [`in_memory::InMemoryCatalog`]
/// (`Catalog::new(CatalogStore::new(InMemoryCatalog::new()))`) sans passer
/// par Postgres.
#[derive(Clone)]
pub struct Catalog {
    store: CatalogStore
}

impl<C> Constructible<C> for Catalog where C: Get<CatalogStore> {
    fn construct(container: &C, _: ()) -> Self {
        Self::new(container.get())
    }
}

impl Catalog {
    pub fn new(store: CatalogStore) -> Self {
        Self { store }
    }

    pub fn in_memory() -> Self {
        Self {
            store: CatalogStore::in_memory()
        }
    }

    pub async fn publish<C>(&self, item: C) -> Result<CatalogItemRef, CatalogError> where C: Catalogable  {
        let data = serde_json::to_vec(&item)?;

        let args = InsertCatalogItem::builder()
            .kind(C::KIND)
            .id(item.id())
            .data(data)
            .build();

        let item_ref = self.store.insert_catalog_item(args).await.map_err(CatalogError::StoreError)?;
        Ok(item_ref)
    }

    pub async fn deref<C>(&self, r: &CatalogItemRef) -> Result<Item<C>, CatalogError> where C: Catalogable {
        let data = self
            .store
            .get_catalog_item(&r.kind, &r.id, r.version)
            .await
            .map_err(CatalogError::StoreError)?
            .ok_or_else(|| CatalogError::NotFound { kind: r.kind.clone(), id: r.id.clone() })?;
        let item = serde_json::from_slice(&data)?;
        Ok(Item { version: r.version, item })
    }

    /// Référence de la version active la plus récente de `id` — sans lire
    /// son contenu, voir [`Self::latest`] pour l'item désérialisé.
    pub async fn latest_ref<C>(&self, id: &str) -> Result<CatalogItemRef, CatalogError> where C: Catalogable {
        self.store
            .last_catalog_ref(C::KIND, id)
            .await
            .map_err(CatalogError::StoreError)?
            .ok_or_else(|| CatalogError::NotFound { kind: C::KIND.to_string(), id: id.to_string() })
    }

    /// Version active la plus récente de `id`, désérialisée — combine
    /// [`Self::latest_ref`] et [`Self::deref`].
    pub async fn latest<C>(&self, id: &str) -> Result<Item<C>, CatalogError> where C: Catalogable {
        let r = self.latest_ref::<C>(id).await?;
        self.deref(&r).await
    }

    pub async fn list<C>(&self) -> Result<Vec<CatalogItemRef>, CatalogError> where C: Catalogable {
        self.store.list_catalog_refs(C::KIND).await.map_err(CatalogError::StoreError)
    }

    pub async fn deprecate(&self, r: &CatalogItemRef) -> Result<(), CatalogError> {
        self.store.deprecate_catalog_item(&r.kind, &r.id).await.map_err(CatalogError::StoreError)
    }

    /// Soft-delete : retire `r.id` de toute lecture du catalogue (voir
    /// [`StoreCatalog::delete_catalog_item`]) — plus strict que
    /// [`Self::deprecate`], dont le contenu reste accessible à quiconque
    /// détient déjà la référence exacte.
    pub async fn delete(&self, r: &CatalogItemRef) -> Result<(), CatalogError> {
        self.store.delete_catalog_item(&r.kind, &r.id).await.map_err(CatalogError::StoreError)
    }
}
