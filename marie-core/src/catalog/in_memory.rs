use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use parking_lot::Mutex;

use crate::catalog::{
    CatalogItemRef,
    store::{InsertCatalogItem, StoreCatalog},
};

/// Une ligne de `catalog_item` (voir `migrations/0012_catalog.sql`,
/// `migrations/0013_catalog_soft_delete.sql`) — l'index de `Self` dans le
/// `Vec` de [`InMemoryCatalog::items`] tient lieu de `version` : comme côté
/// Postgres, une publication n'écrase jamais la précédente, elle en ajoute
/// une nouvelle à la fin (voir [`InMemoryCatalog::insert_catalog_item`]).
struct StoredItem {
    data: Vec<u8>,
    deprecated: bool,
    deleted: bool,
}

/// Implémentation en mémoire de [`StoreCatalog`] — même rôle que
/// [`crate::store::PgStore`] mais sans dépendance à Postgres, pour tester
/// [`super::Catalog`] (via [`super::store::CatalogStore::new`]) sans base de
/// données. Reproduit les mêmes règles que la table `catalog_item` :
/// `deprecate_catalog_item`/`delete_catalog_item` marquent *toutes* les
/// versions existantes d'un `(kind, id)`, pas seulement la dernière (même
/// portée que leurs équivalents SQL, `UPDATE ... WHERE kind = $1 AND id =
/// $2` sans filtre sur `version`) ; `get_catalog_item` reste lisible pour
/// une version dépréciée mais pas pour une version supprimée.
#[derive(Default, Clone)]
pub struct InMemoryCatalog {
    items: Arc<Mutex<HashMap<(String, String), Vec<StoredItem>>>>,
}

impl InMemoryCatalog {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl StoreCatalog for InMemoryCatalog {
    async fn insert_catalog_item(&self, args: InsertCatalogItem) -> crate::Result<CatalogItemRef> {
        let mut items = self.items.lock();
        let versions = items.entry((args.kind.clone(), args.id.clone())).or_default();
        let version = versions.len() as u64;
        versions.push(StoredItem { data: args.data, deprecated: false, deleted: false });

        Ok(CatalogItemRef { kind: args.kind, id: args.id, version })
    }

    async fn get_catalog_item(&self, kind: &str, id: &str, version: u64) -> crate::Result<Option<Vec<u8>>> {
        let items = self.items.lock();

        Ok(items
            .get(&(kind.to_string(), id.to_string()))
            .and_then(|versions| versions.get(version as usize))
            .filter(|item| !item.deleted)
            .map(|item| item.data.clone()))
    }

    async fn last_catalog_ref(&self, kind: &str, id: &str) -> crate::Result<Option<CatalogItemRef>> {
        let items = self.items.lock();

        Ok(items.get(&(kind.to_string(), id.to_string())).and_then(|versions| {
            versions
                .iter()
                .enumerate()
                .rev()
                .find(|(_, item)| !item.deprecated && !item.deleted)
                .map(|(version, _)| CatalogItemRef { kind: kind.to_string(), id: id.to_string(), version: version as u64 })
        }))
    }

    async fn list_catalog_refs(&self, kind: &str) -> crate::Result<Vec<CatalogItemRef>> {
        let items = self.items.lock();

        Ok(items
            .iter()
            .filter(|((item_kind, _), _)| item_kind == kind)
            .filter_map(|((_, id), versions)| {
                versions
                    .iter()
                    .enumerate()
                    .rev()
                    .find(|(_, item)| !item.deprecated && !item.deleted)
                    .map(|(version, _)| CatalogItemRef { kind: kind.to_string(), id: id.clone(), version: version as u64 })
            })
            .collect())
    }

    async fn deprecate_catalog_item(&self, kind: &str, id: &str) -> crate::Result<()> {
        let mut items = self.items.lock();

        if let Some(versions) = items.get_mut(&(kind.to_string(), id.to_string())) {
            versions.iter_mut().for_each(|item| item.deprecated = true);
        }

        Ok(())
    }

    async fn delete_catalog_item(&self, kind: &str, id: &str) -> crate::Result<()> {
        let mut items = self.items.lock();

        if let Some(versions) = items.get_mut(&(kind.to_string(), id.to_string())) {
            versions.iter_mut().for_each(|item| item.deleted = true);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn insert(kind: &str, id: &str, data: &[u8]) -> InsertCatalogItem {
        InsertCatalogItem::builder().kind(kind).id(id).data(data.to_vec()).build()
    }

    #[tokio::test]
    async fn test_insert_then_get_roundtrips() {
        let store = InMemoryCatalog::new();

        let r#ref = store.insert_catalog_item(insert("model", "gpt", b"v0")).await.unwrap();
        assert_eq!(r#ref.version, 0);

        let data = store.get_catalog_item("model", "gpt", 0).await.unwrap();
        assert_eq!(data, Some(b"v0".to_vec()));
    }

    #[tokio::test]
    async fn test_successive_inserts_increment_version() {
        let store = InMemoryCatalog::new();

        store.insert_catalog_item(insert("model", "gpt", b"v0")).await.unwrap();
        let r#ref = store.insert_catalog_item(insert("model", "gpt", b"v1")).await.unwrap();

        assert_eq!(r#ref.version, 1);
        assert_eq!(store.last_catalog_ref("model", "gpt").await.unwrap().map(|r| r.version), Some(1));
    }

    #[tokio::test]
    async fn test_deprecate_hides_from_last_ref_but_keeps_get() {
        let store = InMemoryCatalog::new();
        store.insert_catalog_item(insert("model", "gpt", b"v0")).await.unwrap();

        store.deprecate_catalog_item("model", "gpt").await.unwrap();

        assert_eq!(store.last_catalog_ref("model", "gpt").await.unwrap(), None);
        assert_eq!(store.get_catalog_item("model", "gpt", 0).await.unwrap(), Some(b"v0".to_vec()));
    }

    #[tokio::test]
    async fn test_delete_hides_from_both_last_ref_and_get() {
        let store = InMemoryCatalog::new();
        store.insert_catalog_item(insert("model", "gpt", b"v0")).await.unwrap();

        store.delete_catalog_item("model", "gpt").await.unwrap();

        assert_eq!(store.last_catalog_ref("model", "gpt").await.unwrap(), None);
        assert_eq!(store.get_catalog_item("model", "gpt", 0).await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_list_catalog_refs_filters_by_kind() {
        let store = InMemoryCatalog::new();
        store.insert_catalog_item(insert("model", "gpt", b"v0")).await.unwrap();
        store.insert_catalog_item(insert("tool", "search", b"v0")).await.unwrap();

        let refs = store.list_catalog_refs("model").await.unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].id, "gpt");
    }
}
