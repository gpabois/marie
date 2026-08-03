use std::sync::Arc;

use async_trait::async_trait;
#[cfg(feature = "catalog")]
use sqlx::Row as _;
use typed_builder::TypedBuilder;

use crate::catalog::in_memory::InMemoryCatalog;
#[cfg(feature = "catalog")]
use crate::store::PgStore;

use super::CatalogItemRef;

#[derive(TypedBuilder)]
pub struct InsertCatalogItem {
    #[builder(setter(transform = |x: impl ToString| x.to_string()))]
    pub(crate) id: String,
    #[builder(setter(transform = |x: impl ToString| x.to_string()))]
    pub(crate) kind: String,
    pub(crate) data: Vec<u8>,
}

/// Stockage bas niveau du catalogue générique (voir [`super::Catalog`]) —
/// `crate::Error` plutôt qu'un `type Error` associé (comme portait
/// l'ancien `CatalogStore`) : un `Error` associé empêcherait `dyn
/// StoreCatalog` d'être un type unique utilisable derrière [`CatalogStore`]
/// (il faudrait fixer `Error` au type concret d'un unique backend), alors
/// que [`crate::Error`] encaisse aussi bien une `sqlx::Error` (voir
/// `impl StoreCatalog for PgStore`) qu'une erreur d'un backend en mémoire
/// (voir `super::in_memory::InMemoryCatalog`), sans que les deux aient
/// besoin de partager un type d'erreur concret commun.
///
/// `insert_catalog_item` prend `data` déjà sérialisé (`Vec<u8>`), pas un
/// `C: Catalogable` générique comme l'ancien `CatalogStore` : une méthode
/// générique n'est pas "object safe", donc incompatible avec `dyn
/// StoreCatalog` — c'est à l'appelant ([`super::Catalog::publish`]) de
/// sérialiser avant d'appeler, symétrique de [`Self::get_catalog_item`] qui
/// renvoie déjà des octets bruts plutôt qu'un type désérialisé.
#[async_trait]
pub trait StoreCatalog {
    async fn insert_catalog_item(&self, args: InsertCatalogItem) -> crate::Result<CatalogItemRef>;
    async fn get_catalog_item(&self, kind: &str, id: &str, version: u64) -> crate::Result<Option<Vec<u8>>>;
    async fn last_catalog_ref(&self, kind: &str, id: &str) -> crate::Result<Option<CatalogItemRef>>;
    /// Référence active la plus récente de chaque `id` connu pour `kind` —
    /// utilisé par `Catalog::list` pour énumérer un catalogue complet sans
    /// connaître ses `id` à l'avance.
    async fn list_catalog_refs(&self, kind: &str) -> crate::Result<Vec<CatalogItemRef>>;
    async fn deprecate_catalog_item(&self, kind: &str, id: &str) -> crate::Result<()>;
    /// Soft-delete : retire `id` de toute lecture du catalogue (y compris
    /// `get_catalog_item` par version explicite, contrairement à
    /// [`Self::deprecate_catalog_item`]) sans supprimer physiquement ses
    /// lignes (voir `migrations/0013_catalog_soft_delete.sql`).
    async fn delete_catalog_item(&self, kind: &str, id: &str) -> crate::Result<()>;
}

/// Type opaque enveloppant n'importe quel backend [`StoreCatalog`] derrière
/// un unique `Arc<dyn StoreCatalog + Send + Sync + 'static>` — permet à
/// [`super::Catalog`] de ne connaître ni `PgStore` ni
/// `super::in_memory::InMemoryCatalog` par leur type concret, pour pouvoir
/// substituer ce dernier en test unitaire sans passer par Postgres.
#[derive(Clone)]
pub struct CatalogStore(Arc<dyn StoreCatalog + Send + Sync + 'static>);

impl CatalogStore {
    pub fn new(store: impl StoreCatalog + Send + Sync + 'static) -> Self {
        Self(Arc::new(store))
    }

    pub fn in_memory() -> Self {
        Self::new(InMemoryCatalog::new())
    }
}

#[async_trait]
impl StoreCatalog for CatalogStore {
    async fn insert_catalog_item(&self, args: InsertCatalogItem) -> crate::Result<CatalogItemRef> {
        self.0.insert_catalog_item(args).await
    }

    async fn get_catalog_item(&self, kind: &str, id: &str, version: u64) -> crate::Result<Option<Vec<u8>>> {
        self.0.get_catalog_item(kind, id, version).await
    }

    async fn last_catalog_ref(&self, kind: &str, id: &str) -> crate::Result<Option<CatalogItemRef>> {
        self.0.last_catalog_ref(kind, id).await
    }

    async fn list_catalog_refs(&self, kind: &str) -> crate::Result<Vec<CatalogItemRef>> {
        self.0.list_catalog_refs(kind).await
    }

    async fn deprecate_catalog_item(&self, kind: &str, id: &str) -> crate::Result<()> {
        self.0.deprecate_catalog_item(kind, id).await
    }

    async fn delete_catalog_item(&self, kind: &str, id: &str) -> crate::Result<()> {
        self.0.delete_catalog_item(kind, id).await
    }
}

/// Implémentation PostgreSQL de [`StoreCatalog`], contre la table
/// `catalog_item` (voir `migrations/0012_catalog.sql`) — même poignée
/// [`PgStore`] que `model`/`tool`/`state_graph` (voir leurs `store.rs`
/// respectifs), mais une seule table partagée par tous les `kind` plutôt
/// qu'une table par type concret : `StoreCatalog` est le support générique
/// utilisé par `catalog::Catalog`, qui ne connaît que des types
/// [`crate::catalog::Catalogable`] arbitraires — sérialisés en JSON (voir
/// `data`), puisque cette table ne connaît elle-même aucun des types
/// concrets qu'elle stocke.
///
/// `insert_catalog_item` ne prend pas de version en entrée : celle-ci est
/// calculée côté SQL (`MAX(version) + 1` pour ce `(kind, id)`, 0 pour une
/// première publication) dans la même requête que l'`INSERT`, pour éviter
/// qu'un aller-retour lecture-puis-écriture depuis Rust ne perde une version
/// concurrente — deux publications concurrentes sur le même `(kind, id)` se
/// résolvent par un conflit de clé primaire (l'une des deux échoue avec
/// `crate::Error`, à charge pour l'appelant de republier) plutôt que
/// silencieusement l'une écrasant l'autre.
#[cfg(feature = "catalog")]
#[async_trait]
impl StoreCatalog for PgStore {
    async fn insert_catalog_item(&self, args: InsertCatalogItem) -> crate::Result<CatalogItemRef> {
        let row = sqlx::query(
            "INSERT INTO catalog_item (kind, id, version, data) \
             SELECT $1, $2, COALESCE(MAX(version), -1) + 1, $3 FROM catalog_item WHERE kind = $1 AND id = $2 \
             RETURNING version",
        )
        .bind(&args.kind)
        .bind(&args.id)
        .bind(&args.data)
        .fetch_one(self.pool())
        .await?;

        Ok(CatalogItemRef {
            kind: args.kind,
            id: args.id,
            version: row.get::<i64, _>("version") as u64,
        })
    }

    async fn get_catalog_item(&self, kind: &str, id: &str, version: u64) -> crate::Result<Option<Vec<u8>>> {
        let row = sqlx::query(
            "SELECT data FROM catalog_item WHERE kind = $1 AND id = $2 AND version = $3 AND deleted_at IS NULL",
        )
        .bind(kind)
        .bind(id)
        .bind(version as i64)
        .fetch_optional(self.pool())
        .await?;
        Ok(row.map(|row| row.get::<Vec<u8>, _>("data")))
    }

    async fn last_catalog_ref(&self, kind: &str, id: &str) -> crate::Result<Option<CatalogItemRef>> {
        let row = sqlx::query(
            "SELECT version FROM catalog_item WHERE kind = $1 AND id = $2 AND NOT deprecated AND deleted_at IS NULL \
             ORDER BY version DESC LIMIT 1",
        )
        .bind(kind)
        .bind(id)
        .fetch_optional(self.pool())
        .await?;

        Ok(row.map(|row| CatalogItemRef {
            kind: kind.to_string(),
            id: id.to_string(),
            version: row.get::<i64, _>("version") as u64,
        }))
    }

    async fn list_catalog_refs(&self, kind: &str) -> crate::Result<Vec<CatalogItemRef>> {
        let rows = sqlx::query(
            "SELECT DISTINCT ON (id) id, version FROM catalog_item \
             WHERE kind = $1 AND NOT deprecated AND deleted_at IS NULL ORDER BY id, version DESC",
        )
        .bind(kind)
        .fetch_all(self.pool())
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| CatalogItemRef {
                kind: kind.to_string(),
                id: row.get::<String, _>("id"),
                version: row.get::<i64, _>("version") as u64,
            })
            .collect())
    }

    async fn deprecate_catalog_item(&self, kind: &str, id: &str) -> crate::Result<()> {
        sqlx::query("UPDATE catalog_item SET deprecated = TRUE WHERE kind = $1 AND id = $2")
            .bind(kind)
            .bind(id)
            .execute(self.pool())
            .await?;
        Ok(())
    }

    async fn delete_catalog_item(&self, kind: &str, id: &str) -> crate::Result<()> {
        sqlx::query("UPDATE catalog_item SET deleted_at = now() WHERE kind = $1 AND id = $2")
            .bind(kind)
            .bind(id)
            .execute(self.pool())
            .await?;
        Ok(())
    }
}
