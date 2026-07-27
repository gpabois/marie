use async_trait::async_trait;
use sqlx::Row as _;
use typed_builder::TypedBuilder;

use crate::catalog::Catalogable;
use crate::store::PgStore;

use super::CatalogItemRef;

#[derive(TypedBuilder)]
pub struct InsertCatalogItem<C> where C: Catalogable {
    #[builder(setter(transform = |x: impl ToString| x.to_string()))]
    id: String,
    #[builder(setter(transform = |x: impl ToString| x.to_string()))]
    kind: String,
    data: C,
}

#[async_trait]
pub trait CatalogStore {
    type Error;

    async fn insert_catalog_item<C>(&self, args: InsertCatalogItem<C>) -> Result<CatalogItemRef, Self::Error> where C: Catalogable;
    async fn get_catalog_item(&self, kind: &str, id: &str, version: u64) -> Result<Option<Vec<u8>>, Self::Error>;
    async fn last_catalog_ref(&self, kind: &str, id: &str) -> Result<Option<CatalogItemRef>, Self::Error>;
    /// Référence active la plus récente de chaque `id` connu pour `kind` —
    /// utilisé par `Catalog::list` pour énumérer un catalogue complet sans
    /// connaître ses `id` à l'avance.
    async fn list_catalog_refs(&self, kind: &str) -> Result<Vec<CatalogItemRef>, Self::Error>;
    async fn deprecate_catalog_item(&self, kind: &str, id: &str) -> Result<(), Self::Error>;
    /// Soft-delete : retire `id` de toute lecture du catalogue (y compris
    /// `get_catalog_item` par version explicite, contrairement à
    /// [`Self::deprecate_catalog_item`]) sans supprimer physiquement ses
    /// lignes (voir `migrations/0013_catalog_soft_delete.sql`).
    async fn delete_catalog_item(&self, kind: &str, id: &str) -> Result<(), Self::Error>;
}

/// Implémentation PostgreSQL de [`CatalogStore`], contre la table
/// `catalog_item` (voir `migrations/0012_catalog.sql`) — même poignée
/// [`PgStore`] que `model`/`tool`/`state_graph` (voir leurs `store.rs`
/// respectifs), mais une seule table partagée par tous les `kind` plutôt
/// qu'une table par type concret : `CatalogStore` est le support générique
/// utilisé par `catalog::Catalog`, qui ne connaît que des types
/// [`Catalogable`] arbitraires — sérialisés en JSON (voir `data`), puisque
/// cette table ne connaît elle-même aucun des types concrets qu'elle stocke.
///
/// `insert_catalog_item` ne prend pas de version en entrée : celle-ci est
/// calculée côté SQL (`MAX(version) + 1` pour ce `(kind, id)`, 0 pour une
/// première publication) dans la même requête que l'`INSERT`, pour éviter
/// qu'un aller-retour lecture-puis-écriture depuis Rust ne perde une version
/// concurrente — deux publications concurrentes sur le même `(kind, id)` se
/// résolvent par un conflit de clé primaire (l'une des deux échoue avec
/// `Self::Error`, à charge pour l'appelant de republier) plutôt que
/// silencieusement l'une écrasant l'autre.
#[async_trait]
impl CatalogStore for PgStore {
    type Error = sqlx::Error;

    async fn insert_catalog_item<C>(&self, args: InsertCatalogItem<C>) -> Result<CatalogItemRef, Self::Error>
    where
        C: Catalogable,
    {
        let data = serde_json::to_vec(&args.data).expect("un Catalogable se sérialise toujours en JSON");

        let row = sqlx::query(
            "INSERT INTO catalog_item (kind, id, version, data) \
             SELECT $1, $2, COALESCE(MAX(version), -1) + 1, $3 FROM catalog_item WHERE kind = $1 AND id = $2 \
             RETURNING version",
        )
        .bind(&args.kind)
        .bind(&args.id)
        .bind(&data)
        .fetch_one(self.pool())
        .await?;

        Ok(CatalogItemRef {
            kind: args.kind,
            id: args.id,
            version: row.get::<i64, _>("version") as u64,
        })
    }

    async fn get_catalog_item(&self, kind: &str, id: &str, version: u64) -> Result<Option<Vec<u8>>, Self::Error> {
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

    async fn last_catalog_ref(&self, kind: &str, id: &str) -> Result<Option<CatalogItemRef>, Self::Error> {
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

    async fn list_catalog_refs(&self, kind: &str) -> Result<Vec<CatalogItemRef>, Self::Error> {
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

    async fn deprecate_catalog_item(&self, kind: &str, id: &str) -> Result<(), Self::Error> {
        sqlx::query("UPDATE catalog_item SET deprecated = TRUE WHERE kind = $1 AND id = $2")
            .bind(kind)
            .bind(id)
            .execute(self.pool())
            .await?;
        Ok(())
    }

    async fn delete_catalog_item(&self, kind: &str, id: &str) -> Result<(), Self::Error> {
        sqlx::query("UPDATE catalog_item SET deleted_at = now() WHERE kind = $1 AND id = $2")
            .bind(kind)
            .bind(id)
            .execute(self.pool())
            .await?;
        Ok(())
    }
}