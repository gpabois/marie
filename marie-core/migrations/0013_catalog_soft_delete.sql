-- Soft-delete d'un élément de catalogue (voir
-- catalog::store::CatalogStore::delete_catalog_item) : plus strict que
-- `deprecated` (une version simplement supplantée par une plus récente,
-- mais dont le contenu reste lisible pour quiconque la référence encore
-- explicitement via `get_catalog_item`) — `deleted_at` retire l'`id` de
-- toute lecture du catalogue, y compris par version explicite, sans
-- supprimer physiquement la ligne (audit / restauration éventuelle).
ALTER TABLE catalog_item ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;

-- Remplace l'index de 0012_catalog.sql : la recherche de version active
-- (`last_catalog_ref`/`list_catalog_refs`) filtre désormais aussi sur
-- `deleted_at IS NULL`, l'index doit porter le même prédicat pour rester
-- utilisable par le planificateur.
DROP INDEX IF EXISTS catalog_item_active_idx;
CREATE INDEX IF NOT EXISTS catalog_item_active_idx ON catalog_item (kind, id, version DESC) WHERE NOT deprecated AND deleted_at IS NULL;
