-- Stockage générique et versionné d'éléments de catalogue (voir
-- catalog::store::CatalogStore) : contrairement aux catalogues dédiés
-- (model/tool/expert, chacun sa propre table typée), celui-ci sert de
-- support bas niveau à `catalog::Catalog`, qui multiplexe plusieurs types
-- `Catalogable` sur une unique table via leur discriminant `kind` (voir
-- `Catalogable::KIND`) — `data` reste un BYTEA opaque (sérialisation du type
-- appelant) puisque cette table ne connaît elle-même aucun des types
-- concrets qu'elle stocke.
--
-- Clé primaire (kind, id, version) : chaque publication d'un `id` insère une
-- nouvelle ligne plutôt que de remplacer la précédente, pour conserver
-- l'historique des versions (voir `CatalogStore::get_catalog_item`, qui
-- adresse une version précise, potentiellement encore référencée par un
-- holder même après une nouvelle publication).
--
-- `deprecated` marque un `id` comme retiré du catalogue actif (voir
-- `CatalogStore::deprecate_catalog_item`) sans supprimer ses lignes :
-- `last_catalog_ref` les exclut de sa recherche, mais elles restent lisibles
-- via `get_catalog_item`. `deleted_at` est plus strict : il retire l'`id` de
-- toute lecture du catalogue, y compris par version explicite (voir
-- `CatalogStore::delete_catalog_item`), sans supprimer physiquement la ligne
-- (audit / restauration éventuelle).
CREATE TABLE IF NOT EXISTS catalog_item (
    kind TEXT NOT NULL,
    id TEXT NOT NULL,
    version BIGINT NOT NULL,
    data BYTEA NOT NULL,
    deprecated BOOLEAN NOT NULL DEFAULT FALSE,
    deleted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (kind, id, version)
);

-- Sert `last_catalog_ref`/`list_catalog_refs` (recherche de la version
-- active la plus récente pour un `(kind, id)` donné) sans scanner les lignes
-- dépréciées/supprimées.
CREATE INDEX IF NOT EXISTS catalog_item_active_idx ON catalog_item (kind, id, version DESC) WHERE NOT deprecated AND deleted_at IS NULL;
