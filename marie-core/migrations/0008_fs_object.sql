-- Backend `ObjectStore` (voir vfs::postgres::PostgresObjectStore) adossé à
-- PostgreSQL : une ligne par objet, indexée sur son chemin complet plutôt que
-- décomposée en arborescence — c'est `vfs::inode::PostgresInodeCatalog` qui
-- porte la hiérarchie (`fs_inode`) et n'utilise cet `ObjectStore` que comme
-- espace clé/valeur plat pour le contenu (voir `object_key`).
--
-- `e_tag` vient d'une séquence dédiée plutôt que d'un hash du contenu : il ne
-- sert qu'à détecter les écritures concurrentes (`PutMode::Update`), pas à
-- dédupliquer — une valeur strictement croissante à chaque écriture suffit et
-- évite de recalculer un hash à chaque `put`.
CREATE SEQUENCE IF NOT EXISTS fs_object_etag_seq;

CREATE TABLE IF NOT EXISTS fs_object (
    path TEXT PRIMARY KEY,
    data BYTEA NOT NULL,
    size BIGINT NOT NULL,
    e_tag BIGINT NOT NULL DEFAULT nextval('fs_object_etag_seq'),
    last_modified TIMESTAMPTZ NOT NULL DEFAULT now()
);
