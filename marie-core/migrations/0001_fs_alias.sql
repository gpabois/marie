-- Table d'alias du VFS (voir persistency::alias::AliasCatalog) : une entrée
-- scope/from_path -> to_path par ligne, plutôt qu'un blob par scope, pour
-- éviter une course lecture-modification-écriture entre deux nœuds posant des
-- alias différents au même moment sur le même scope.
CREATE TABLE IF NOT EXISTS fs_alias (
    scope TEXT NOT NULL,
    from_path TEXT NOT NULL,
    to_path TEXT NOT NULL,
    PRIMARY KEY (scope, from_path)
);
