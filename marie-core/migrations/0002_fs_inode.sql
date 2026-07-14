-- Arborescence de fichiers du VFS (voir persistency::inode::InodeCatalog) :
-- une table unique, partagée par tous les scopes (workspace, session), chacun
-- résolu depuis sa propre racine (voir PostgresInodeCatalog::for_workspace/
-- for_session) plutôt qu'un espace de stockage séparé par scope.
CREATE TABLE IF NOT EXISTS fs_inode (
    id BIGSERIAL PRIMARY KEY,
    parent_id BIGINT NULL REFERENCES fs_inode(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    kind SMALLINT NOT NULL,
    object_key TEXT NULL,
    size BIGINT NOT NULL DEFAULT 0,
    UNIQUE (parent_id, name)
);

-- `UNIQUE (parent_id, name)` ne suffit pas à garantir une racine unique :
-- Postgres ne considère jamais deux NULL comme égaux dans une contrainte
-- d'unicité classique. Cet index partiel, lui, porte sur une expression
-- constante et ne s'applique qu'aux lignes parent_id IS NULL — au plus une
-- ligne racine peut donc exister.
CREATE UNIQUE INDEX IF NOT EXISTS fs_inode_root_idx ON fs_inode ((1)) WHERE parent_id IS NULL;
