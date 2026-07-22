-- Arborescence de fichiers (voir vfs::inode::PostgresInodeCatalog) : une
-- table unique `fs_inode`, partagée par tous les scopes (workspace, session,
-- ...) — chaque scope n'est qu'un sous-arbre de cette même table, repéré par
-- son propre `root_id`, jamais une table ou un espace de clés séparé (voir la
-- doc de `PostgresInodeCatalog`).
--
-- `kind` porte le discriminant de l'enum `InodeKind` (0 = Dir, 1 = File) —
-- `object_key` n'a de sens que pour `kind = 1` (voir `Inode::object_key`),
-- clé opaque vers l'`ObjectStore` associé (ex: `vfs::postgres::PostgresObjectStore`)
-- qui porte le contenu ; cette table ne connaît que la hiérarchie, jamais les
-- octets.
--
-- `ON DELETE CASCADE` sur `parent_id` fait qu'une suppression récursive
-- (`PostgresInodeCatalog::remove`) ne demande qu'un seul `DELETE` sur la
-- racine du sous-arbre visé ; l'appelant récupère séparément les
-- `object_key` du sous-arbre avant cet appel (la cascade ne dit rien à
-- l'`ObjectStore`, voir la doc de `InodeCatalog::remove`).
CREATE TABLE IF NOT EXISTS fs_inode (
    id BIGSERIAL PRIMARY KEY,
    parent_id BIGINT REFERENCES fs_inode(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    kind SMALLINT NOT NULL,
    object_key TEXT,
    size BIGINT NOT NULL DEFAULT 0,
    UNIQUE (parent_id, name)
);

-- Un seul nœud peut être racine globale de la table (`parent_id IS NULL`) —
-- celle sous laquelle `PostgresInodeCatalog::for_workspace`/`for_session`
-- dérivent chacun leur propre `root_id`. Index unique partiel sur
-- l'expression constante `(1)` plutôt que sur une colonne : c'est la
-- contrainte "au plus une ligne" qui protège `ensure_global_root` contre la
-- course de deux nœuds démarrant en même temps (voir son commentaire).
CREATE UNIQUE INDEX IF NOT EXISTS fs_inode_root_idx ON fs_inode ((1)) WHERE parent_id IS NULL;
