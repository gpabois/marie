-- Table d'alias d'un VFS (voir vfs::alias::PostgresAliasCatalog) : une ligne
-- par alias `from -> to` plutôt qu'un blob par scope, pour éviter une course
-- lecture-modification-écriture si deux nœuds posent des alias différents au
-- même moment sur le même scope (voir `PostgresAliasCatalog::set`, qui
-- s'appuie sur `ON CONFLICT (scope, from_path)` ci-dessous).
--
-- `scope` distingue les alias d'un workspace de ceux de ses sessions
-- (`workspace:{id}` / `session:{id}`, voir `PostgresAliasCatalog::for_workspace`/
-- `for_session`) — même table pour tous les scopes, comme `fs_inode`, plutôt
-- qu'une table par scope.
CREATE TABLE IF NOT EXISTS fs_alias (
    scope TEXT NOT NULL,
    from_path TEXT NOT NULL,
    to_path TEXT NOT NULL,
    PRIMARY KEY (scope, from_path)
);
