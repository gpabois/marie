-- Contenu CRDT durable des workspaces (voir
-- persistency::workspace::WorkspaceStore) — même principe que la table
-- `session` (voir 0003_session.sql).
CREATE TABLE IF NOT EXISTS workspace (
    id TEXT PRIMARY KEY,
    value BYTEA NOT NULL
);
