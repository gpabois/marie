-- Copie persistée d'un workspace (voir workspace::store::WorkspaceStore) —
-- même principe que la table `session` : depuis que le workspace est une
-- struct concrète servie par un unique pair propriétaire (et plus un document
-- CRDT `yrs` fusionné entre holders), chaque collection (`sessions`/`vars`)
-- a sa propre colonne JSONB lisible plutôt qu'un blob opaque.
--
-- `created_at`/`last_updated_at` sont posés par `PgStore::insert`/`replace`
-- (voir leur doc), jamais par l'appelant — le `DEFAULT now()` n'est qu'un
-- filet de sécurité si une ligne était un jour insérée hors de ce chemin.
CREATE TABLE IF NOT EXISTS workspace (
    id TEXT PRIMARY KEY,
    sessions JSONB NOT NULL,
    vars JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
