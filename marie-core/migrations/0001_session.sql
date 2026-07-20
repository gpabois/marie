-- Copie persistée d'une session (voir session::store::SessionStore) — chaque
-- collection de `Session` (frames/graphs/orchestrations/hitls/logs/vars) a sa
-- propre colonne JSONB plutôt qu'un blob unique : contrairement à l'ancien
-- contenu de session (diff CRDT `yrs`, remplacé par cette table), cette
-- `Session`-ci est un enregistrement classique remplacé en bloc à chaque
-- mutation (voir la doc de `Session`), donc décomposable colonne à colonne
-- comme `expert`/`model`/`tool`.
--
-- `created_at`/`last_updated_at` sont posés par `PgStore::insert`/`replace`
-- (voir leur doc), jamais par l'appelant — le `DEFAULT now()` n'est qu'un
-- filet de sécurité si une ligne était un jour insérée hors de ce chemin.
CREATE TABLE IF NOT EXISTS session (
    id TEXT PRIMARY KEY,
    frames JSONB NOT NULL,
    graphs JSONB NOT NULL,
    orchestrations JSONB NOT NULL,
    hitls JSONB NOT NULL,
    logs JSONB NOT NULL,
    vars JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
