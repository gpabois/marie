-- Copie persistée d'une session (voir session::store::SessionStore) — chaque
-- collection de `Session` (frames/snapshots/hitls) a sa propre table (voir
-- 0005/0006/0007) plutôt qu'un blob unique : contrairement à l'ancien
-- contenu de session (diff CRDT `yrs`), cette `Session`-ci est un
-- enregistrement classique remplacé en bloc à chaque mutation (voir la doc
-- de `Session`), donc décomposable table à table comme `expert`/`model`/
-- `tool`.
--
-- `root_frame` référence la racine de l'arbre de frames — chaque frame vit
-- dans sa propre ligne de `marie_session_frames` (voir
-- `session::frames::FrameTree`), cette table ne porte donc que l'identifiant
-- de la racine, pas l'arbre entier. `status` (voir `session::SessionStatus`)
-- est en JSONB plutôt qu'un TEXT à discriminant seul : `SessionStatus::Failed`
-- porte un message d'erreur, pas seulement un nom de variante.
--
-- `created_at`/`last_updated_at` sont posés par `StoreSession::upsert_session`
-- (voir sa doc), jamais par l'appelant — le `DEFAULT now()` n'est qu'un
-- filet de sécurité si une ligne était un jour insérée hors de ce chemin.
CREATE TABLE IF NOT EXISTS marie_sessions (
    id TEXT PRIMARY KEY,
    root_frame TEXT,
    status JSONB NOT NULL DEFAULT '"Pending"'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
