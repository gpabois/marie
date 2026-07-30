-- Remplace `frames` (l'arbre de frames sérialisé en un seul bloc JSONB, voir
-- `0014_session_tree.sql`) par `root_frame` : depuis `0015_session_frame.sql`,
-- chaque frame vit dans sa propre ligne de `marie_session_frames`
-- (voir `session::frames::FrameTree`, adossé à un cache Postgres par noeud
-- plutôt qu'à un blob unique), donc `marie_sessions` (voir
-- `session::model::Session`) n'a plus besoin de porter que l'identifiant de
-- la racine de l'arbre, pas l'arbre entier.
ALTER TABLE marie_sessions DROP COLUMN IF EXISTS frames;
ALTER TABLE marie_sessions ADD COLUMN IF NOT EXISTS root_frame TEXT;
