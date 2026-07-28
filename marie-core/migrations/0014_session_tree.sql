-- Ajoute la donnée de session elle-même (l'arbre de frames, voir
-- `session::tree::FrameTree`) à `marie_sessions` (voir `0002_session.sql`,
-- qui ne portait que les colonnes d'identification/horodatage) — colonne
-- ajoutée séparément plutôt qu'en réécrivant la migration existante, déjà
-- appliquée, pour rester idempotente.
ALTER TABLE marie_sessions ADD COLUMN IF NOT EXISTS frames JSONB NOT NULL DEFAULT '{"root":null,"arena":{}}'::jsonb;
