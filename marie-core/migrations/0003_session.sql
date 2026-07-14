-- Contenu CRDT durable des sessions (voir persistency::session::SessionStore)
-- : `value` porte un diff yrs complet (encode_diff_v1 depuis un vecteur
-- d'état vide), pas des colonnes décomposées — le contenu n'a de sens que
-- rejoué par `yrs`, jamais interrogé directement en SQL.
CREATE TABLE IF NOT EXISTS session (
    id TEXT PRIMARY KEY,
    value BYTEA NOT NULL
);
