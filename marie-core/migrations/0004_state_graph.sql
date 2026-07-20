-- Catalogue de graphes d'états, copie locale pour récupération à froid (voir
-- session::state::catalog::store::StateGraphStore) — pas de chiffrement, une
-- déclaration de graphe ne porte aucune information sensible. Attributs
-- décomposés en colonnes concrètes ; `nodes`/`edges` restent en JSONB
-- (collections structurées, voir session::state::{Node, Edge}) plutôt qu'un
-- BYTEA opaque.
CREATE TABLE IF NOT EXISTS state_graph (
    id TEXT PRIMARY KEY,
    entry TEXT NOT NULL,
    nodes JSONB NOT NULL,
    edges JSONB NOT NULL
);
