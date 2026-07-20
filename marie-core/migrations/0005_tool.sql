-- Catalogue de tools, copie locale pour récupération à froid (voir
-- tools::catalog::store::ToolStore) — pas de chiffrement, une déclaration de
-- tool ne porte aucune information sensible. `name` (voir ToolId) sert à la
-- fois de clé primaire et d'identifiant : contrairement à `expert`/`model`,
-- un tool ne porte pas de champ `id` distinct. `parameters_schema` reste en
-- JSONB (schéma arbitraire fourni par l'appelant) plutôt qu'un BYTEA opaque —
-- inspectable/requêtable via les opérateurs JSON de Postgres.
CREATE TABLE IF NOT EXISTS tool (
    name TEXT PRIMARY KEY,
    description TEXT NOT NULL,
    parameters_schema JSONB NOT NULL
);
