-- Catalogue de tools, copie locale pour récupération à froid (voir
-- tools::catalog::store::ToolStore) — pas de chiffrement, une déclaration de
-- tool ne porte aucune information sensible. Attributs décomposés en colonnes
-- concrètes ; `parameters_schema` reste un JSON (schéma arbitraire fourni par
-- l'appelant, voir tools::ToolSignature) mais en JSONB natif plutôt qu'un
-- BYTEA opaque — inspectable/requêtable via les opérateurs JSON de Postgres.
CREATE TABLE IF NOT EXISTS tool (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    parameters_schema JSONB NOT NULL,
    scope TEXT NOT NULL
);
