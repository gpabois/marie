-- Catalogue d'experts, copie locale pour récupération à froid (voir
-- expert::catalog::store::ExpertStore) — pas de chiffrement, une déclaration
-- d'expert ne porte aucune information sensible. Attributs décomposés en
-- colonnes concrètes ; `allowed_tools` reste en JSONB (liste de ToolId) plutôt
-- qu'un BYTEA opaque.
CREATE TABLE IF NOT EXISTS expert (
    id TEXT PRIMARY KEY,
    prompt TEXT NOT NULL,
    model_id TEXT NOT NULL,
    allowed_tools JSONB NOT NULL
);
