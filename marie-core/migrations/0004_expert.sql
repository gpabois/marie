-- Catalogue d'experts, copie locale pour récupération à froid (voir
-- expert::catalog::store::ExpertStore) — pas de chiffrement, une déclaration
-- d'expert ne porte aucune information sensible. Attributs décomposés en
-- colonnes concrètes ; `allowed_tools` reste en JSONB (liste de ToolId) plutôt
-- qu'un BYTEA opaque.
CREATE TABLE IF NOT EXISTS marie_experts (
    id TEXT PRIMARY KEY,
    prompt TEXT NOT NULL,
    model_id TEXT NOT NULL,
    allowed_tools JSONB NOT NULL
    CONSTRAINT fk_expert_model FOREIGN KEY (model_id) REFERENCES models(id)
);
