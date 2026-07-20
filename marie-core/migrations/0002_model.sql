-- Catalogue de modèles, copie locale chiffrée pour récupération à froid (voir
-- model::catalog::store::ModelStore) — attributs décomposés en colonnes
-- concrètes plutôt qu'un blob JSON opaque, à l'exception de `api_key_*` :
-- la clé API est déjà chiffrée (voir model::catalog::store::StoredModel::encrypt),
-- jamais en clair sur disque, et son contenu chiffré n'a de sens que
-- déchiffré — rien à décomposer de plus fin que ciphertext/nonce/algorithm.
-- `kind` porte le discriminant de l'enum `Model`/`EncryptedModel` (une seule
-- valeur possible aujourd'hui, 'openai_compatible') : les colonnes
-- spécifiques à d'autres variantes futures resteraient NULL pour celle-ci,
-- comme le seraient des champs `Option` sur la variante Rust correspondante.
CREATE TABLE IF NOT EXISTS model (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    base_url TEXT NOT NULL,
    client_id TEXT NOT NULL,
    api_key_ciphertext BYTEA NOT NULL,
    api_key_nonce BYTEA NOT NULL,
    api_key_algorithm TEXT NOT NULL,
    model_name TEXT NOT NULL,
    system_prompt TEXT
);
