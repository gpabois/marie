-- Stockage générique de secrets chiffrés au repos (voir
-- secret::store::SecretStore) — `ciphertext`/`nonce`/`algorithm` sont déjà le
-- résultat d'un chiffrement (voir SecretManager::derive_storage_key), jamais
-- de secret en clair sur cette table. `key_epoch` (voir secret::KeyEpoch)
-- permet de savoir quelle epoch redériver au déchiffrement pendant une
-- rotation de master key.
CREATE TABLE IF NOT EXISTS secret (
    id TEXT PRIMARY KEY,
    key_epoch INTEGER NOT NULL,
    ciphertext BYTEA NOT NULL,
    nonce BYTEA NOT NULL,
    algorithm TEXT NOT NULL
);
