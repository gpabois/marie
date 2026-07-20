-- Epoch de la master key ayant chiffré api_key_ciphertext (voir
-- secret::KeyEpoch / secret::EncryptedSecret::key_epoch) : permet de savoir
-- quelle epoch redériver au déchiffrement pendant une rotation de master
-- key (voir model::catalog::rotate). DEFAULT 0 = DEFAULT_EPOCH, donc toutes
-- les lignes déjà présentes restent lisibles sans backfill.
ALTER TABLE model ADD COLUMN IF NOT EXISTS api_key_epoch INTEGER NOT NULL DEFAULT 0;
