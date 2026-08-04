use marie::secret::{KeyEpoch, SecretKey};

const ENV_VAR: &str = "MARIE_SECRET";

/// Charge la clé maître du cluster depuis `MARIE_SECRET` (hex, 64
/// caractères = 32 octets) si présente, sinon en génère une aléatoire pour
/// la durée du process — une instance `connect local` n'a par construction
/// aucun pair avec qui s'accorder sur un secret partagé (pas de réseau),
/// une clé éphémère suffit donc : elle ne sert qu'à prouver l'appartenance
/// au cluster entre pairs (voir la doc de `SecretManager` dans
/// `marie-core`), un rôle sans objet ici.
pub fn load_or_generate_epoch() -> (Vec<(KeyEpoch, SecretKey)>, KeyEpoch) {
    let key = match std::env::var(ENV_VAR) {
        Ok(hex_str) => parse_hex_key(&hex_str).unwrap_or_else(|| {
            eprintln!("{ENV_VAR} invalide (attendu 64 caractères hexadécimaux) — génération d'une clé éphémère");
            random_key()
        }),
        Err(_) => random_key(),
    };

    (vec![(0, key)], 0)
}

fn random_key() -> SecretKey {
    rand::random()
}

fn parse_hex_key(s: &str) -> Option<SecretKey> {
    let bytes = hex::decode(s.trim()).ok()?;
    bytes.try_into().ok()
}
