use std::time::{Duration, Instant};

use chacha20poly1305::{AeadCore as _, ChaCha20Poly1305, KeyInit as _, Nonce, aead::{self, Aead}};
use hkdf::{Hkdf, InvalidLength};
use hmac::{Hmac, Mac};
use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use thiserror::Error;
use zeroize::{Zeroize, Zeroizing};

use crate::session::SessionId;

pub type SecretResult<T> = Result<T, SecretError>;
pub type SecretKey = [u8; 32];

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Error)]
pub enum SecretError {
    #[error("HKDF expand failed: {0}")]
    HkdfExpandFailed(InvalidLength),
    #[error("HMAC init failed: {0}")]
    MacInitFailed(hmac::digest::InvalidLength),
    #[error("Encryption failed: {0}")]
    EncryptionFailed(aead::Error),
    #[error("Decryption failed: {0}")]
    DecryptionFailed(aead::Error),
    #[error("Decryption failed: {0}")]
    Utf8DecodingFailed(std::string::FromUtf8Error)
}

pub struct SecretManager {
    // Stockée en mémoire lockée
    master_key: Zeroizing<[u8; 32]>,
    key_rotation_interval: Duration,
    last_rotation: Instant,
}

impl SecretManager {
    pub fn new(master_key_bytes: &[u8; 32]) -> Self {
        let mut key = Zeroizing::new([0u8; 32]);
        key.as_mut().copy_from_slice(master_key_bytes);
        
        Self {
            master_key: key,
            key_rotation_interval: Duration::from_secs(86400), // 24h
            last_rotation: Instant::now(),
        }
    }
    
    /// Dérive une clé par nœud via HKDF
    pub fn derive_node_key(&self, node_id: &PeerId) -> SecretResult<SecretKey> {
        use SecretError::HkdfExpandFailed;

        let hkdf = Hkdf::<Sha256>::new(None, self.master_key.as_ref());
        let mut key = [0u8; 32];

        hkdf.expand(node_id.to_bytes().as_ref(), &mut key)
            .map_err(HkdfExpandFailed)?;

        Ok(key)
    }

    /// Dérive la clé utilisée pour chiffrer un secret *au repos* (voir
    /// `model::catalog::store`), par opposition à [`Self::derive_node_key`]
    /// (spécifique à un pair) ou [`Self::derive_session_key`] (spécifique à
    /// une session) : contexte HKDF fixe, donc identique sur tous les nœuds
    /// partageant la même master key et stable d'un redémarrage à l'autre
    /// (contrairement au `PeerId`, régénéré à chaque démarrage — voir
    /// `network::cp::derive_node_id`). Nécessaire pour qu'un nœud puisse
    /// déchiffrer, à froid, ce qu'il a lui-même persisté avant redémarrage.
    pub fn derive_storage_key(&self) -> SecretResult<SecretKey> {
        use SecretError::HkdfExpandFailed;

        let hkdf = Hkdf::<Sha256>::new(None, self.master_key.as_ref());
        let mut key = [0u8; 32];

        hkdf.expand(b"marie/at-rest-storage/v1", &mut key)
            .map_err(HkdfExpandFailed)?;

        Ok(key)
    }
    
    /// Calcule la preuve d'appartenance au cluster pour `node_id` sur `nonce` :
    /// HMAC-SHA256(clé dérivée pour `node_id`, nonce). Toute instance de
    /// `SecretManager` construite avec la même master key calcule exactement
    /// la même preuve pour un couple `(node_id, nonce)` donné — la master key
    /// elle-même ne transite jamais sur le réseau. Utilisée pour authentifier
    /// automatiquement les nœuds `control plane` du cluster (voir
    /// `network::actor::NetworkActor`).
    pub fn prove_membership(&self, node_id: &PeerId, nonce: &[u8]) -> SecretResult<[u8; 32]> {
        let node_key = self.derive_node_key(node_id)?;
        let mut mac = <HmacSha256 as Mac>::new_from_slice(&node_key).map_err(SecretError::MacInitFailed)?;
        mac.update(nonce);
        Ok(mac.finalize().into_bytes().into())
    }

    /// Vérifie une preuve produite par [`Self::prove_membership`] pour le
    /// couple `(node_id, nonce)`. La comparaison est en temps constant
    /// (déléguée à `hmac::Mac::verify_slice`).
    pub fn verify_membership(&self, node_id: &PeerId, nonce: &[u8], proof: &[u8]) -> SecretResult<bool> {
        let node_key = self.derive_node_key(node_id)?;
        let mut mac = <HmacSha256 as Mac>::new_from_slice(&node_key).map_err(SecretError::MacInitFailed)?;
        mac.update(nonce);
        Ok(mac.verify_slice(proof).is_ok())
    }

    /// Dérive une clé par session
    pub fn derive_session_key(
        &self,
        node_key: &[u8; 32],
        session_id: &SessionId
    ) -> SecretResult<SecretKey> {
          use SecretError::HkdfExpandFailed;

        let hkdf = Hkdf::<Sha256>::new(None, node_key);
        let mut key = [0u8; 32];
        hkdf.expand(session_id.as_bytes(), &mut key)
            .map_err(HkdfExpandFailed)?;

        Ok(key)
    }
    
    /// Chiffre une clé API pour stockage
    pub fn encrypt_api_key(
        &self,
        api_key: &str,
        session_key: &[u8; 32],
    ) -> SecretResult<EncryptedSecret> {
        pub use SecretError::EncryptionFailed;

        let cipher = ChaCha20Poly1305::new(session_key.into());
        let nonce = ChaCha20Poly1305::generate_nonce(&mut rand::thread_rng());
        
        let ciphertext = cipher
            .encrypt(&nonce, api_key.as_bytes())
            .map_err(EncryptionFailed)?;
        
        Ok(EncryptedSecret {
            ciphertext,
            nonce: nonce.to_vec(),
            algorithm: "ChaCha20-Poly1305".to_string(),
        })
    }
    
    /// Déchiffre une clé API (seulement à l'exécution)
    pub fn decrypt_api_key(
        &self,
        encrypted: &EncryptedSecret,
        session_key: &[u8; 32],
    ) -> SecretResult<String> {
        pub use SecretError::{DecryptionFailed, Utf8DecodingFailed};

        let cipher = ChaCha20Poly1305::new(session_key.into());
        let nonce = Nonce::from_slice(&encrypted.nonce);
        
        let plaintext = cipher
            .decrypt(nonce, encrypted.ciphertext.as_ref())
            .map_err(DecryptionFailed)?;
        
        String::from_utf8(plaintext).map_err(Utf8DecodingFailed)
    }
    
    /// Rotation des clés master
    pub fn rotate_master_key(&mut self, new_key: &[u8; 32]) {
        self.master_key.zeroize();
        self.master_key.copy_from_slice(new_key);
        self.last_rotation = Instant::now();
    }

    pub fn needs_rotation(&self) -> bool {
        self.last_rotation.elapsed() >= self.key_rotation_interval
    }
}

impl Drop for SecretManager {
    fn drop(&mut self) {
        self.master_key.zeroize();
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EncryptedSecret {
    pub ciphertext: Vec<u8>,
    pub nonce: Vec<u8>,
    pub algorithm: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct KeyDerivation {
    pub method: String,  // "HKDF-SHA256"
    pub iterations: u32,
    pub salt: Option<Vec<u8>>,
}