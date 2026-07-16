use std::{ops::{Deref, DerefMut}, sync::{Arc, Mutex}, time::{Duration, Instant}};

use chacha20poly1305::{AeadCore as _, ChaCha20Poly1305, KeyInit as _, Nonce, aead::{self, Aead}};
use hkdf::{Hkdf, InvalidLength};
use hmac::{Hmac, Mac};
use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use thiserror::Error;
use zeroize::{Zeroize, Zeroizing};


pub type SecretResult<T> = Result<T, SecretError>;
pub type SecretKey = [u8; 32];

type HmacSha256 = Hmac<Sha256>;

pub trait Encryptable : Sized {
    type Encrypted;

    fn encrypt<C>(self, codec: &C) -> SecretResult<Self::Encrypted> where C: SecretCodec;
    fn decrypt<C>(encrypted: Self::Encrypted, codec: &C) -> SecretResult<Self> where C: SecretCodec;
}


pub trait SecretCodec {
    fn encrypt(&self, key: impl AsRef<[u8]>) -> SecretResult<EncryptedSecret>;
    fn decrypt(&self, encrypted: EncryptedSecret) -> SecretResult<Vec<u8>>;

    fn encrypt_str(&self, key: impl ToString) -> SecretResult<EncryptedSecret> {
        let key = key.to_string();
        self.encrypt(key)
    }

    fn decrypt_str(&self, encrypted: EncryptedSecret) -> SecretResult<String> {
        self
        .decrypt(encrypted)
        .and_then(|bytes| String::from_utf8(bytes).map_err(SecretError::Utf8DecodingFailed))
    }
}

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

#[derive(Clone)]
pub struct PeerSecretManager {
    global: SecretManager,
    peer_key: [u8; 32]
}

impl SecretCodec for PeerSecretManager {
    fn encrypt(&self, secret: impl AsRef<[u8]>) -> SecretResult<EncryptedSecret> {
        use SecretError::EncryptionFailed;

        let cipher = ChaCha20Poly1305::new(&self.peer_key.into());
        let nonce = ChaCha20Poly1305::generate_nonce(&mut rand::thread_rng());
        
        let ciphertext = cipher
            .encrypt(&nonce, secret.as_ref())
            .map_err(EncryptionFailed)?;
        
        Ok(EncryptedSecret {
            ciphertext,
            nonce: nonce.to_vec(),
            algorithm: "ChaCha20-Poly1305".to_string(),
        })
    }

    fn decrypt(&self, encrypted_secret: EncryptedSecret) -> SecretResult<Vec<u8>> {
        pub use SecretError::{DecryptionFailed};

        let cipher = ChaCha20Poly1305::new(&self.peer_key.into());
        let nonce = Nonce::from_slice(&encrypted_secret.nonce);
        
        let plaintext = cipher
            .decrypt(nonce, encrypted_secret.ciphertext.as_ref())
            .map_err(DecryptionFailed)?;
        
        Ok(plaintext)      
    }
}

impl PeerSecretManager {
    pub fn encrypt(&self, secret: &str) -> SecretResult<EncryptedSecret> {
        use SecretError::EncryptionFailed;

        let cipher = ChaCha20Poly1305::new(&self.peer_key.into());
        let nonce = ChaCha20Poly1305::generate_nonce(&mut rand::thread_rng());
        
        let ciphertext = cipher
            .encrypt(&nonce, secret.as_ref())
            .map_err(EncryptionFailed)?;
        
        Ok(EncryptedSecret {
            ciphertext,
            nonce: nonce.to_vec(),
            algorithm: "ChaCha20-Poly1305".to_string(),
        })
    }

    pub fn decrypt(&self, encrypted_secret: &EncryptedSecret) -> SecretResult<String> {
        pub use SecretError::{DecryptionFailed, Utf8DecodingFailed};

        let cipher = ChaCha20Poly1305::new(&self.peer_key.into());
        let nonce = Nonce::from_slice(&encrypted_secret.nonce);
        
        let plaintext = cipher
            .decrypt(nonce, encrypted_secret.ciphertext.as_ref())
            .map_err(DecryptionFailed)?;
        
        String::from_utf8(plaintext).map_err(Utf8DecodingFailed)        
    }
}

struct Inner {
    // Stockée en mémoire lockée
    master_key: Zeroizing<[u8; 32]>,
    key_rotation_interval: Duration,
    last_rotation: Instant,
}

#[derive(Clone)]
pub struct SecretManager(Arc<Mutex<Inner>>);

impl SecretManager {
    pub fn new(master_key_bytes: &[u8; 32]) -> Self {
        let mut key = Zeroizing::new([0u8; 32]);
        key.as_mut().copy_from_slice(master_key_bytes);
        
        let inner = Inner {
            master_key: key,
            key_rotation_interval: Duration::from_secs(86400), // 24h
            last_rotation: Instant::now(),
        };

        Self(Arc::new(Mutex::new(inner)))
    }
    
    pub fn for_peer(&self, peer_id: PeerId) -> PeerSecretManager {
        let peer_key = self.derive_node_key(&peer_id).unwrap();
        PeerSecretManager { global: self.clone(), peer_key }
    }

    /// Dérive une clé par nœud via HKDF
    pub fn derive_node_key(&self, node_id: &PeerId) -> SecretResult<SecretKey> {
        use SecretError::HkdfExpandFailed;

        let ref_mk = &self.0.lock().unwrap().master_key;
        let hkdf = Hkdf::<Sha256>::new(None, ref_mk.as_ref());
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

        let ref_mk = &self.0.lock().unwrap().master_key;

        let hkdf = Hkdf::<Sha256>::new(None, ref_mk.as_ref());
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

    /// Rotation des clés master
    pub fn rotate_master_key(&self, new_key: &[u8; 32]) {
        let mut guard = self.0.lock().unwrap();

        guard.deref_mut().master_key.zeroize();
        guard.deref_mut().master_key.copy_from_slice(new_key);
        guard.deref_mut().last_rotation = Instant::now();
    }

    pub fn needs_rotation(&self) -> bool {
        let guard = self.0.lock().unwrap();
        guard.deref().last_rotation.elapsed() >= guard.deref().key_rotation_interval
    }
}

impl Drop for SecretManager {
    fn drop(&mut self) {
        self.0.lock().unwrap().deref_mut().master_key.zeroize();
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