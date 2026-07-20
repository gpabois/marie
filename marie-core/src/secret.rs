use std::{collections::BTreeMap, sync::{Arc, Mutex}};

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

/// Identifiant de génération d'une master key (voir [`SecretManager::with_epochs`]) :
/// pendant une rotation, plusieurs epochs coexistent en mémoire — l'ancienne
/// reste nécessaire pour déchiffrer ce qui a été chiffré avant la bascule et
/// pour vérifier les preuves d'appartenance émises par un pair pas encore
/// basculé, tant qu'une passe de re-chiffrement (voir
/// `model::catalog::rotate`) n'a pas migré ces données vers la nouvelle.
/// Un simple compteur suivi par l'opérateur, pas un identifiant généré par
/// le crate (la master key elle-même est déjà pré-partagée hors crate).
pub type KeyEpoch = u32;

/// Epoch utilisée par [`SecretManager::new`] pour un déploiement à clé
/// unique, non rotatif.
pub const DEFAULT_EPOCH: KeyEpoch = 0;

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
    Utf8DecodingFailed(std::string::FromUtf8Error),
    #[error("epoch de clé inconnue ou déjà retirée : {0}")]
    UnknownEpoch(KeyEpoch),
    #[error("impossible de retirer l'epoch courante ({0}) : basculer vers une autre epoch au préalable (voir SecretManager::set_current_epoch)")]
    CannotRetireCurrentEpoch(KeyEpoch),
    #[error("impossible de retirer la dernière epoch restante : un SecretManager sans aucune clé ne peut plus rien chiffrer/déchiffrer")]
    LastEpoch,
    #[error("l'epoch courante ({0}) doit figurer parmi les clés fournies à with_epochs")]
    CurrentEpochNotProvided(KeyEpoch),
    #[error("au moins une epoch de clé est requise")]
    NoKeysProvided,
}

/// Clé dérivée pour un pair précis (voir [`SecretManager::for_peer`]) : seul
/// ce pair peut déchiffrer ce qui a été chiffré avec (chiffrement en
/// transit, ex. une clé API de modèle envoyée à un worker), par opposition
/// à [`EpochSecretKey`] (chiffrement au repos, déchiffrable par n'importe
/// quel appelant qui redérive la même clé). Porte l'epoch qui l'a produite
/// (voir doc de champ `epoch`) pour que l'`EncryptedSecret` résultant reste
/// déchiffrable après un changement d'epoch courante.
#[derive(Clone)]
pub struct PeerSecretManager {
    global: SecretManager,
    epoch: KeyEpoch,
    peer_key: [u8; 32],
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
            key_epoch: self.epoch,
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
        SecretCodec::encrypt(self, secret)
    }

    pub fn decrypt(&self, encrypted_secret: &EncryptedSecret) -> SecretResult<String> {
        SecretCodec::decrypt_str(self, encrypted_secret.clone())
    }
}

/// Chiffrement au repos avec une clé brute dérivée (voir
/// [`SecretManager::derive_storage_key`]) — contrairement à
/// [`PeerSecretManager`], pas de notion de pair destinataire : n'importe
/// quel appelant qui dérive la même clé (donc, en pratique, le nœud qui a
/// écrit la donnée) peut déchiffrer, ce qui est exactement ce que veut un
/// store local (voir `model::catalog::store::StoredModel::{encrypt,decrypt}`).
/// Ne porte aucune epoch (une clé brute n'a pas ce contexte) : l'epoch
/// stampée dans l'[`EncryptedSecret`] produit vaut toujours
/// [`DEFAULT_EPOCH`] ici — les appelants qui ont besoin du bon tagging
/// d'epoch doivent passer par [`EpochSecretKey`], qui l'écrase après coup.
impl SecretCodec for SecretKey {
    fn encrypt(&self, secret: impl AsRef<[u8]>) -> SecretResult<EncryptedSecret> {
        use SecretError::EncryptionFailed;

        let cipher = ChaCha20Poly1305::new(self.into());
        let nonce = ChaCha20Poly1305::generate_nonce(&mut rand::thread_rng());

        let ciphertext = cipher.encrypt(&nonce, secret.as_ref()).map_err(EncryptionFailed)?;

        Ok(EncryptedSecret { key_epoch: DEFAULT_EPOCH, ciphertext, nonce: nonce.to_vec(), algorithm: "ChaCha20-Poly1305".to_string() })
    }

    fn decrypt(&self, encrypted: EncryptedSecret) -> SecretResult<Vec<u8>> {
        use SecretError::DecryptionFailed;

        let cipher = ChaCha20Poly1305::new(self.into());
        let nonce = Nonce::from_slice(&encrypted.nonce);

        cipher.decrypt(nonce, encrypted.ciphertext.as_ref()).map_err(DecryptionFailed)
    }
}

/// Clé de chiffrement au repos liée à l'epoch qui l'a produite (voir
/// [`SecretManager::derive_storage_key`]/[`SecretManager::derive_storage_key_for_epoch`]) :
/// stampe systématiquement cette epoch dans chaque [`EncryptedSecret`]
/// qu'elle produit, pour que [`SecretManager::derive_storage_key_for_epoch`]
/// puisse plus tard sélectionner la bonne clé au déchiffrement — y compris
/// après que l'epoch courante du nœud a changé (voir le runbook de rotation
/// sur [`SecretManager`]).
pub struct EpochSecretKey {
    pub epoch: KeyEpoch,
    key: SecretKey,
}

/// N'affiche jamais `key` (matériel de clé sensible) — seule l'epoch est
/// pertinente pour du diagnostic/logging.
impl std::fmt::Debug for EpochSecretKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EpochSecretKey").field("epoch", &self.epoch).finish_non_exhaustive()
    }
}

impl SecretCodec for EpochSecretKey {
    fn encrypt(&self, secret: impl AsRef<[u8]>) -> SecretResult<EncryptedSecret> {
        let mut encrypted = self.key.encrypt(secret)?;
        encrypted.key_epoch = self.epoch;
        Ok(encrypted)
    }

    fn decrypt(&self, encrypted: EncryptedSecret) -> SecretResult<Vec<u8>> {
        self.key.decrypt(encrypted)
    }
}

struct Inner {
    /// Master keys actuellement chargées en mémoire, indexées par epoch —
    /// voir [`KeyEpoch`] pour la sémantique. Chaque valeur est zéroïsée
    /// individuellement à son propre `Drop` (retrait via
    /// [`SecretManager::retire_epoch`], ou destruction de `Inner` quand la
    /// dernière référence `Arc` disparaît) : pas besoin de logique de
    /// nettoyage explicite supplémentaire.
    keys: BTreeMap<KeyEpoch, Zeroizing<[u8; 32]>>,
    /// Epoch utilisée pour tout nouveau chiffrement/toute nouvelle preuve
    /// d'appartenance émise par ce nœud — toujours une clé de `keys` (voir
    /// invariant maintenu par [`SecretManager::with_epochs`]/
    /// [`SecretManager::set_current_epoch`]).
    current_epoch: KeyEpoch,
}

/// Détient la ou les master keys du cluster et dérive tout le reste par
/// HKDF (voir [`Self::derive_key`]) — jamais la master key elle-même sur le
/// réseau (voir [`Self::prove_membership`]).
///
/// # Rotation en cas de fuite
///
/// Une master key divulguée permet de dériver exactement les mêmes clés que
/// n'importe quel nœud légitime (chiffrement au repos, chiffrement en
/// transit, preuves d'appartenance) : c'est une compromission totale, pas
/// partielle. La rotation se fait sans coupure de service en laissant
/// coexister temporairement l'ancienne et la nouvelle epoch :
///
/// 1. **Générer** une nouvelle clé et lui assigner l'epoch suivante (compteur
///    suivi côté opérateur).
/// 2. **Déploiement double-clé** : charger la nouvelle epoch sur chaque nœud
///    via [`Self::add_epoch`], sans la rendre courante — tant que tous les
///    nœuds n'ont pas les deux epochs, ne pas basculer (un nœud en retard ne
///    pourrait pas encore déchiffrer/vérifier ce qu'un nœud à jour émettrait
///    sous la nouvelle epoch).
/// 3. **Basculer** : une fois tous les nœuds à jour, [`Self::set_current_epoch`]
///    sur chacun — les nouvelles écritures utilisent désormais la nouvelle
///    epoch, les anciennes données restent lisibles via l'ancienne.
/// 4. **Re-chiffrer** : lancer la passe de migration (voir
///    `model::catalog::rotate::reencrypt_model_store`, généralisable à tout
///    autre store chiffré) pour faire migrer les données existantes vers la
///    nouvelle epoch.
/// 5. **Vérifier** qu'il ne reste plus aucune donnée sous l'ancienne epoch
///    (voir `model::catalog::rotate::assert_no_rows_at_epoch`) et laisser
///    passer une marge de sécurité (RPC en vol, sauvegardes).
/// 6. **Retirer** l'ancienne epoch de chaque nœud via [`Self::retire_epoch`] :
///    c'est cette étape qui neutralise réellement la clé divulguée — après
///    elle, plus aucun nœud n'accepte de déchiffrer ni de vérifier quoi que
///    ce soit sous cette epoch.
#[derive(Clone)]
pub struct SecretManager(Arc<Mutex<Inner>>);

/// N'affiche jamais de matériel de clé — seules les epochs chargées et
/// l'epoch courante sont pertinentes pour du diagnostic/logging (voir
/// [`Self::epochs`]/[`Self::current_epoch`]).
impl std::fmt::Debug for SecretManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let guard = self.0.lock().unwrap();
        f.debug_struct("SecretManager")
            .field("epochs", &guard.keys.keys().collect::<Vec<_>>())
            .field("current_epoch", &guard.current_epoch)
            .finish_non_exhaustive()
    }
}

impl SecretManager {
    /// Construit un [`SecretManager`] à partir d'une unique master key, à
    /// [`DEFAULT_EPOCH`] — cas d'un cluster qui n'est pas en cours de
    /// rotation. Voir [`Self::with_epochs`] pour un ensemble multi-epochs.
    pub fn new(master_key_bytes: &SecretKey) -> Self {
        Self::with_epochs([(DEFAULT_EPOCH, *master_key_bytes)], DEFAULT_EPOCH)
            .expect("DEFAULT_EPOCH est à la fois fournie et courante")
    }

    /// Construit un [`SecretManager`] portant plusieurs epochs simultanément
    /// (rotation en cours, voir le runbook ci-dessus) : `current` sélectionne
    /// celle utilisée pour tout nouveau chiffrement/toute nouvelle preuve,
    /// les autres restent disponibles en lecture seule (déchiffrement de
    /// données pas encore migrées, vérification de preuves émises par un
    /// pair pas encore basculé). Erreur si `epochs` est vide ou si `current`
    /// n'y figure pas.
    pub fn with_epochs(
        epochs: impl IntoIterator<Item = (KeyEpoch, SecretKey)>,
        current: KeyEpoch,
    ) -> SecretResult<Self> {
        let keys: BTreeMap<KeyEpoch, Zeroizing<[u8; 32]>> = epochs
            .into_iter()
            .map(|(epoch, key)| (epoch, Zeroizing::new(key)))
            .collect();

        if keys.is_empty() {
            return Err(SecretError::NoKeysProvided);
        }
        if !keys.contains_key(&current) {
            return Err(SecretError::CurrentEpochNotProvided(current));
        }

        Ok(Self(Arc::new(Mutex::new(Inner { keys, current_epoch: current }))))
    }

    /// Charge une nouvelle epoch en mémoire vive, sans redémarrage (`Inner`
    /// est déjà `Arc<Mutex<_>>`) — ne la rend pas courante, voir
    /// [`Self::set_current_epoch`] (étape 2 du runbook de rotation).
    pub fn add_epoch(&self, epoch: KeyEpoch, key: SecretKey) {
        self.0.lock().unwrap().keys.insert(epoch, Zeroizing::new(key));
    }

    /// Bascule l'epoch utilisée pour tout nouveau chiffrement/toute nouvelle
    /// preuve d'appartenance (étape 3 du runbook de rotation) — échoue si
    /// `epoch` n'a pas été chargée au préalable (voir [`Self::add_epoch`]).
    pub fn set_current_epoch(&self, epoch: KeyEpoch) -> SecretResult<()> {
        let mut guard = self.0.lock().unwrap();
        if !guard.keys.contains_key(&epoch) {
            return Err(SecretError::UnknownEpoch(epoch));
        }
        guard.current_epoch = epoch;
        Ok(())
    }

    /// Retire définitivement une epoch de la mémoire vive de ce nœud
    /// (zéroïsée immédiatement, voir doc de champ [`Inner::keys`]) — étape 6
    /// du runbook de rotation, celle qui neutralise réellement une master
    /// key divulguée une fois répétée sur tous les nœuds du cluster. Refuse
    /// de retirer l'epoch courante ([`Self::set_current_epoch`] d'abord) ou
    /// la dernière epoch restante (un nœud sans aucune clé ne pourrait plus
    /// rien chiffrer/déchiffrer). Ne vérifie PAS par elle-même qu'aucune
    /// donnée persistée ne dépend encore de cette epoch — c'est le rôle de
    /// `model::catalog::rotate::assert_no_rows_at_epoch`, à appeler avant.
    pub fn retire_epoch(&self, epoch: KeyEpoch) -> SecretResult<()> {
        let mut guard = self.0.lock().unwrap();
        if guard.current_epoch == epoch {
            return Err(SecretError::CannotRetireCurrentEpoch(epoch));
        }
        if guard.keys.len() <= 1 {
            return Err(SecretError::LastEpoch);
        }
        if let Some(mut key) = guard.keys.remove(&epoch) {
            key.zeroize();
        }
        Ok(())
    }

    /// Epoch actuellement utilisée pour tout nouveau chiffrement/toute
    /// nouvelle preuve d'appartenance.
    pub fn current_epoch(&self) -> KeyEpoch {
        self.0.lock().unwrap().current_epoch
    }

    /// `true` si `epoch` est actuellement chargée (pas nécessairement
    /// courante).
    pub fn has_epoch(&self, epoch: KeyEpoch) -> bool {
        self.0.lock().unwrap().keys.contains_key(&epoch)
    }

    /// Toutes les epochs actuellement chargées, triées — pour diagnostic
    /// (ex. confirmer qu'un nœud a bien reçu la nouvelle epoch avant de
    /// basculer, étape 2→3 du runbook de rotation).
    pub fn epochs(&self) -> Vec<KeyEpoch> {
        self.0.lock().unwrap().keys.keys().copied().collect()
    }

    /// Prépare un chiffrement/déchiffrement en transit à destination de
    /// `peer_id`, sous l'epoch courante (voir [`Self::for_peer_epoch`] pour
    /// une epoch spécifique, nécessaire côté déchiffrement d'un
    /// `EncryptedSecret` reçu pendant une rotation).
    pub fn for_peer(&self, peer_id: PeerId) -> SecretResult<PeerSecretManager> {
        self.for_peer_epoch(peer_id, self.current_epoch())
    }

    /// Comme [`Self::for_peer`], mais dérive la clé pour une epoch
    /// spécifique plutôt que l'epoch courante de ce nœud — nécessaire côté
    /// déchiffrement, dont l'epoch (portée par l'[`EncryptedSecret`] reçu)
    /// peut légitimement différer de la nôtre pendant une rotation (voir le
    /// runbook sur [`SecretManager`]).
    pub fn for_peer_epoch(&self, peer_id: PeerId, epoch: KeyEpoch) -> SecretResult<PeerSecretManager> {
        let peer_key = self.derive_node_key_for_epoch(epoch, &peer_id)?;
        Ok(PeerSecretManager { global: self.clone(), epoch, peer_key })
    }

    /// Primitive de dérivation : (clé de `epoch`, `namespace`) -> clé de 32
    /// octets par HKDF-SHA256. Deux namespaces différents sous la même
    /// epoch produisent des clés sans rapport — c'est ce hook que tout futur
    /// champ/type chiffré (y compris des données externes apportées par les
    /// utilisateurs de Marie, hors catalogues du cluster) doit appeler avec
    /// SON PROPRE namespace plutôt que d'en réutiliser un déjà pris (voir
    /// [`Self::derive_storage_key_for_epoch`]/[`Self::derive_node_key_for_epoch`]
    /// pour les deux namespaces existants), pour ne jamais partager de
    /// matériel de clé entre domaines de données non liés.
    pub fn derive_key(&self, epoch: KeyEpoch, namespace: &[u8]) -> SecretResult<SecretKey> {
        use SecretError::{HkdfExpandFailed, UnknownEpoch};

        let guard = self.0.lock().unwrap();
        let ikm = guard.keys.get(&epoch).ok_or(UnknownEpoch(epoch))?;

        let hkdf = Hkdf::<Sha256>::new(None, ikm.as_ref());
        let mut key = [0u8; 32];
        hkdf.expand(namespace, &mut key).map_err(HkdfExpandFailed)?;

        Ok(key)
    }

    /// Comme [`Self::derive_key`], sous l'epoch courante.
    pub fn derive_key_current(&self, namespace: &[u8]) -> SecretResult<SecretKey> {
        self.derive_key(self.current_epoch(), namespace)
    }

    /// Dérive une clé par nœud via HKDF, sous l'epoch courante.
    pub fn derive_node_key(&self, node_id: &PeerId) -> SecretResult<SecretKey> {
        self.derive_node_key_for_epoch(self.current_epoch(), node_id)
    }

    /// Comme [`Self::derive_node_key`], sous une epoch spécifique — voir
    /// [`Self::for_peer_epoch`].
    pub fn derive_node_key_for_epoch(&self, epoch: KeyEpoch, node_id: &PeerId) -> SecretResult<SecretKey> {
        self.derive_key(epoch, node_id.to_bytes().as_ref())
    }

    /// Dérive la clé utilisée pour chiffrer un secret *au repos*, sous
    /// l'epoch courante (voir `model::catalog::store`), par opposition à
    /// [`Self::derive_node_key`] (spécifique à un pair). Voir
    /// [`Self::derive_storage_key_for_epoch`] côté déchiffrement d'une
    /// donnée persistée sous une epoch antérieure.
    pub fn derive_storage_key(&self) -> SecretResult<EpochSecretKey> {
        self.derive_storage_key_for_epoch(self.current_epoch())
    }

    /// Comme [`Self::derive_storage_key`], sous une epoch spécifique —
    /// nécessaire pour déchiffrer une donnée persistée sous une epoch autre
    /// que la courante (voir `model::catalog::store::StoredModel::decrypt`
    /// et le runbook de rotation sur [`SecretManager`]).
    pub fn derive_storage_key_for_epoch(&self, epoch: KeyEpoch) -> SecretResult<EpochSecretKey> {
        let key = self.derive_key(epoch, b"marie/at-rest-storage/v1")?;
        Ok(EpochSecretKey { epoch, key })
    }

    /// Calcule la preuve d'appartenance au cluster pour `node_id` sur
    /// `nonce`, sous l'epoch courante : HMAC-SHA256(clé dérivée pour
    /// `node_id`, nonce). Toute instance de `SecretManager` ayant chargé
    /// cette epoch calcule exactement la même preuve pour un couple
    /// `(node_id, nonce)` donné — la master key elle-même ne transite jamais
    /// sur le réseau. L'epoch retournée doit accompagner la preuve sur le
    /// fil pour que [`Self::verify_membership`] sache quelle clé essayer.
    /// Utilisée pour authentifier automatiquement les nœuds `control plane`
    /// du cluster (voir `network::actor::NetworkActor`).
    pub fn prove_membership(&self, node_id: &PeerId, nonce: &[u8]) -> SecretResult<(KeyEpoch, [u8; 32])> {
        let epoch = self.current_epoch();
        let node_key = self.derive_node_key_for_epoch(epoch, node_id)?;
        let mut mac = <HmacSha256 as Mac>::new_from_slice(&node_key).map_err(SecretError::MacInitFailed)?;
        mac.update(nonce);
        Ok((epoch, mac.finalize().into_bytes().into()))
    }

    /// Vérifie une preuve produite par [`Self::prove_membership`] pour le
    /// couple `(node_id, nonce)`, sous l'`epoch` qu'elle indique avoir
    /// utilisée. La comparaison est en temps constant (déléguée à
    /// `hmac::Mac::verify_slice`). Une epoch inconnue ou déjà retirée (voir
    /// [`Self::retire_epoch`]) fait échouer la vérification (`Ok(false)`),
    /// pas une erreur — une preuve faite sous une epoch retirée doit
    /// simplement cesser d'être acceptée, comme n'importe quelle preuve
    /// invalide.
    pub fn verify_membership(&self, node_id: &PeerId, nonce: &[u8], epoch: KeyEpoch, proof: &[u8]) -> SecretResult<bool> {
        let node_key = match self.derive_node_key_for_epoch(epoch, node_id) {
            Ok(key) => key,
            Err(SecretError::UnknownEpoch(_)) => return Ok(false),
            Err(other) => return Err(other),
        };
        let mut mac = <HmacSha256 as Mac>::new_from_slice(&node_key).map_err(SecretError::MacInitFailed)?;
        mac.update(nonce);
        Ok(mac.verify_slice(proof).is_ok())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EncryptedSecret {
    /// Epoch de la master key qui a produit ce chiffré (voir [`KeyEpoch`]) —
    /// permet de sélectionner la bonne clé au déchiffrement même après un
    /// changement d'epoch courante (voir
    /// `SecretManager::derive_storage_key_for_epoch`/`for_peer_epoch`).
    pub key_epoch: KeyEpoch,
    pub ciphertext: Vec<u8>,
    pub nonce: Vec<u8>,
    pub algorithm: String,
}

#[cfg(test)]
mod tests {
    use libp2p::PeerId;

    use super::*;

    fn key(byte: u8) -> SecretKey {
        [byte; 32]
    }

    #[test]
    fn storage_key_round_trip_under_one_epoch() {
        let secret = SecretManager::new(&key(1));
        let storage_key = secret.derive_storage_key().unwrap();

        let encrypted = storage_key.encrypt_str("hunter2").unwrap();
        assert_eq!(encrypted.key_epoch, DEFAULT_EPOCH);

        let decrypted = storage_key.decrypt_str(encrypted).unwrap();
        assert_eq!(decrypted, "hunter2");
    }

    #[test]
    fn decrypt_with_wrong_epoch_key_fails_cleanly() {
        let secret = SecretManager::new(&key(1));
        secret.add_epoch(1, key(2));

        let encrypted = secret.derive_storage_key().unwrap().encrypt_str("hunter2").unwrap();
        assert_eq!(encrypted.key_epoch, DEFAULT_EPOCH);

        let wrong_key = secret.derive_storage_key_for_epoch(1).unwrap();
        let err = wrong_key.decrypt_str(encrypted).unwrap_err();
        assert!(matches!(err, SecretError::DecryptionFailed(_)));
    }

    #[test]
    fn added_epoch_not_current_still_decrypts_old_data() {
        let secret = SecretManager::new(&key(1));
        let encrypted = secret.derive_storage_key().unwrap().encrypt_str("hunter2").unwrap();

        secret.add_epoch(1, key(2));
        assert_eq!(secret.current_epoch(), DEFAULT_EPOCH); // pas encore basculée

        let decrypted = secret
            .derive_storage_key_for_epoch(encrypted.key_epoch)
            .unwrap()
            .decrypt_str(encrypted)
            .unwrap();
        assert_eq!(decrypted, "hunter2");
    }

    #[test]
    fn set_current_epoch_changes_new_encryptions_tag() {
        let secret = SecretManager::new(&key(1));
        secret.add_epoch(1, key(2));
        secret.set_current_epoch(1).unwrap();

        let encrypted = secret.derive_storage_key().unwrap().encrypt_str("hunter2").unwrap();
        assert_eq!(encrypted.key_epoch, 1);
    }

    #[test]
    fn retire_epoch_makes_it_unreadable() {
        let secret = SecretManager::new(&key(1));
        let encrypted = secret.derive_storage_key().unwrap().encrypt_str("hunter2").unwrap();

        secret.add_epoch(1, key(2));
        secret.set_current_epoch(1).unwrap();
        secret.retire_epoch(DEFAULT_EPOCH).unwrap();

        let err = secret.derive_storage_key_for_epoch(DEFAULT_EPOCH).unwrap_err();
        assert!(matches!(err, SecretError::UnknownEpoch(e) if e == DEFAULT_EPOCH));

        // Le chiffré epoch 0 n'est donc plus déchiffrable via ce SecretManager.
        let _ = encrypted;
    }

    #[test]
    fn cannot_retire_current_epoch() {
        let secret = SecretManager::new(&key(1));
        let err = secret.retire_epoch(DEFAULT_EPOCH).unwrap_err();
        assert!(matches!(err, SecretError::CannotRetireCurrentEpoch(e) if e == DEFAULT_EPOCH));
    }

    #[test]
    fn cannot_retire_last_remaining_epoch() {
        let secret = SecretManager::new(&key(1));
        secret.add_epoch(1, key(2));
        secret.set_current_epoch(1).unwrap();

        // Retire l'epoch 0, il n'en reste qu'une (1, courante) : ne peut pas
        // non plus être retirée (garde "epoch courante", qui recouvre ici
        // aussi le cas "dernière epoch restante").
        secret.retire_epoch(DEFAULT_EPOCH).unwrap();
        let err = secret.retire_epoch(1).unwrap_err();
        assert!(matches!(err, SecretError::CannotRetireCurrentEpoch(_) | SecretError::LastEpoch));
    }

    #[test]
    fn with_epochs_rejects_current_not_provided() {
        let err = SecretManager::with_epochs([(0, key(1))], 1).unwrap_err();
        assert!(matches!(err, SecretError::CurrentEpochNotProvided(1)));
    }

    #[test]
    fn with_epochs_rejects_empty_set() {
        let err = SecretManager::with_epochs(std::iter::empty(), 0).unwrap_err();
        assert!(matches!(err, SecretError::NoKeysProvided));
    }

    #[test]
    fn derive_key_namespaces_are_independent() {
        let secret = SecretManager::new(&key(1));
        let a = secret.derive_key(DEFAULT_EPOCH, b"namespace-a").unwrap();
        let b = secret.derive_key(DEFAULT_EPOCH, b"namespace-b").unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn for_peer_round_trips_across_epoch_change() {
        let secret = SecretManager::new(&key(1));
        let peer = PeerId::random();

        let sender = secret.for_peer(peer).unwrap();
        let encrypted = sender.encrypt("hunter2").unwrap();
        assert_eq!(encrypted.key_epoch, DEFAULT_EPOCH);

        secret.add_epoch(1, key(2));
        secret.set_current_epoch(1).unwrap();

        // Le destinataire doit dériver sous l'epoch portée par le chiffré
        // reçu, pas sous sa propre epoch courante (désormais 1).
        let receiver = secret.for_peer_epoch(peer, encrypted.key_epoch).unwrap();
        let decrypted = receiver.decrypt(&encrypted).unwrap();
        assert_eq!(decrypted, "hunter2");
    }

    #[test]
    fn for_peer_mismatched_epoch_fails() {
        let secret = SecretManager::new(&key(1));
        secret.add_epoch(1, key(2));
        let peer = PeerId::random();

        let sender = secret.for_peer_epoch(peer, DEFAULT_EPOCH).unwrap();
        let encrypted = sender.encrypt("hunter2").unwrap();

        let wrong_receiver = secret.for_peer_epoch(peer, 1).unwrap();
        let err = wrong_receiver.decrypt(&encrypted).unwrap_err();
        assert!(matches!(err, SecretError::DecryptionFailed(_)));
    }

    #[test]
    fn membership_proof_round_trips_and_is_epoch_aware() {
        let secret = SecretManager::new(&key(1));
        let node = PeerId::random();
        let nonce = b"nonce";

        let (epoch, proof) = secret.prove_membership(&node, nonce).unwrap();
        assert!(secret.verify_membership(&node, nonce, epoch, &proof).unwrap());
    }

    #[test]
    fn membership_proof_fails_once_epoch_retired() {
        let secret = SecretManager::new(&key(1));
        let node = PeerId::random();
        let nonce = b"nonce";

        let (epoch, proof) = secret.prove_membership(&node, nonce).unwrap();

        secret.add_epoch(1, key(2));
        secret.set_current_epoch(1).unwrap();
        secret.retire_epoch(epoch).unwrap();

        assert!(!secret.verify_membership(&node, nonce, epoch, &proof).unwrap());
    }
}
