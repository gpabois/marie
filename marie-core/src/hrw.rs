use std::cmp::Ordering;

use libp2p::PeerId;

pub trait Pickable {
    /// Identifiant stable (PeerId encodé, adresse, nom de shard...).
    fn id_bytes(&self) -> Vec<u8>;
 
    /// Poids relatif. 1.0 par défaut = tous les nœuds sont équivalents.
    fn weight(&self) -> f64 {
        1.0
    }
}

impl Pickable for PeerId {
    fn id_bytes(&self) -> Vec<u8> {
        self.to_bytes()
    }
}

const FNV_OFFSET: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;
 
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}
 
// fmix64 (MurmurHash3) : ré-étale les bits pour une meilleure avalanche
// que FNV-1a seul, notamment sur les bits hauts.
fn fmix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51afd7ed558ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ceb9fe1a85ec53);
    k ^= k >> 33;
    k
}
 
/// Combine `key` et l'id du nœud en un u64 bien distribué.
///
/// Pas cryptographique — suffisant pour de l'équilibrage de charge. Si `key`
/// peut être choisi par un tiers non fiable (risque de concentration
/// délibérée), remplacer par un hash gardé par clé secrète (SipHash keyé,
/// blake3 en mode keyed) partagée par tous les nœuds.
fn combined_hash(key: &[u8], node_id: &[u8]) -> u64 {
    let mut h = fnv1a(key);
    h = h.wrapping_mul(FNV_PRIME) ^ fnv1a(node_id);
    fmix64(h)
}
 
/// Score non pondéré : le hash brut. Le plus haut gagne.
/// Correct uniquement si tous les nœuds ont une capacité équivalente.
pub fn score(key: &[u8], node_id: &[u8]) -> u64 {
    combined_hash(key, node_id)
}
 
/// Score pondéré (méthode logarithmique). Le plus haut gagne.
/// Donne une probabilité de sélection proportionnelle à `weight`.
pub fn weighted_score(key: &[u8], node_id: &[u8], weight: f64) -> f64 {
    debug_assert!(weight > 0.0, "le poids doit être strictement positif");
    let h = combined_hash(key, node_id);
    // normalise en (0, 1], jamais exactement 0 (ln(0) = -inf)
    let u = (h as f64 + 1.0) / (u64::MAX as f64 + 2.0);
    -weight / u.ln()
}
 
/// Choisit le nœud gagnant pour `key` parmi `nodes` (pondéré).
pub fn pick<'a, N: Pickable>(key: &[u8], nodes: &'a [N]) -> Option<&'a N> {
    nodes.iter().max_by(|a, b| {
        weighted_score(key, &a.id_bytes(), a.weight())
            .partial_cmp(&weighted_score(key, &b.id_bytes(), b.weight()))
            .unwrap_or(Ordering::Equal)
    })
}
 
/// Classe les `n` meilleurs candidats pour `key`, du meilleur au moins bon.
/// Utile pour une boucle de dispatch avec retry déterministe : si le premier
/// choix échoue (Busy/timeout), on retente le suivant dans cet ordre plutôt
/// que de re-choisir au hasard.
pub fn pick_top_n<'a, N: Pickable>(key: &[u8], nodes: &'a [N], n: usize) -> Vec<&'a N> {
    let mut scored: Vec<(&N, f64)> = nodes
        .iter()
        .map(|node| (node, weighted_score(key, &node.id_bytes(), node.weight())))
        .collect();
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
    scored.into_iter().take(n).map(|(node, _)| node).collect()
}