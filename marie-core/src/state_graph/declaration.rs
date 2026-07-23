use std::borrow::Borrow;
use std::fmt;

use serde::{Deserialize, Serialize};

use super::{Edge, Node};

/// Identifiant unique d'un graphe dans le
/// [`StateGraphCatalog`](crate::state_graph::catalog::StateGraphCatalog).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct StateGraphId(String);

impl StateGraphId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl fmt::Display for StateGraphId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for StateGraphId {
    fn from(id: String) -> Self {
        Self(id)
    }
}

impl From<&str> for StateGraphId {
    fn from(id: &str) -> Self {
        Self(id.to_owned())
    }
}

impl Borrow<str> for StateGraphId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl AsRef<[u8]> for StateGraphId {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

/// Déclaration réutilisable d'un graphe d'états, hébergée dans le
/// [`StateGraphCatalog`](crate::state_graph::catalog::StateGraphCatalog)
/// sur le même modèle décentralisé que
/// [`ExpertCatalog`](crate::expert::catalog::ExpertCatalog)/
/// [`ModelCatalog`](crate::model::catalog::ModelCatalog) (catalogue `LoroDoc`
/// hébergé sur un pair choisi par hash-ring, pas de réplication Raft) : les
/// mêmes `nodes`/`edges`/`entry` qu'un [`super::StateGraph`] déjà construit,
/// mais conservés sous forme de template nommé plutôt que d'état en cours
/// d'exécution — contrairement à [`super::StateGraph`], ne porte pas de
/// curseurs : chaque instanciation (voir
/// [`crate::state_graph::client::StateGraphClient::instantiate`])
/// reconstruit un graphe frais, positionné sur `entry`, plutôt que de
/// partager un état mutable entre plusieurs usages du même template. Ne porte
/// aucun secret (les seuls champs sensibles possibles, un
/// [`ExpertId`](crate::expert::ExpertId) référencé par un
/// [`Executable::Agent`](crate::state_graph::executable::Executable::Agent),
/// ne sont que des identifiants) — rien à chiffrer pour le stockage au repos,
/// sur le même modèle que [`crate::expert::Expert`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StateGraphDeclaration {
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
    pub entry: String,
}
