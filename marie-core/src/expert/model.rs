use std::borrow::Borrow;
use std::fmt;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::model::ModelId;
use crate::tools::ToolId;

/// Identifiant unique d'un expert dans l'[`ExpertCatalog`](crate::expert::catalog::ExpertCatalog).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, JsonSchema)]
pub struct ExpertId(String);

impl ExpertId {
    pub fn new(id: impl ToString) -> Self {
        Self(id.to_string())
    }
}

impl fmt::Display for ExpertId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for ExpertId {
    fn from(id: String) -> Self {
        Self(id)
    }
}

impl From<&str> for ExpertId {
    fn from(id: &str) -> Self {
        Self(id.to_owned())
    }
}

impl Borrow<str> for ExpertId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl AsRef<[u8]> for ExpertId {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

/// Déclaration d'un expert, répliquée via Raft (voir
/// `network::cp::state::ControlPlaneState::experts`) : un agent préconfiguré
/// avec son propre prompt, un modèle dédié (voir
/// [`ModelId`](crate::model::catalog::ModelId)) et la liste des tools qu'il a
/// le droit d'appeler (voir
/// [`ToolId`](crate::tools::catalog::ToolId)). Contrairement à
/// [`crate::model::declaration::Model`], ne porte aucun secret —
/// rien à chiffrer pour le stockage au repos (voir `expert::catalog::store`)
/// ni pour le transit réseau, sur le même modèle que
/// [`crate::tools::declaration::ToolDeclaration`]. Ne référence les modèles
/// et tools que par identifiant : leur résolution (existence, contenu actuel)
/// se fait au moment de l'utilisation, pas à la déclaration de l'expert.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Expert {
    pub id: ExpertId,
    pub prompt: String,
    pub model_id: ModelId,
    pub allowed_tools: Vec<ToolId>,
}
