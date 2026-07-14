use std::borrow::Borrow;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::tools::ToolSignature;

/// Identifiant unique d'un tool dans le [`ToolCatalog`](crate::tools::catalog::ToolCatalog).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ToolId(String);

impl ToolId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl fmt::Display for ToolId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for ToolId {
    fn from(id: String) -> Self {
        Self(id)
    }
}

impl From<&str> for ToolId {
    fn from(id: &str) -> Self {
        Self(id.to_owned())
    }
}

impl Borrow<str> for ToolId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

/// Portée d'un tool déclaré dans le catalogue : `Global` est appelable depuis
/// n'importe quel agent, `Session` uniquement depuis un agent dont la frame
/// le liste explicitement (voir `agent::frame::AgentFrame::allowed_tools`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ToolScope {
    Global,
    Session,
}

/// Déclaration d'un tool, répliquée via Raft (voir
/// `network::cp::state::ControlPlaneState::tools`) : la signature exposée au
/// modèle (voir [`crate::model::execute`]) et sa portée. Contrairement à
/// [`crate::model::declaration::Model`], ne porte aucun secret —
/// rien à chiffrer pour le stockage au repos (voir `tools::catalog::store`)
/// ni pour le transit réseau. Ne référence pas non plus le nœud qui
/// l'exécute : un exécuteur peut apparaître, disparaître ou changer de nœud
/// sans que cette déclaration change (voir
/// `tools::client::ToolClient::register_executor`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolDeclaration {
    pub signature: ToolSignature,
    pub scope: ToolScope,
}
