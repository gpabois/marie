use std::{borrow::Borrow, ops::Deref};
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::secret::store::SecretRef;

/// Identifiant unique d'un modèle dans le [`ModelCatalog`](crate::model::catalog::ModelCatalog).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ModelId(String);

impl AsRef<[u8]> for ModelId {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl Deref for ModelId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0.as_str()
    }
}

impl ModelId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl fmt::Display for ModelId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for ModelId {
    fn from(id: String) -> Self {
        Self(id)
    }
}

impl From<&str> for ModelId {
    fn from(id: &str) -> Self {
        Self(id.to_owned())
    }
}

impl Borrow<str> for ModelId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

/// Déclaration d'un modèle dans le [`ModelCatalog`](crate::model::catalog::ModelCatalog).
/// Un enum plutôt qu'une struct : chaque variante porte le jeu d'attributs
/// propre à son protocole d'accès (aujourd'hui uniquement une API compatible
/// OpenAI, voir [`Self::OpenAICompatible`]) — de futures variantes (par
/// exemple un provider avec une authentification différente) pourront
/// coexister sans forcer des champs `Option` non pertinents sur les autres.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum Model {
    #[serde(rename = "open-ai-compat")]
    OpenAICompatible {
        id: String,
        base_url: String,
        client_id: String,
        api_key: SecretRef,
        model: String,
        /// Prompt système appliqué par défaut à tout agent utilisant ce modèle.
        /// `None` si le modèle n'en définit pas (l'appelant fournit alors son
        /// propre contexte système, voir [`crate::agent::context::Context`]).
        system_prompt: Option<String>,
    },
}
