use std::{borrow::Borrow, ops::Deref};
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::catalog::Catalogable;
use crate::secret::vault::{SecretRef, Vault, VaultError};

/// Identifiant unique d'un modèle dans le [`ModelCatalog`](crate::model::catalog::ModelCatalog).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ModelId(String);

impl AsRef<[u8]> for ModelId {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl AsRef<str> for ModelId {
    fn as_ref(&self) -> &str {
        self.0.as_str()
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
pub enum EncryptedModel {
    #[serde(rename = "open-ai-compat")]
    OpenAICompatible {
        id: ModelId,
        base_url: String,
        client_id: String,
        api_key: SecretRef,
        model: String,
        system_prompt: Option<String>,
    },
}

impl Catalogable for EncryptedModel {
    const KIND: &str = "/marie/catalog/models";

    fn id(&self) -> &str {
        match self {
            EncryptedModel::OpenAICompatible { id, .. } => id.as_ref(),
        }
    }
}

pub enum Model {
    OpenAICompatible {
        id: ModelId,
        base_url: String,
        client_id: String,
        api_key: String,
        model: String,
        system_prompt: Option<String>,
    },
}

impl EncryptedModel {
    pub async fn decrypt(self, vault: &Vault) -> Result<Model, VaultError> {
        match self {
            EncryptedModel::OpenAICompatible { id, base_url, client_id, api_key, model, system_prompt } => {
                let api_key = vault.get_decrypted_str(api_key, "/marie/secrets/models").await?;
                Ok(Model::OpenAICompatible { id, base_url, client_id, api_key, model, system_prompt })
            },
        }
        
    }
}