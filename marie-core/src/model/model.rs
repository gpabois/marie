use std::{borrow::Borrow, ops::Deref};
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::secret::{Encryptable, EncryptedSecret, SecretCodec, SecretResult};

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
        api_key: String,
        model: String,
        /// Prompt système appliqué par défaut à tout agent utilisant ce modèle.
        /// `None` si le modèle n'en définit pas (l'appelant fournit alors son
        /// propre contexte système, voir [`crate::agent::context::Context`]).
        system_prompt: Option<String>,
    },
}

impl Encryptable for Model {
    type Encrypted = EncryptedModel;

    fn encrypt<C>(self, codec: &C) -> SecretResult<Self::Encrypted> where C: SecretCodec {
        Ok(match self {
            Self::OpenAICompatible { id, base_url, client_id, model, system_prompt, api_key } => {
                EncryptedModel::OpenAICompatible { id, base_url, client_id, api_key: codec.encrypt_str(api_key)?, model, system_prompt }
            }
        })
    }

    fn decrypt<C>(encrypted: Self::Encrypted, codec: &C) -> crate::secret::SecretResult<Self> where C: crate::secret::SecretCodec {
        Ok(match encrypted {
            EncryptedModel::OpenAICompatible { id, base_url, client_id, api_key, model, system_prompt } => {
                Model::OpenAICompatible { id, base_url, client_id, api_key: codec.decrypt_str(api_key)?, model, system_prompt }
            },
        })
    }
}

impl Model {
    pub fn id(&self) -> &str {
        match self {
            Model::OpenAICompatible { id, .. } => id.as_str(),
        }
    }
}

/// Représentation d'un [`Model`] telle qu'elle transite entre le control
/// plane et un nœud consommateur (voir `RpcCall::GET_MODEL`) : la clé API n'y
/// est jamais en clair, seulement chiffrée pour le nœud destinataire (voir
/// `SecretManager::derive_node_key` côté control plane et
/// `NetworkClient::decrypt_secret` côté consommateur).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EncryptedModel {
    OpenAICompatible {
        id: String,
        base_url: String,
        client_id: String,
        api_key: EncryptedSecret,
        model: String,
        system_prompt: Option<String>,
    },
}

impl EncryptedModel {
    /// Clé API chiffrée de ce modèle, voir [`Self::into_model`].
    #[must_use]
    pub fn api_key(&self) -> &EncryptedSecret {
        match self {
            Self::OpenAICompatible { api_key, .. } => api_key,
        }
    }

    /// Reconstitue la déclaration en clair une fois `api_key` déchiffrée
    /// localement (voir `NetworkClient::decrypt_secret` ou
    /// `model::catalog::store::decrypt_from_storage`).
    #[must_use]
    pub fn decrypt(self, api_key: String) -> Model {
        match self {
            Self::OpenAICompatible { id, base_url, client_id, model, system_prompt, .. } => {
                Model::OpenAICompatible { id, base_url, client_id, api_key, model, system_prompt }
            }
        }
    }
}
