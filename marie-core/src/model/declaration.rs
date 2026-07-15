use std::borrow::Borrow;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::secret::EncryptedSecret;

/// Identifiant unique d'un modèle dans le [`ModelCatalog`](crate::model::catalog::ModelCatalog).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ModelId(String);

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
pub enum Model {
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

impl Model {
    /// Clé API en clair de ce modèle — jamais transmise ni persistée telle
    /// quelle, voir [`Self::encrypt`].
    #[must_use]
    pub fn api_key(&self) -> &str {
        match self {
            Self::OpenAICompatible { api_key, .. } => api_key,
        }
    }

    /// Produit la représentation chiffrée de cette déclaration, destinée à
    /// transiter sur le réseau (voir [`RpcCall::GET_MODEL`](crate::network::cp::rpc::RpcCall::GET_MODEL))
    /// ou à être persistée au repos (voir `model::catalog::store`) :
    /// `api_key` doit déjà avoir été chiffrée pour le destinataire (voir
    /// `SecretManager::encrypt_api_key`), jamais en clair.
    #[must_use]
    pub fn encrypt(&self, api_key: EncryptedSecret) -> EncryptedModel {
        match self {
            Self::OpenAICompatible { id, base_url, client_id, model, system_prompt, .. } => {
                EncryptedModel::OpenAICompatible {
                    id: id.clone(),
                    base_url: base_url.clone(),
                    client_id: client_id.clone(),
                    api_key,
                    model: model.clone(),
                    system_prompt: system_prompt.clone(),
                }
            }
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
