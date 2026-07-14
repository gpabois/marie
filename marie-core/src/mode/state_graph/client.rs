use std::collections::HashMap;

use thiserror::Error;

use crate::{
    mode::state_graph::{StateGraph, StateGraphError, catalog::StateGraphId, declaration::StateGraphDeclaration},
    network::actor::NetworkClient,
};

#[derive(Debug, Error)]
pub enum StateGraphClientError {
    #[error("graphe d'états inconnu : {0}")]
    UnknownStateGraph(StateGraphId),
    #[error("échec réseau : {0}")]
    Network(String),
    /// La déclaration récupérée (ou sur le point d'être enregistrée) ne
    /// forme pas un graphe cohérent — voir [`StateGraphError`]. Pour
    /// [`StateGraphClient::set`], ce rejet est local (avant toute
    /// proposition Raft) : mieux vaut refuser une déclaration invalide à
    /// l'enregistrement que de laisser
    /// [`StateGraphClient::instantiate`] échouer plus tard pour quiconque
    /// la référence.
    #[error("déclaration de graphe invalide : {0}")]
    InvalidDeclaration(#[from] StateGraphError),
}

/// Point d'entrée pour le CRUD du catalogue de graphes d'états (répliqué via
/// Raft, sur le même modèle que [`crate::expert::client::ExpertClient`]), et
/// pour instancier un [`StateGraph`] exécutable à partir d'une déclaration
/// nommée du catalogue (voir [`Self::instantiate`]).
#[derive(Clone)]
pub struct StateGraphClient(NetworkClient);

impl StateGraphClient {
    #[must_use]
    pub fn new(client: NetworkClient) -> Self {
        Self(client)
    }

    /// Récupère la déclaration d'un graphe d'états auprès du control plane.
    pub async fn get(&self, id: impl Into<StateGraphId>) -> Result<StateGraphDeclaration, StateGraphClientError> {
        let id = id.into();

        self.0
            .get_state_graph(id.clone())
            .await
            .map_err(|error| StateGraphClientError::Network(error.to_string()))?
            .ok_or(StateGraphClientError::UnknownStateGraph(id))
    }

    /// Liste tout le catalogue de graphes d'états connu du control plane.
    pub async fn list(&self) -> Result<HashMap<StateGraphId, StateGraphDeclaration>, StateGraphClientError> {
        self.0.list_state_graphs().await.map_err(|error| StateGraphClientError::Network(error.to_string()))
    }

    /// Crée ou remplace la déclaration d'un graphe d'états dans le catalogue
    /// (répliqué via Raft, voir `ControlPlaneRequest::SetStateGraph`).
    /// Rejette localement toute déclaration incohérente (voir
    /// [`StateGraph::new`]) avant même de la proposer au cluster.
    pub async fn set(&self, id: impl Into<StateGraphId>, declaration: StateGraphDeclaration) -> Result<(), StateGraphClientError> {
        StateGraph::new(declaration.nodes.clone(), declaration.edges.clone(), declaration.entry.clone())?;

        self.0.set_state_graph(id, declaration).await.map_err(|error| StateGraphClientError::Network(error.to_string()))
    }

    /// Retire un graphe d'états du catalogue (répliqué via Raft, voir
    /// `ControlPlaneRequest::RemoveStateGraph`).
    pub async fn remove(&self, id: impl Into<StateGraphId>) -> Result<(), StateGraphClientError> {
        self.0.remove_state_graph(id).await.map_err(|error| StateGraphClientError::Network(error.to_string()))
    }

    /// Instancie un [`StateGraph`] exécutable à partir d'une déclaration
    /// nommée du catalogue — un nouvel exemplaire à chaque appel, positionné
    /// sur `entry` (voir [`StateGraphDeclaration`]) : plusieurs sessions
    /// peuvent instancier le même graphe du catalogue en parallèle sans
    /// partager leur progression respective (voir
    /// [`crate::session::crdt::YrsSession::push_mode`], qui reçoit le
    /// résultat par valeur).
    pub async fn instantiate(&self, id: impl Into<StateGraphId>) -> Result<StateGraph, StateGraphClientError> {
        let declaration = self.get(id).await?;
        Ok(StateGraph::new(declaration.nodes, declaration.edges, declaration.entry)?)
    }
}
