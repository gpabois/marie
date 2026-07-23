use libp2p::PeerId;
use thiserror::Error;

use crate::{
    network::bootstrap::BootstrapClient,
    rpc::{RpcClient, RpcError, Void},
    state_graph::{
        NS_STATE_GRAPH, StateGraph, StateGraphError,
        declaration::{StateGraphDeclaration, StateGraphId},
        rpc::{GetStateGraph, InsertStateGraph, ListStateGraph, RemoveStateGraph, SetStateGraphRequest, UpdateStateGraph},
    },
};

#[derive(Debug, Error)]
pub enum StateGraphClientError {
    #[error("aucun catalogue de graphes d'états n'est disponible")]
    NoCatalogAvailable,
    #[error("graphe d'états inconnu : {0}")]
    UnknownStateGraph(StateGraphId),
    #[error("[StateGraph] échec de l'appel distant : {0}")]
    RpcError(#[from] RpcError),
    /// La déclaration récupérée (ou sur le point d'être enregistrée) ne
    /// forme pas un graphe cohérent — voir [`StateGraphError`]. Pour
    /// [`StateGraphClient::set`], ce rejet est local (avant tout appel
    /// réseau) : mieux vaut refuser une déclaration invalide à
    /// l'enregistrement que de laisser [`StateGraphClient::instantiate`]
    /// échouer plus tard pour quiconque la référence.
    #[error("déclaration de graphe invalide : {0}")]
    InvalidDeclaration(#[from] StateGraphError),
}

/// Point d'entrée pour le CRUD du catalogue de graphes d'états, sur le même
/// modèle que [`crate::expert::client::ExpertClient`]/[`crate::model::client::ModelClient`] :
/// chaque opération sélectionne de manière déterministe le pair qui héberge
/// le catalogue (voir [`Self::select_catalog`]) plutôt que de s'appuyer sur
/// une réplication Raft. Sert aussi de point d'entrée pour instancier un
/// [`StateGraph`] exécutable à partir d'une déclaration nommée du catalogue
/// (voir [`Self::instantiate`]).
#[derive(Clone)]
pub struct StateGraphClient {
    local_peer_id: PeerId,
    rpc: RpcClient,
    bootstrap: BootstrapClient,
}

impl StateGraphClient {
    #[must_use]
    pub fn new(local_peer_id: PeerId, rpc: RpcClient, bootstrap: BootstrapClient) -> Self {
        Self { rpc, bootstrap, local_peer_id }
    }

    /// Récupère la déclaration d'un graphe d'états auprès du catalogue.
    pub async fn get(&self, id: impl Into<StateGraphId>) -> Result<StateGraphDeclaration, StateGraphClientError> {
        let id = id.into();
        let catalog = self.select_catalog(&id)?;

        self.rpc.invoke::<GetStateGraph>(id.clone(), [catalog]).await?.ok_or(StateGraphClientError::UnknownStateGraph(id))
    }

    /// Liste tout le catalogue de graphes d'états connu du pair sélectionné.
    pub async fn list(&self) -> Result<Vec<StateGraphDeclaration>, StateGraphClientError> {
        let catalog = self.select_catalog(self.local_peer_id.to_bytes())?;

        self.rpc.invoke::<ListStateGraph>(Void, [catalog]).await.map_err(StateGraphClientError::from)
    }

    /// Crée ou remplace la déclaration d'un graphe d'états dans le
    /// catalogue. Rejette localement toute déclaration incohérente (voir
    /// [`StateGraph::new`]) avant même de la proposer au réseau.
    pub async fn set(&self, id: impl Into<StateGraphId>, declaration: StateGraphDeclaration) -> Result<(), StateGraphClientError> {
        let id = id.into();
        StateGraph::new(declaration.nodes.clone(), declaration.edges.clone(), declaration.entry.clone())?;
        let catalog = self.select_catalog(&id)?;

        self.rpc.invoke::<InsertStateGraph>(SetStateGraphRequest { id, declaration }, [catalog]).await?;

        Ok(())
    }

    /// Met à jour la déclaration d'un graphe d'états existant — même
    /// validation locale que [`Self::set`].
    pub async fn update(&self, id: impl Into<StateGraphId>, declaration: StateGraphDeclaration) -> Result<(), StateGraphClientError> {
        let id = id.into();
        StateGraph::new(declaration.nodes.clone(), declaration.edges.clone(), declaration.entry.clone())?;
        let catalog = self.select_catalog(&id)?;

        self.rpc.invoke::<UpdateStateGraph>(SetStateGraphRequest { id, declaration }, [catalog]).await?;

        Ok(())
    }

    /// Retire un graphe d'états du catalogue.
    pub async fn remove(&self, id: impl Into<StateGraphId>) -> Result<(), StateGraphClientError> {
        let id = id.into();
        let catalog = self.select_catalog(&id)?;

        self.rpc.invoke::<RemoveStateGraph>(id, [catalog]).await?;

        Ok(())
    }

    /// Instancie un [`StateGraph`] exécutable à partir d'une déclaration
    /// nommée du catalogue — un nouvel exemplaire à chaque appel, positionné
    /// sur `entry` (voir [`StateGraphDeclaration`]) : plusieurs sessions
    /// peuvent instancier le même graphe du catalogue en parallèle sans
    /// partager leur progression respective.
    pub async fn instantiate(&self, id: impl Into<StateGraphId>) -> Result<StateGraph, StateGraphClientError> {
        let declaration = self.get(id).await?;
        Ok(StateGraph::new(declaration.nodes, declaration.edges, declaration.entry)?)
    }

    /// Sélection déterministe d'un catalogue.
    fn select_catalog(&self, id: impl AsRef<[u8]>) -> Result<PeerId, StateGraphClientError> {
        use StateGraphClientError::NoCatalogAvailable;
        self.bootstrap.select_peer(NS_STATE_GRAPH, &id).ok_or(NoCatalogAvailable)
    }
}
