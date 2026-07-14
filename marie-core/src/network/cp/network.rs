use libp2p::PeerId;
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use crate::network::actor::NetworkService;
use crate::network::cp::rpc::RpcCall;
use crate::network::cp::types::{RaftNode, RaftNodeId, TypeConfig};

/// Erreur de transport survenue lors d'un appel RPC Raft via libp2p
/// (échec du canal interne, timeout, ou erreur applicative distante).
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct RpcTransportError(String);

fn to_rpc_error<E>(error: anyhow::Error) -> RPCError<RaftNodeId, RaftNode, E>
where
    E: std::error::Error,
{
    RPCError::Network(NetworkError::new(&RpcTransportError(error.to_string())))
}

/// Construit un [`Network`] par nœud cible, branché sur le canal libp2p partagé
/// du control plane (voir [`RaftNetworkFactory`]).
pub struct NetworkFactory {
    client: NetworkService,
}

impl NetworkFactory {
    #[must_use]
    pub fn new(client: NetworkService) -> Self {
        Self { client }
    }
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = Network;

    async fn new_client(&mut self, _target: RaftNodeId, node: &RaftNode) -> Self::Network {
        Network {
            client: self.client.clone(),
            peer_id: node.peer_id,
        }
    }
}

/// Implémentation de [`RaftNetwork`] pour un nœud Raft distant, adressé par son
/// `PeerId` libp2p et joignable via [`NetworkClient::rpc_to`].
pub struct Network {
    client: NetworkService,
    peer_id: Option<PeerId>,
}

impl Network {
    fn require_peer_id<E>(&self) -> Result<PeerId, RPCError<RaftNodeId, RaftNode, E>>
    where
        E: std::error::Error,
    {
        self.peer_id
            .ok_or_else(|| to_rpc_error(anyhow::anyhow!("le noeud raft n'a pas de peer_id libp2p")))
    }
}

impl RaftNetwork<TypeConfig> for Network {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<RaftNodeId>, RPCError<RaftNodeId, RaftNode, RaftError<RaftNodeId>>> {
        let peer_id = self.require_peer_id()?;
        let call = RpcCall::new(RpcCall::APPEND_ENTRIES, rpc);
        self.client.rpc_to(call, peer_id).await.map_err(to_rpc_error)
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<RaftNodeId>,
        RPCError<RaftNodeId, RaftNode, RaftError<RaftNodeId, InstallSnapshotError>>,
    > {
        let peer_id = self.require_peer_id()?;
        let call = RpcCall::new(RpcCall::INSTALL_SNAPSHOT, rpc);
        self.client.rpc_to(call, peer_id).await.map_err(to_rpc_error)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<RaftNodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<RaftNodeId>, RPCError<RaftNodeId, RaftNode, RaftError<RaftNodeId>>> {
        let peer_id = self.require_peer_id()?;
        let call = RpcCall::new(RpcCall::VOTE, rpc);
        self.client.rpc_to(call, peer_id).await.map_err(to_rpc_error)
    }
}