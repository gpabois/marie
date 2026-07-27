use std::fmt;
use std::io::Cursor;

use async_trait::async_trait;
use libp2p::PeerId;
use openraft::{
    error::{NetworkError, RPCError, RaftError, RemoteError},
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest,
        VoteResponse,
    },
    Raft,
};
use serde::{Deserialize, Serialize};

use crate::{
    lease::protocol,
    rpc::{RemoteProcedureCall, RpcClient},
};

/// Identifiant de nœud openraft pour le groupe Raft de
/// [`crate::lease::authority::LeaseAuthority`].
///
/// openraft exige `NodeId: Copy` (voir `openraft::node::NodeIdEssential`), ce
/// que `crate::node::NodeId` (qui enveloppe un `Vec<u8>`, la représentation
/// brute d'un `PeerId`) ne peut pas fournir — d'où ce type dédié : un hash
/// stable et déterministe du `PeerId`, calculé indépendamment par chaque
/// nœud à partir des mêmes octets, sans registre central à tenir à jour.
/// L'adressage réel (retrouver le `PeerId` pour router un appel) passe par le
/// type `Node` de la config (voir [`LeaseTypeConfig`]), qui réutilise
/// `crate::node::NodeId` — déjà `Default + Clone + Serialize/Deserialize` et
/// convertible depuis/vers `PeerId`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct LeaseNodeId(u64);

impl fmt::Display for LeaseNodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

impl From<PeerId> for LeaseNodeId {
    fn from(value: PeerId) -> Self {
        LeaseNodeId(stable_hash(&value.to_bytes()))
    }
}

impl From<&PeerId> for LeaseNodeId {
    fn from(value: &PeerId) -> Self {
        LeaseNodeId::from(*value)
    }
}

impl From<&crate::node::NodeId> for LeaseNodeId {
    /// Un [`crate::node::NodeId`] enveloppe déjà les octets bruts d'un
    /// `PeerId` (voir `node.rs`) — hasher directement ces octets évite un
    /// aller-retour `NodeId -> PeerId -> LeaseNodeId` partout où ce module ne
    /// connaît qu'un `NodeId` (ex. la composition du groupe Raft, dérivée
    /// d'`Annuary`).
    fn from(value: &crate::node::NodeId) -> Self {
        LeaseNodeId(stable_hash(value.as_ref()))
    }
}

const FNV_OFFSET: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

/// FNV-1a + fmix64 (MurmurHash3) : même recette que celle, privée, de
/// [`crate::hrw`] — dupliquée ici plutôt qu'exportée depuis ce module partagé,
/// ce hash n'a de sens que pour dériver un [`LeaseNodeId`] stable, pas comme
/// utilitaire général de répartition de charge.
fn stable_hash(bytes: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51afd7ed558ccd);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xc4ceb9fe1a85ec53);
    hash ^= hash >> 33;
    hash
}

openraft::declare_raft_types!(
    pub LeaseTypeConfig:
        D = protocol::LeaseRequest,
        R = protocol::LeaseResult,
        NodeId = LeaseNodeId,
        Node = crate::node::NodeId,
);

pub type LeaseRaft = Raft<LeaseTypeConfig>;

// --- RPC de transport Raft (vote / append_entries / install_snapshot) ---
//
// Écrits à la main plutôt que via la macro `core_rpc!` : celle-ci génère
// aujourd'hui un `execute(self, args, caller: PeerId)` qui ne correspond plus
// à la vraie signature de `RemoteProcedureCall::execute`
// (`caller: crate::node::NodeId`, voir `rpc::mod`) — même écart que celui,
// préexistant, visible dans `workspace/rpc.rs`/`session/rpc.rs`. Un correctif
// de la macro est hors du périmètre de cette passe ; écrire ces trois `impl`
// à la main, avec la bonne signature, l'évite simplement.

#[derive(Clone)]
pub struct RaftVote(pub LeaseRaft);

#[async_trait]
impl RemoteProcedureCall for RaftVote {
    const NAME: &'static str = "/marie/lease/raft/vote";
    type Args = VoteRequest<LeaseNodeId>;
    type Return = Result<VoteResponse<LeaseNodeId>, RaftError<LeaseNodeId>>;

    async fn execute(self, args: Self::Args, _caller: crate::node::NodeId) -> Self::Return {
        self.0.vote(args).await
    }
}

#[derive(Clone)]
pub struct RaftAppendEntries(pub LeaseRaft);

#[async_trait]
impl RemoteProcedureCall for RaftAppendEntries {
    const NAME: &'static str = "/marie/lease/raft/append-entries";
    type Args = AppendEntriesRequest<LeaseTypeConfig>;
    type Return = Result<AppendEntriesResponse<LeaseNodeId>, RaftError<LeaseNodeId>>;

    async fn execute(self, args: Self::Args, _caller: crate::node::NodeId) -> Self::Return {
        self.0.append_entries(args).await
    }
}

#[derive(Clone)]
pub struct RaftInstallSnapshot(pub LeaseRaft);

#[async_trait]
impl RemoteProcedureCall for RaftInstallSnapshot {
    const NAME: &'static str = "/marie/lease/raft/install-snapshot";
    type Args = InstallSnapshotRequest<LeaseTypeConfig>;
    type Return = Result<InstallSnapshotResponse<LeaseNodeId>, RaftError<LeaseNodeId, openraft::error::InstallSnapshotError>>;

    async fn execute(self, args: Self::Args, _caller: crate::node::NodeId) -> Self::Return {
        self.0.install_snapshot(args).await
    }
}

/// [`RaftNetworkFactory`] adossée à [`RpcClient`] : une instance par nœud
/// cible, construite à la demande par openraft (voir sa doc — ne doit pas
/// ouvrir de connexion elle-même, `RpcClient` s'appuie de toute façon sur le
/// swarm libp2p déjà connecté).
#[derive(Clone)]
pub struct LeaseNetworkFactory(pub RpcClient);

// Pas de `#[async_trait]` ici : les traits `RaftNetworkFactory`/`RaftNetwork`
// sont annotés côté openraft par leur propre macro (`#[add_async_trait]`,
// support natif des `async fn` en trait), incompatible avec la désucrification
// du crate `async-trait` (l'`impl` obtiendrait alors des durées de vie
// différentes de la déclaration du trait — erreur E0195).
impl RaftNetworkFactory<LeaseTypeConfig> for LeaseNetworkFactory {
    type Network = LeaseNetwork;

    async fn new_client(&mut self, target: LeaseNodeId, node: &crate::node::NodeId) -> Self::Network {
        LeaseNetwork { rpc_client: self.0.clone(), target, node: node.clone() }
    }
}

pub struct LeaseNetwork {
    rpc_client: RpcClient,
    target: LeaseNodeId,
    node: crate::node::NodeId,
}

impl LeaseNetwork {
    /// Traduit un échec de transport (`RpcClient::invoke`) en
    /// `RPCError::Network` — la réponse `Err(RaftError)` d'un pair
    /// effectivement joint est distincte, voir [`Self::remote`].
    fn transport_error<E: std::error::Error>(err: crate::rpc::RpcError) -> RPCError<LeaseNodeId, crate::node::NodeId, E> {
        RPCError::Network(NetworkError::new(&err))
    }

    /// Traduit la réponse `Err(RaftError)` renvoyée par le pair `target` en
    /// `RPCError::RemoteError`.
    fn remote<E: std::error::Error>(&self, err: RaftError<LeaseNodeId, E>) -> RPCError<LeaseNodeId, crate::node::NodeId, RaftError<LeaseNodeId, E>> {
        RPCError::RemoteError(RemoteError::new(self.target, err))
    }
}

impl RaftNetwork<LeaseTypeConfig> for LeaseNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<LeaseTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<LeaseNodeId>, RPCError<LeaseNodeId, crate::node::NodeId, RaftError<LeaseNodeId>>> {
        let result = self.rpc_client.invoke::<RaftAppendEntries>(rpc, [self.node.clone()]).await.map_err(Self::transport_error)?;
        result.map_err(|err| self.remote(err))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<LeaseNodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<LeaseNodeId>, RPCError<LeaseNodeId, crate::node::NodeId, RaftError<LeaseNodeId>>> {
        let result = self.rpc_client.invoke::<RaftVote>(rpc, [self.node.clone()]).await.map_err(Self::transport_error)?;
        result.map_err(|err| self.remote(err))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<LeaseTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<LeaseNodeId>,
        RPCError<LeaseNodeId, crate::node::NodeId, RaftError<LeaseNodeId, openraft::error::InstallSnapshotError>>,
    > {
        let result = self.rpc_client.invoke::<RaftInstallSnapshot>(rpc, [self.node.clone()]).await.map_err(Self::transport_error)?;
        result.map_err(|err| self.remote(err))
    }
}
