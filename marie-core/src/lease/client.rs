use std::sync::Arc;

use async_trait::async_trait;
use libp2p::PeerId;
use openraft::error::{ClientWriteError, RaftError};
use parking_lot::Mutex;

use crate::{
    annuary::{capabilities::Capabilities, Annuary},
    bail, err,
    lease::{
        protocol::{LeaseOp, LeaseRequest, LeaseResult},
        raft::{LeaseNodeId, LeaseRaft},
    },
    rpc::{RemoteProcedureCall, RpcClient},
    session::SessionId,
};

/// RPC servie par tout nœud du groupe Raft de
/// [`crate::lease::authority::LeaseAuthority`] : reçoit une [`LeaseRequest`]
/// et la soumet à son `Raft` local via `client_write` — si ce nœud n'est pas
/// le leader, openraft renvoie une `ClientWriteError::ForwardToLeader` (voir
/// [`LeaseClient::retry_on_forward`], qui l'utilise pour retenter contre le
/// leader indiqué plutôt que de faire suivre l'appel elle-même).
#[derive(Clone)]
pub struct SubmitLease(pub LeaseRaft);

#[async_trait]
impl RemoteProcedureCall for SubmitLease {
    const NAME: &'static str = "/marie/lease/submit";
    type Args = LeaseRequest;
    type Return = Result<LeaseResult, RaftError<LeaseNodeId, ClientWriteError<LeaseNodeId, crate::node::NodeId>>>;

    async fn execute(self, args: Self::Args, _caller: crate::node::NodeId) -> Self::Return {
        self.0.client_write(args).await.map(|response| response.data)
    }
}

/// Client léger pour acquérir/renouveler/relâcher un bail de session — même
/// silhouette que `SessionStoreClient`/`WorkspaceClient` (enveloppe fine
/// autour d'un appel RPC), sans dépendre directement d'un `Raft` local :
/// n'importe quel pair `Capability::Lease` peut recevoir la requête, celui
/// qui la reçoit se charge de la soumettre à son propre `Raft` (voir
/// [`SubmitLease`]) et, s'il n'est pas leader, cette étape est retentée une
/// fois contre le leader indiqué.
#[derive(Clone)]
pub struct LeaseClient {
    rpc_client: RpcClient,
    annuary: Annuary,
    /// Dernier leader connu — évite de retenter `candidate()` à chaque appel
    /// une fois le leader découvert une première fois. Volontairement
    /// best-effort : un leader qui change entre-temps se traduit simplement
    /// par un nouveau `ForwardToLeader`, géré comme n'importe quel autre.
    leader_hint: Arc<Mutex<Option<PeerId>>>,
}

impl LeaseClient {
    pub fn new(rpc_client: RpcClient, annuary: Annuary) -> Self {
        Self { rpc_client, annuary, leader_hint: Arc::new(Mutex::new(None)) }
    }

    pub async fn acquire(&self, session_id: SessionId, holder: PeerId, ttl: chrono::Duration) -> crate::Result<LeaseResult> {
        self.submit(session_id, LeaseOp::Acquire { holder, ttl }).await
    }

    pub async fn renew(&self, session_id: SessionId, holder: PeerId, epoch: u64, ttl: chrono::Duration) -> crate::Result<LeaseResult> {
        self.submit(session_id, LeaseOp::Renew { holder, epoch, ttl }).await
    }

    pub async fn release(&self, session_id: SessionId, holder: PeerId, epoch: u64) -> crate::Result<LeaseResult> {
        self.submit(session_id, LeaseOp::Release { holder, epoch }).await
    }

    async fn submit(&self, session_id: SessionId, op: LeaseOp) -> crate::Result<LeaseResult> {
        let request = LeaseRequest { request_id: crate::id::generate_id(), session_id, op };

        let Some(target) = self.candidate() else {
            bail!("aucun pair Capability::Lease connu pour soumettre la requête de bail");
        };

        match self.rpc_client.invoke::<SubmitLease>(request.clone(), [target]).await? {
            Ok(result) => Ok(result),
            Err(raft_error) => self.retry_on_forward(request, raft_error, target).await,
        }
    }

    /// Pair candidat pour une première tentative : le dernier leader connu,
    /// sinon n'importe quel pair `Capability::Lease` connu d'[`Annuary`].
    fn candidate(&self) -> Option<PeerId> {
        if let Some(hint) = *self.leader_hint.lock() {
            return Some(hint);
        }

        let mut lease_capability = Capabilities::default();
        lease_capability.set_lease();
        self.annuary.peers_with(lease_capability).into_iter().next()
    }

    async fn retry_on_forward(
        &self,
        request: LeaseRequest,
        error: RaftError<LeaseNodeId, ClientWriteError<LeaseNodeId, crate::node::NodeId>>,
        tried: PeerId,
    ) -> crate::Result<LeaseResult> {
        let message = error.to_string();

        let leader_node = match error {
            RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) => forward.leader_node,
            _ => None,
        };

        let Some(leader_node) = leader_node else {
            bail!("échec de la soumission de la requête de bail sur {tried} : {message}");
        };

        let target: PeerId = leader_node.into();
        *self.leader_hint.lock() = Some(target);

        self.rpc_client
            .invoke::<SubmitLease>(request, [target])
            .await?
            .map_err(|error| err!("échec de la soumission de la requête de bail sur le leader {target} : {error}"))
    }
}
