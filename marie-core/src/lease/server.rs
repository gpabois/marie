use std::{collections::BTreeMap, sync::Arc, time::Duration};

use libp2p::PeerId;
use tracing::warn;

use crate::{
    annuary::{capabilities::Capabilities, Annuary},
    lease::{
        client::SubmitLease,
        raft::{LeaseNetworkFactory, LeaseNodeId, LeaseRaft, RaftAppendEntries, RaftInstallSnapshot, RaftVote},
        storage::{LeaseLogStore, LeaseStateMachine},
    },
    rpc::{RemoteProcedureCall, RpcClient, RpcServer},
};

/// Délai laissé à [`Annuary`] pour découvrir les pairs `Capability::Lease`
/// avant de calculer la composition initiale du groupe Raft — même principe
/// que le délai de bootstrap déterministe de l'ancien control plane Raft
/// (voir `CLAUDE.md`), juste sourcé depuis `Annuary` plutôt qu'un namespace
/// de rendez-vous.
const BOOTSTRAP_DELAY: Duration = Duration::from_secs(5);

/// Construit et démarre le nœud Raft de [`crate::lease::authority::LeaseAuthority`]
/// pour ce processus : stockage en mémoire (voir `lease::storage`), transport
/// RPC adossé à `rpc_client`/`rpc_server` (voir `lease::raft`), composition de
/// groupe calculée depuis `annuary` (voir [`bootstrap`]).
///
/// Fonction volontairement autonome : elle ne construit ni `Annuary` ni le
/// swarm réseau, et n'est appelée depuis aucun point de démarrage de nœud
/// pour cette passe — au contraire, un futur point d'assemblage (ex.
/// `network::catalog::start_catalog`, aujourd'hui même en cours de
/// refonte) est libre de l'appeler une fois `Annuary` disponible.
pub async fn build_lease_authority(
    rpc_client: RpcClient,
    mut rpc_server: RpcServer,
    annuary: Annuary,
    local_peer_id: PeerId,
) -> crate::Result<LeaseRaft> {
    let local_node_id = LeaseNodeId::from(local_peer_id);

    let config = Arc::new(openraft::Config::default().validate()?);
    let network = LeaseNetworkFactory(rpc_client);
    let log_store = LeaseLogStore::default();
    let state_machine = LeaseStateMachine::default();

    let raft = LeaseRaft::new(local_node_id, config, network, log_store, state_machine).await?;

    RaftVote(raft.clone()).register(&mut rpc_server);
    RaftAppendEntries(raft.clone()).register(&mut rpc_server);
    RaftInstallSnapshot(raft.clone()).register(&mut rpc_server);
    SubmitLease(raft.clone()).register(&mut rpc_server);

    tokio::spawn(bootstrap(raft.clone(), annuary, local_peer_id));

    Ok(raft)
}

/// Élection déterministe du nœud qui appelle `raft.initialize(..)` : après
/// [`BOOTSTRAP_DELAY`], chaque nœud lit `annuary.peers_with(Capability::Lease)`
/// (+ lui-même) et n'initialise que s'il détient le [`LeaseNodeId`] le plus
/// bas de cet ensemble — comme l'ancien control plane Raft (voir
/// `CLAUDE.md` : "bootstrap leader élu de façon déterministe — le plus petit
/// id dérivé parmi les pairs vus pendant `BOOTSTRAP_DELAY`"), transposé à
/// `Annuary` au lieu d'un namespace de rendez-vous. Plusieurs nœuds
/// appelant `initialize` avec la même composition est sans risque (voir la
/// doc de `Raft::initialize`) ; ce calcul déterministe n'est qu'une
/// optimisation pour éviter les tentatives concurrentes inutiles.
///
/// Limite connue, hors périmètre de cette passe : la composition n'est
/// calculée qu'une fois, ici — un pair `Capability::Lease` qui rejoint après
/// coup ne devient pas automatiquement votant (pas de `change_membership`
/// dynamique).
async fn bootstrap(raft: LeaseRaft, annuary: Annuary, local_peer_id: PeerId) {
    tokio::time::sleep(BOOTSTRAP_DELAY).await;

    let mut lease_capability = Capabilities::default();
    lease_capability.set_lease();

    let mut peers = annuary.peers_with(lease_capability);
    if !peers.contains(&local_peer_id) {
        peers.push(local_peer_id);
    }

    let lowest = peers.iter().min_by_key(|peer_id| LeaseNodeId::from(**peer_id)).copied();
    if lowest != Some(local_peer_id) {
        return;
    }

    match raft.is_initialized().await {
        Ok(true) => return,
        Err(error) => {
            warn!(%error, "impossible de vérifier l'état d'initialisation du groupe Raft de LeaseAuthority");
            return;
        }
        Ok(false) => {}
    }

    let members: BTreeMap<LeaseNodeId, crate::node::NodeId> =
        peers.into_iter().map(|peer_id| (LeaseNodeId::from(peer_id), crate::node::NodeId::from(peer_id))).collect();

    if let Err(error) = raft.initialize(members).await {
        warn!(%error, "échec de l'initialisation du groupe Raft de LeaseAuthority");
    }
}

#[cfg(test)]
mod tests {
    //! Exerce [`LeaseAuthority`](crate::lease::authority::LeaseAuthority) à
    //! travers un vrai consensus Raft, mais avec un transport bouclé en
    //! mémoire (registre `LeaseNodeId -> LeaseRaft`, appels directs) plutôt
    //! que `RpcClient`/le swarm libp2p réel — le reste du crate ne compile
    //! pas encore intégralement (voir le plan de cette passe), un test
    //! multi-process de bout en bout n'est donc pas réaliste ici.

    use std::{collections::{BTreeMap, HashMap}, sync::Arc, time::Duration};

    use chrono::Duration as ChronoDuration;
    use libp2p::PeerId;
    use openraft::{
        error::{RPCError, RaftError, RemoteError, Unreachable},
        network::{RPCOption, RaftNetwork, RaftNetworkFactory},
        raft::{
            AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest,
            VoteResponse,
        },
    };
    use parking_lot::Mutex;

    use crate::{
        id::generate_id,
        lease::{
            protocol::{LeaseOp, LeaseRequest, LeaseResult},
            raft::{LeaseNodeId, LeaseRaft, LeaseTypeConfig},
            storage::{LeaseLogStore, LeaseStateMachine},
        },
        session::SessionId,
    };

    type Registry = Arc<Mutex<HashMap<LeaseNodeId, LeaseRaft>>>;

    #[derive(Clone)]
    struct LoopbackFactory(Registry);

    impl RaftNetworkFactory<LeaseTypeConfig> for LoopbackFactory {
        type Network = LoopbackNetwork;

        async fn new_client(&mut self, target: LeaseNodeId, _node: &crate::node::NodeId) -> Self::Network {
            LoopbackNetwork { registry: self.0.clone(), target }
        }
    }

    struct LoopbackNetwork {
        registry: Registry,
        target: LeaseNodeId,
    }

    fn target_raft<E: std::error::Error>(
        registry: &Registry,
        target: LeaseNodeId,
    ) -> Result<LeaseRaft, RPCError<LeaseNodeId, crate::node::NodeId, E>> {
        registry.lock().get(&target).cloned().ok_or_else(|| {
            RPCError::Unreachable(Unreachable::new(&std::io::Error::new(std::io::ErrorKind::NotFound, "pair inconnu du registre de test")))
        })
    }

    impl RaftNetwork<LeaseTypeConfig> for LoopbackNetwork {
        async fn append_entries(
            &mut self,
            rpc: AppendEntriesRequest<LeaseTypeConfig>,
            _option: RPCOption,
        ) -> Result<AppendEntriesResponse<LeaseNodeId>, RPCError<LeaseNodeId, crate::node::NodeId, RaftError<LeaseNodeId>>> {
            let raft = target_raft(&self.registry, self.target)?;
            raft.append_entries(rpc).await.map_err(|err| RPCError::RemoteError(RemoteError::new(self.target, err)))
        }

        async fn vote(
            &mut self,
            rpc: VoteRequest<LeaseNodeId>,
            _option: RPCOption,
        ) -> Result<VoteResponse<LeaseNodeId>, RPCError<LeaseNodeId, crate::node::NodeId, RaftError<LeaseNodeId>>> {
            let raft = target_raft(&self.registry, self.target)?;
            raft.vote(rpc).await.map_err(|err| RPCError::RemoteError(RemoteError::new(self.target, err)))
        }

        async fn install_snapshot(
            &mut self,
            rpc: InstallSnapshotRequest<LeaseTypeConfig>,
            _option: RPCOption,
        ) -> Result<
            InstallSnapshotResponse<LeaseNodeId>,
            RPCError<LeaseNodeId, crate::node::NodeId, RaftError<LeaseNodeId, openraft::error::InstallSnapshotError>>,
        > {
            let raft = target_raft(&self.registry, self.target)?;
            raft.install_snapshot(rpc).await.map_err(|err| RPCError::RemoteError(RemoteError::new(self.target, err)))
        }
    }

    fn random_peer_id() -> PeerId {
        libp2p::identity::Keypair::generate_ed25519().public().to_peer_id()
    }

    async fn new_test_node(id: LeaseNodeId, registry: Registry) -> LeaseRaft {
        let config = Arc::new(openraft::Config::default().validate().unwrap());
        LeaseRaft::new(id, config, LoopbackFactory(registry), LeaseLogStore::default(), LeaseStateMachine::default()).await.unwrap()
    }

    async fn wait_for_leader(raft: &LeaseRaft, expected: LeaseNodeId) {
        for _ in 0..200 {
            if raft.current_leader().await == Some(expected) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("timed out waiting for {expected} to become leader");
    }

    #[tokio::test]
    async fn acquire_renew_release_reacquire_bumps_epoch() {
        let peer_a = random_peer_id();
        let peer_b = random_peer_id();
        let peer_c = random_peer_id();

        let id_a = LeaseNodeId::from(peer_a);
        let id_b = LeaseNodeId::from(peer_b);
        let id_c = LeaseNodeId::from(peer_c);

        let registry: Registry = Arc::new(Mutex::new(HashMap::new()));

        let raft_a = new_test_node(id_a, registry.clone()).await;
        let raft_b = new_test_node(id_b, registry.clone()).await;
        let raft_c = new_test_node(id_c, registry.clone()).await;

        registry.lock().insert(id_a, raft_a.clone());
        registry.lock().insert(id_b, raft_b.clone());
        registry.lock().insert(id_c, raft_c.clone());

        let members: BTreeMap<LeaseNodeId, crate::node::NodeId> = [
            (id_a, crate::node::NodeId::from(peer_a)),
            (id_b, crate::node::NodeId::from(peer_b)),
            (id_c, crate::node::NodeId::from(peer_c)),
        ]
        .into_iter()
        .collect();

        raft_a.initialize(members).await.unwrap();
        wait_for_leader(&raft_a, id_a).await;

        let session_id = SessionId::new(generate_id());

        let acquire = |holder: PeerId, epoch_or_none: Option<u64>| {
            let session_id = session_id;
            let raft_a = raft_a.clone();
            async move {
                let op = match epoch_or_none {
                    None => LeaseOp::Acquire { holder, ttl: ChronoDuration::seconds(30) },
                    Some(epoch) => LeaseOp::Renew { holder, epoch, ttl: ChronoDuration::seconds(30) },
                };
                let request = LeaseRequest { request_id: generate_id(), session_id, op };
                raft_a.client_write(request).await.unwrap().data
            }
        };

        // Node A acquiert le bail : premier appel sur une session inconnue, epoch 1.
        let granted = acquire(peer_a, None).await;
        let epoch = match granted {
            LeaseResult::Granted { epoch, .. } => epoch,
            other => panic!("attendu Granted, obtenu {other:?}"),
        };
        assert_eq!(epoch, 1);

        // Node B tente d'acquérir la même session pendant que A la détient toujours : refusé.
        let denied = acquire(peer_b, None).await;
        match denied {
            LeaseResult::Denied { current_holder, current_epoch } => {
                assert_eq!(current_holder, peer_a);
                assert_eq!(current_epoch, epoch);
            }
            other => panic!("attendu Denied, obtenu {other:?}"),
        }

        // A renouvelle son propre bail avec l'epoch qu'il détient.
        let renewed = acquire(peer_a, Some(epoch)).await;
        assert!(matches!(renewed, LeaseResult::Renewed { .. }), "attendu Renewed, obtenu {renewed:?}");

        // A relâche le bail.
        let request = LeaseRequest {
            request_id: generate_id(),
            session_id,
            op: LeaseOp::Release { holder: peer_a, epoch },
        };
        let released = raft_a.client_write(request).await.unwrap().data;
        assert!(matches!(released, LeaseResult::Renewed { .. }), "attendu Renewed (relâché), obtenu {released:?}");

        // Ré-acquisition : l'epoch doit avoir été incrémenté.
        let granted_again = acquire(peer_c, None).await;
        match granted_again {
            LeaseResult::Granted { epoch: new_epoch, .. } => assert_eq!(new_epoch, epoch + 1),
            other => panic!("attendu Granted, obtenu {other:?}"),
        }
    }
}
