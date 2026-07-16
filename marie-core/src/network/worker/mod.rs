use libp2p::rendezvous::Namespace;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use typed_builder::TypedBuilder;

use crate::{job::JobId, layer::{IntoService as _, LayerExt as _}, model::client::ModelClient, network::{actor::NetworkActor, bootstrap::BootstrapClientActor, create_swarm, mux::FrameLayer, rpc::RpcMuxLayer, worker::{layers::WorkerEventLayer, server::{WorkerServer, WorkerServerActor, WorkerServerArgs}}}, pubsub::layer::PubSubLayer, rpc::{RpcClient, RpcError, RpcServer, RpcServerActor}, secret::{SecretKey, SecretManager}};

pub mod info;
pub mod client;
pub mod server;
mod layers;

pub const RPC_EXECUTE_JOB: &str = "marie/worker/execute";
pub const RPC_WATCH_JOB: &str = "marie/worker/watch";

pub const NS_WORKER: &str = "marie/ns/workers";
pub const NS_WORKER_WATCHDOG: &str = "marie/ns/workers/watchdogs";

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error("aucun worker n'est accessible")]
    NoWorkerFound,
    #[error("aucun watchdog n'est accessible")]
    NoWatchdogFound,
    #[error("erreur lors de l'appel distant")]
    RpcError(#[from] RpcError)
}

#[derive(Clone, Serialize, Deserialize)]
pub enum JobResult {
    Success,
    Failed(String)
}

pub struct JobContext {}

#[derive(Serialize, Deserialize)]
pub enum WorkerEvent {
    JobExecutionDone {
        id: JobId,
        result: JobResult
    }
}

impl WorkerEvent {
    pub const TOPIC_PREFIX: &str = "marie/workers/events";

    pub fn topic(&self) -> String {
        match self {
            WorkerEvent::JobExecutionDone { .. } => format!("{0}/job-done", Self::TOPIC_PREFIX),
        }
    }
}

#[derive(TypedBuilder)]
pub struct WorkerArgs {
    master_key: SecretKey
}

/// `secret` : secret partagé par le cluster, utilisé pour vérifier
/// automatiquement qu'un pair prétendant être control plane l'est vraiment
/// (voir `secret::SecretManager::verify_membership` et
/// `network::actor::NetworkActor`) avant de lui faire confiance et de lui
/// envoyer des jobs.
///
/// `pool`/`store` : backends du VFS des sessions (voir
/// `persistency::vfs::WorkspaceVfs`), partagés par tous les workers du
/// cluster — `store` au choix via `persistency::FilesystemConfig`.
///
/// `rust_registry` : fonctions Rust utilisables comme `Executable::Rust` par
/// les nœuds/arêtes d'un `mode::state_graph::StateGraph` exécuté sur ce
/// worker (voir [`RustRegistry`]) — à peupler par l'appelant avant ou après
/// `start`, l'instance passée ici reste modifiable ensuite (`RustRegistry`
/// est bon marché à cloner, mutation intérieure). Un
/// [`AgentRuntime`](crate::mode::executable::AgentRuntime) est construit ici
/// même, à partir du [`NetworkClient`] de ce worker, pour les nœuds
/// `Executable::Agent` d'un tel graphe — pas besoin de le recevoir en
/// paramètre, contrairement à `rust_registry` : il n'y a rien à peupler par
/// avance, juste des clients vers le control plane.
///
/// `ready` : signalé avec le [`NetworkClient`] de ce nœud dès la connexion
/// établie, avant que la boucle ci-dessous ne démarre — voir
/// `node::Marie::start`.
///
/// `shutdown` : demande d'arrêt propre (voir `node::MarieHandle::shutdown`)
/// — la boucle cesse d'accepter de nouveaux événements dès qu'elle se
/// déclenche, puis les jobs déjà en vol (voir [`execute_rpc`]) ont jusqu'à
/// [`SHUTDOWN_GRACE_PERIOD`] pour rapporter leur issue (voir
/// [`drain_job_tasks`]) avant que la connexion réseau ne soit coupée.
pub async fn start_worker(args: WorkerArgs) -> Result<(), anyhow::Error> {
    use super::NodeKind::Worker;

    let swarm = create_swarm(Worker, |_| {}).await?;
    let local_peer_id = *swarm.local_peer_id();
    
    let secret_mngr = SecretManager::new(&args.master_key);


    let net = NetworkActor::new(swarm, Worker);

    // on démarre un client bootstrap qui va s'enregistrer sur le namespace des workers
    let boostrap = BootstrapClientActor::new(
        net.transport(), 
        local_peer_id, 
        [Namespace::from_static(NS_WORKER)]
    );

    let mut rpc_client: RpcClient = net.transport()
        .chain::<FrameLayer, _>(())
        .chain::<RpcMuxLayer, _>(())
        .into_service(());


    let mut rpc_server: RpcServer = net.transport()
        .chain::<FrameLayer, _>(())
        .chain::<RpcMuxLayer, _>(())
        .into_service(());

    
    let worker_args = WorkerServerArgs::builder()
        .rpc_server(rpc_server)
        .job_context_builder(move |job| ())
        .build();

    let mut worker_server = net.transport()
        .chain::<PubSubLayer, _>(())
        .chain::<WorkerEventLayer, _>(())
        .into_service(worker_args);


    net.listen();

    Ok(())
}


pub async fn start_watchdog() -> Result<(), anyhow::Error> {
    use super::NodeKind::WorkerWatchdog;

    let swarm = create_swarm(WorkerWatchdog, |_| {}).await?;
    let local_peer_id = *swarm.local_peer_id();

    let net = NetworkActor::new(swarm, WorkerWatchdog);

    // on démarre un client bootstrap qui va s'enregistrer sur le namespace des workers watchdogs
    let boostrap = BootstrapClientActor::new(
        net.transport(), 
        local_peer_id, 
        [Namespace::from_static(NS_WORKER_WATCHDOG)]
    );

    Ok(())
}