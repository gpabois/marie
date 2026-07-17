use libp2p::rendezvous::Namespace;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use typed_builder::TypedBuilder;

use crate::{job::{Job, JobId, JobState}, layer::{IntoService as _, LayerExt as _}, model::client::ModelClient, network::{actor::{Network, NetworkActor}, bootstrap::{self, BootstrapClientActor, client::BootstrapArgs}, create_swarm, mux::FrameLayer, rpc::RpcMuxLayer, worker::{layers::WorkerEventLayer, server::{WorkerServer, WorkerServerActor, WorkerServerArgs}, watchdog::{WorkerWatchdog, WorkerWatchdogArgs}}}, pubsub::{PubSubMessage, layers::PubSubLayer}, rpc::{self, RpcClient, RpcError, RpcServer, RpcServerActor}, secret::{SecretKey, SecretManager}, tools::{JOB_TOOL_EXECUTE, ToolCall, worker::ToolWorker}};

pub mod info;
pub mod client;
pub mod server;
mod layers;
pub mod watchdog;

pub const RPC_SCHEDULE_JOB: &str = "marie/worker/schedule";
pub const RPC_WATCH_JOB: &str = "marie/worker/watch";
pub const RPC_GET_STATE_JOB: &str = "marie/worker/job/get-state";

pub const NS_WORKER: &str = "marie/ns/workers";
pub const NS_WORKER_WATCHDOG: &str = "marie/ns/workers/watchdogs";

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error("aucun worker n'est accessible")]
    NoWorkerFound,
    #[error("aucun watchdog n'est accessible")]
    NoWatchdogFound,
    #[error("erreur lors de l'appel distant")]
    RpcError(#[from] RpcError),
    #[error("ce n'est pas un évènement du worker")]
    NotWorkerEvent
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobResult {
    Success(serde_json::Value),
    Failed(String)
}

pub struct JobContext {}

#[derive(Serialize, Deserialize)]
pub enum WorkerEvent {
    JobDone {
        id: JobId,
        result: JobResult
    },
    JobStateUpdate {
        id: JobId,
        state: JobState
    }
}

impl TryFrom<PubSubMessage> for WorkerEvent {
    type Error = WorkerError;

    fn try_from(value: PubSubMessage) -> Result<Self, Self::Error> {
        use WorkerError::NotWorkerEvent;

        if !Self::is(&value) { return Err(NotWorkerEvent) };

        serde_json::from_slice(&value.payload).map_err(|_| NotWorkerEvent)
    }
}

impl WorkerEvent {
    pub fn is(msg:& PubSubMessage) -> bool {
        msg.topic.starts_with(Self::TOPIC_PREFIX)
    }
}

impl WorkerEvent {
    pub const TOPIC_PREFIX: &str = "marie/workers/events";

    pub fn topic(&self) -> String {
        match self {
            WorkerEvent::JobDone { .. } => format!("{0}/job-done", Self::TOPIC_PREFIX),
            WorkerEvent::JobStateUpdate { id, state } => todo!(),
        }
    }
}

pub struct JobContext {

}

#[derive(TypedBuilder)]
pub struct WorkerArgs {
    master_key: SecretKey
}

pub async fn start_worker(args: WorkerArgs) -> Result<(), anyhow::Error> {
    use super::NodeKind::Worker;

    let swarm = create_swarm(Worker).await?;
    let local_peer_id = *swarm.local_peer_id();
    
    let net = NetworkActor::new(swarm, Worker);

    // on démarre un client bootstrap qui va s'enregistrer sur le namespace des workers
    let bootstrap = bootstrap::build_client(&net, BootstrapArgs::builder().local_peer_id(local_peer_id).build());
    
    let worker_args = WorkerServerArgs::builder()
        .rpc_server(rpc::build_server(&net))
        .bootstrap(bootstrap.clone())
        .job_context_builder(|_| JobContext {})
        .build();

    let worker_server = build_server(&net, worker_args);
    let tool_worker = ToolWorker::new(&worker_server);

    net.clone().listen().await;

    Ok(())
}

/// Démarre un worker watchdog
pub async fn start_watchdog() -> Result<(), anyhow::Error> {
    use super::NodeKind::WorkerWatchdog;

    let swarm = create_swarm(WorkerWatchdog).await?;
    let local_peer_id = *swarm.local_peer_id();

    let net = NetworkActor::new(swarm, WorkerWatchdog);

    // on démarre un client bootstrap qui va s'enregistrer sur le namespace des workers watchdogs
    let bootstrap = bootstrap::build_client(&net, BootstrapArgs::builder().local_peer_id(local_peer_id).build());

    let args = WorkerWatchdogArgs::builder()
        .bootstrap(bootstrap)
        .rpc_client(rpc::build_client(&net))
        .rpc_server(rpc::build_server(&net))
        .build();

    let _watchdog = build_watchdog(&net, args);

    net.listen().await;

    Ok(())
}

pub fn build_server<Cx, B>(net: &Network, args: WorkerServerArgs<Cx, B>) -> WorkerServer<Cx> 
where B: Fn(&Job) -> Cx + Send + Sync + 'static, Cx: Send + 'static
{
    net.transport()
        .chain::<PubSubLayer, _>(())
        .chain::<WorkerEventLayer, _>(())
        .into_service(args)
}

pub fn build_watchdog(net: &Network, args: WorkerWatchdogArgs) -> WorkerWatchdog {
    net.transport()
        .chain::<PubSubLayer, _>(())
        .chain::<WorkerEventLayer, _>(())
        .into_service(args)
}