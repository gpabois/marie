use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    job::{JobId, JobState},
    layer::{IntoService as _, LayerExt as _},
    network::{swarm::SwarmNetwork},
    worker::layers::WorkerEventLayer,
    pubsub::{PubSubMessage, layers::PubSubLayer},
    rpc::RpcError,
};

#[cfg(feature = "worker")]
use typed_builder::TypedBuilder;

#[cfg(feature = "worker")]
use crate::{
    job::JobInstance,
    bootstrap::{self, client::BootstrapArgs},
    watchdog::{WorkerWatchdog, WorkerWatchdogArgs},
    rpc,
    session::client::SessionClient, tools::{builtin::register_builtins_tools_executors, worker::{ToolWorkerArgs, ToolWorker}}
};

pub mod info;
pub mod client;

#[cfg(feature="worker-server")]
pub mod server;
pub mod protocol;
pub use protocol::*;
#[cfg(feature="worker-server")]
pub use server::{WorkerServer, WorkerServerArgs};

pub(crate) mod layers;
#[cfg(feature="worker-watchdog")]
mod watchdog;
#[cfg(feature="worker-watchdog")]
pub use watchdog::{WorkerWatchdog, WorkerWatchdogArgs};

pub use client::WorkerClient;

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

#[cfg(feature = "worker")]
#[derive(TypedBuilder)]
pub struct WorkerArgs {
    tools: ToolWorkerArgs
}
