use serde::{Deserialize, Serialize};
use thiserror::Error;

#[cfg(feature = "worker")]
use typed_builder::TypedBuilder;

pub mod info;
pub mod client;

#[cfg(feature="worker-server")]
pub mod server;

mod protocol;
pub mod rpc;
pub use protocol::*;
#[cfg(feature="worker-server")]
pub use server::{Worker, WorkerArgs};

pub use client::WorkerClient;

use crate::job::JobId;

pub const RPC_SCHEDULE_JOB: &str = "marie/worker/schedule";
pub const RPC_WATCH_JOB: &str = "marie/worker/watch";
pub const RPC_GET_STATE_JOB: &str = "marie/worker/job/get-state";

pub const NS_WORKER: &str = "marie/ns/workers";
pub const NS_WORKER_WATCHDOG: &str = "marie/ns/workers/watchdogs";

#[derive(Debug, Clone, Error, Serialize, Deserialize, PartialEq)]
pub enum WorkerError {
    #[error("aucun exécuteur de travail trouvé pour {0}")]
    NoJobExecutorFound(String),
    #[error("le job {0} n'a pas été trouvé")]
    NoJobFound(JobId),
    #[error("l'exécution du travail a paniqué : {0}")]
    Panicked(String),
    #[error("l'exécution a échoué : {0}")]
    ExecutionError(crate::Error),
    #[error("aucun travailleur disponible")]
    NoWorkerAvailable,
    #[error("le sondage a expiré sans réponse")]
    ProbingTimeOut,
    #[error("le travailleur n'a pas reconnu la tâche dans les temps")]
    AckTimeOut,
    #[error("une erreur est survenue pendant la programmation de la tâche: {0}")]
    ScheduledError(String),
    #[error("erreur lors de l'appel RPC : {0}")]
    RpcError(String),
}

/// Pas de `#[from]` direct (voir `ScheduledError`, qui suit le même
/// principe) : `crate::rpc::RpcError` ne dérive pas `PartialEq`, requis ici
/// par `#[derive(PartialEq)]` sur [`WorkerError`] — le message est donc
/// converti en `String` plutôt que le type d'erreur conservé tel quel.
impl From<crate::rpc::RpcError> for WorkerError {
    fn from(error: crate::rpc::RpcError) -> Self {
        WorkerError::RpcError(error.to_string())
    }
}
