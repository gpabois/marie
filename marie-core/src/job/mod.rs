
use std::fmt;

use async_trait::async_trait;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
#[cfg(feature = "job-executor")]
use crate::worker::Worker;
use crate::{id::ID, node::NodeId, worker::{WorkerError}};

pub use marie_macros::core_job;

#[derive(Debug, Hash, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobId(ID);

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<[u8]> for JobId {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl From<ID> for JobId {
    fn from(value: ID) -> Self {
        Self(value)
    }
}

// Diffusé sur Gossipsub par le Control Plane
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobInstance {
    pub id: JobId,
    pub name: String,
    pub args: serde_json::Value
}

#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum JobState<R=Value> {
    #[default]
    Unknown,
    PendingScheduling { instance: JobInstance, worker: NodeId},
    PendingAck { worker: NodeId },
    Probing { worker: NodeId },
    Scheduled { worker: NodeId },
    Running { worker: NodeId },
    Completed(R),
    Failed { error: WorkerError },
}

impl JobState<Value> {
    pub fn deserialize<R>(self) -> Result<JobState<R>, WorkerError> where R: DeserializeOwned {
        Ok(match self {
            JobState::Unknown => JobState::Unknown,
            JobState::PendingScheduling { instance, worker } => JobState::PendingScheduling { instance, worker },
            JobState::PendingAck { worker } => JobState::PendingAck { worker },
            JobState::Probing { worker } => JobState::Probing { worker },
            JobState::Scheduled { worker } => JobState::Scheduled { worker },
            JobState::Running { worker } => JobState::Running { worker },
            JobState::Completed(value) => JobState::Completed(serde_json::from_value(value).map_err(|err| WorkerError::SerdeError(err.to_string()))?),
            JobState::Failed { error } => JobState::Failed { error },
        })
    }
}


impl<R> JobState<R> {
    pub fn has_terminated(&self) -> bool {
        use JobState::{ Completed, Failed };

        matches!(self, Completed(_) | Failed {..})
    }

    pub fn is_probing(&self) -> bool {
        use JobState::Probing;
        matches!(self, Probing{..})
    }

    pub fn is_pending_ack(&self) -> bool {
        use JobState::PendingAck;

        matches!(self, PendingAck{..})
    }
}

/// Calqué sur [`crate::rpc::RemoteProcedureCall`] : sans ce trait, le nom
/// d'un job (`Job::NAME`, la clé de dispatch envoyée sur
/// [`crate::network::worker::RPC_SCHEDULE_JOB`]) et les types de ses
/// `Args`/`Return` étaient dispersés entre une constante `JOB_*` et les
/// closures passées à `WorkerServer::register_job_executor`/
/// `WorkerClient::spawn` — rien n'empêchait le nom utilisé côté appelant de
/// diverger silencieusement de celui enregistré côté worker. Colocaliser les
/// trois sur un seul type élimine ce risque à la compilation.
#[async_trait]
pub trait Job: Sized {
    const NAME: &'static str;
    type Args: Serialize + DeserializeOwned;
    type Return: Serialize + DeserializeOwned;

    #[cfg(feature = "job-executor")]
    async fn execute(self, args: Self::Args) -> Result<Self::Return, crate::Error>;

    #[cfg(feature = "job-executor")]
    fn register(self, worker: &mut Worker) where Self: Clone + Send + Sync + 'static {
        let func = move |args| {
            self.clone().execute(args)
        };

        worker.register_job_executor(Self::NAME, func);
    }
}
