
use async_trait::async_trait;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
#[cfg(feature = "job-executor")]
use crate::worker::Worker;
use crate::{id::ID, node::NodeId, worker::{WorkerError}};

pub use marie_macros::core_job;

pub type JobId = ID;
// Diffusé sur Gossipsub par le Control Plane
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JobInstance {
    pub id: JobId,
    pub name: String,
    pub args: serde_json::Value
}

/// Cycle de vie d'un job — volontairement découplé de celui de l'agent qu'il
/// exécute (voir [`JobKind::RunAgent`]) : un job représente *un run borné*,
/// pas la vie entière de l'agent. `Completed`, `Failed` et `Yielded` sont
/// tous les trois terminaux — aucun ne redevient jamais `Pending`. Reprendre
/// un agent après un `Yielded` (condition d'attente résolue) ou un `Failed`
/// (nouvelle tentative) se fait en soumettant un *nouveau* [`JobInstance`] portant
/// le même [`GlobalAgentId`] (voir `network::cp::mod::submit_resume_job`),
/// jamais en mutant celui-ci — c'est ce qui permet à
/// `ControlPlaneState::jobs` de rester un simple historique append-only de
/// runs plutôt qu'un état de session à faire évoluer en place.
#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum JobState {
    #[default]
    Unknown,
    PendingScheduling { instance: JobInstance, worker: NodeId},
    PendingAck { worker: NodeId },
    Probing { worker: NodeId },
    Scheduled { worker: NodeId },
    Running { worker: NodeId },
    Completed(serde_json::Value),
    Failed { error: WorkerError },
}

impl JobState {
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
