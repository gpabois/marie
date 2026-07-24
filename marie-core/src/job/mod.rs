
use async_trait::async_trait;
use libp2p::PeerId;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use crate::{id::ID, worker::{JobContext, server::WorkerServer}};

pub type JobId = ID;
// Diffusé sur Gossipsub par le Control Plane
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobInstance {
    pub id: ID,
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
    Pending,
    Scheduled { worker: PeerId },
    /// `worker` : rapporté par le worker lui-même (voir
    /// `network::worker::report_job_state`), pas recalculé par le control
    /// plane — nécessaire pour dériver les détenteurs actifs d'une session
    /// directement depuis `jobs` (voir `ControlPlaneState::session_holders`)
    /// sans pointeur séparé.
    Running { worker: PeerId },
    Completed(serde_json::Value),
    Failed { error: String },
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
    async fn execute(self, args: Self::Args, cx: JobContext) -> Result<Self::Return, anyhow::Error>;

    #[cfg(feature = "job-executor")]
    fn register(self, worker: &mut WorkerServer<JobContext>) where Self: Clone + Send + Sync + 'static {
        let func = move |cx, args| {
            self.clone().execute(args, cx)
        };

        worker.register_job_executor(Self::NAME, func);
    }
}
