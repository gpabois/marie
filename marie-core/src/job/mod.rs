use std::collections::HashMap;

use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use crate::id::ID;
use tokio::sync::{mpsc, oneshot};

use crate::agent::{GlobalAgentId, status::YieldStatus};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobKind {
    RunAgent(GlobalAgentId)
}

pub type JobId = ID;
// Diffusé sur Gossipsub par le Control Plane
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: ID,
    pub kind: JobKind,
}

/// Cycle de vie d'un job — volontairement découplé de celui de l'agent qu'il
/// exécute (voir [`JobKind::RunAgent`]) : un job représente *un run borné*,
/// pas la vie entière de l'agent. `Completed`, `Failed` et `Yielded` sont
/// tous les trois terminaux — aucun ne redevient jamais `Pending`. Reprendre
/// un agent après un `Yielded` (condition d'attente résolue) ou un `Failed`
/// (nouvelle tentative) se fait en soumettant un *nouveau* [`Job`] portant
/// le même [`GlobalAgentId`] (voir `network::cp::mod::submit_resume_job`),
/// jamais en mutant celui-ci — c'est ce qui permet à
/// `ControlPlaneState::jobs` de rester un simple historique append-only de
/// runs plutôt qu'un état de session à faire évoluer en place.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum JobState {
    Pending,
    Scheduled { worker: PeerId },
    /// `worker` : rapporté par le worker lui-même (voir
    /// `network::worker::report_job_state`), pas recalculé par le control
    /// plane — nécessaire pour dériver les détenteurs actifs d'une session
    /// directement depuis `jobs` (voir `ControlPlaneState::session_holders`)
    /// sans pointeur séparé.
    Running { worker: PeerId },
    Completed { result: String },
    Failed { error: String },
    /// Le run s'est arrêté sans conclure (voir [`YieldStatus`]) — terminal,
    /// comme `Completed`/`Failed` : voir la note sur ce type.
    Yielded { reason: YieldStatus },
}
