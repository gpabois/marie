use serde::{Deserialize, Serialize};

use crate::{job::JobState, session::protocol::FrameResponse};


#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FrameStatus {
    #[default]
    Pending,
    RunCompleted(FrameResponse),
    RunFailed(String),
    Failed(String),
    Completed,
    Ready,
    Running(JobState),
    RunExhausted,
    WaitingChildren
}

impl FrameStatus {
    pub fn has_terminated(&self) -> bool {
        matches!(self, FrameStatus::Failed(_) | FrameStatus::Completed)
    }

    pub fn has_completed(&self) -> bool {
        matches!(self, FrameStatus::Completed)
    }

    pub fn has_failed(&self) -> bool {
        matches!(self, FrameStatus::Failed(_))
    }
}