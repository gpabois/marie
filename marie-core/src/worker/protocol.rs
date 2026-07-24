use serde::{Deserialize, Serialize};

use crate::{job::{JobId, JobState}, pubsub::PubSubMessage, worker::{JobResult, WorkerError}};

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
            WorkerEvent::JobStateUpdate { .. } => format!("{0}/job-state-update", Self::TOPIC_PREFIX),
        }
    }
}
