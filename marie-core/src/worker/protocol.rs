use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{events::{Event, EventEnvelope}, node::NodeId, job::{JobId, JobState}, worker::WorkerError};

#[derive(Clone, Serialize, Deserialize)]
pub enum WorkerEvent {
    JobUpdate {
        id: JobId,
        state: JobState
    }
}

impl Event for WorkerEvent {
    const TOPIC: &str = "/marie/workers/events";

    fn id(&self) -> String {
        match self {
            WorkerEvent::JobUpdate { id, .. } => id.to_string()
        }
    }

    fn topics(&self) -> Vec<String> {
        vec![<Self as Event>::TOPIC.to_string()]
    }
}

pub enum WorkMessage {
    Ack(JobAck)
}

impl From<JobAck> for WorkMessage {
    fn from(value: JobAck) -> Self {
        WorkMessage::Ack(value)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct JobAck {
    pub worker_id: NodeId,
    pub job_id: JobId
}