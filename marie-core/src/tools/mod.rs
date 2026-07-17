pub mod catalog;
pub mod client;
pub mod server;
pub mod layers;
pub(crate) mod worker;

use std::fmt::Display;

use bytemuck::{Pod, Zeroable};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use crate::{id::ID, job::JobId, network::worker::JobResult, pubsub::PubSubMessage, session::SessionId, tools::client::ToolError};


pub const RPC_TOOL_INSERT: &str = "marie/tools/insert";
pub const RPC_TOOL_GET: &str = "marie/tools/get";
pub const RPC_TOOL_UPDATE: &str = "marie/tools/update";
pub const RPC_TOOL_REMOVE: &str = "marie/tools/remove";
pub const RPC_TOOL_LIST: &str = "marie/tools/list";
pub const RPC_TOOL_EXECUTE: &str = "marie/tools/execute";
pub const JOB_TOOL_EXECUTE: &str = "marie/jobs/tools/execute";
pub const NS_TOOL: &str = "marie/ns/tools";


pub type ToolName = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolId(String);

impl Display for ToolId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<[u8]> for ToolId {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl From<&str> for ToolId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<String> for ToolId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Tool {
    pub name: ToolName,
    pub description: String,
    pub parameters_schema: Value
}

#[derive(Debug, Hash, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct ToolCallId(SessionId, ID);

impl AsRef<[u8]> for ToolCallId {
    fn as_ref(&self) -> &[u8] {
        bytemuck::bytes_of(self)
    }
}

impl ToolCallId {
    pub fn new(session_id: SessionId, id: ID) -> Self {
        Self(session_id, id)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: ToolCallId,
    pub name: ToolName,
    pub parameters: Option<Value>
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ToolCallError {
    TimeOut,
    Custom(String)
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ToolCallResult {
    Success(Option<String>),
    Failed(ToolCallError),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ToolEvent {
    JobDone {
        id: JobId, 
        result: JobResult
    },
    ToolExecutionDone {
        id: ToolCallId,
        result: ToolCallResult
    }
}

impl TryFrom<PubSubMessage> for ToolEvent {
    type Error = ToolError;

    fn try_from(value: PubSubMessage) -> Result<Self, Self::Error> {
        use ToolError::NotToolEvent;

        if !Self::is(&value) { return Err(NotToolEvent) };

        serde_json::from_slice(&value.payload).map_err(|_| NotToolEvent)
    }
}

impl From<ToolEvent> for PubSubMessage {
    fn from(value: ToolEvent) -> Self {
        PubSubMessage { 
            id: String::default(), 
            topic: value.topic(), 
            payload: serde_json::to_vec(&value).unwrap(), 
            source: None
        }
    }
}

impl ToolEvent {
    pub fn topic(&self) -> String {
        match self {
            ToolEvent::ToolExecutionDone { .. } => format!("{}/tool-execution-done", Self::TOPIC_PREFIX),
            ToolEvent::JobDone { .. } => format!("{}/job-done", Self::TOPIC_PREFIX),
        }
    }
}

impl ToolEvent {
    pub const TOPIC_PREFIX: &str = "marie/tools/events";

    pub fn is(msg: &PubSubMessage) -> bool {
        msg.topic.starts_with(Self::TOPIC_PREFIX)
    }
}

