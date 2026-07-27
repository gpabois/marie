pub mod catalog;
pub mod client;
#[cfg(feature = "catalog")]
pub mod server;
#[cfg(feature = "catalog")]
pub mod layers;
pub(crate) mod worker;
pub mod builtin;
pub mod rpc;

use std::borrow::Borrow;
use std::fmt::Display;

use async_trait::async_trait;
use bytemuck::{Pod, Zeroable};
use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use crate::{agent::AgentFrameId, id::ID, job::JobId, worker::{JobResult, server::WorkerServer}, events::EventEnvelope, tools::client::ToolError};

pub use rpc::{ExecuteTool, GetTool, InsertTool, ListTool, RemoveTool, UpdateTool};
pub use marie_macros::core_tool;

pub const JOB_TOOL_EXECUTE: &str = "marie/jobs/tools/execute";
pub const NS_TOOL: &str = "marie/ns/tools";


pub type ToolName = String;

#[derive(Debug, Hash, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

impl Borrow<str> for ToolId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolDefinition {
    pub name: ToolId,
    pub description: String,
    pub parameters_schema: Value
}

#[async_trait]
pub trait Toolable: Clone + Sized + 'static {
    const NAME: &str;
    const DESCRIPTION: &str;

    type Args: Serialize + DeserializeOwned + JsonSchema;
    type Return: Serialize + DeserializeOwned;

    fn parameters_schema() -> Value {
        let schema = schema_for!(Self::Args);
        serde_json::to_value(schema).unwrap()
    }

    fn definition() -> ToolDefinition {
        ToolDefinition {
            name: ToolId::from(Self::NAME),
            description: Self::DESCRIPTION.to_string(),
            parameters_schema: Self::parameters_schema()
        }
    }

    #[cfg(feature = "tool-executor")]
    async fn execute(self, args: Self::Args) -> crate::Result<Self::Return>;

    #[cfg(feature = "tool-executor")]
    fn register_executor(self, worker: &mut WorkerServer) where Self: Clone + Send + Sync + 'static {
        let executor = move |args| {
            self.clone().execute(args)
        };

        worker.register_job_executor(Self::NAME, executor);
    }
}

#[derive(Debug, Hash, Clone, Copy, PartialEq, Eq, Pod, Zeroable, Serialize, Deserialize)]
#[repr(C)]
pub struct ToolCallId(ID);

impl AsRef<[u8]> for ToolCallId {
    fn as_ref(&self) -> &[u8] {
        bytemuck::bytes_of(self)
    }
}

impl ToolCallId {
    pub fn new(id: ID) -> Self {
        Self(id)
    }
}

/// Sur le même modèle que [`crate::agent::AgentId`]'s `Display` — utilisé
/// pour préfixer les sorties de tool réinjectées dans le contexte de
/// l'agent appelant (voir `session::server::report_tool_execution`).
impl Display for ToolCallId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: ToolCallId,
    /// Agent à l'origine de l'appel — `ToolCallId` ne porte que le
    /// `SessionId` (routage du catalogue de tools), pas l'agent précis dans
    /// cette session ; nécessaire pour que
    /// `session::server::report_tool_execution` sache dans quel frame
    /// réinjecter le résultat.
    pub agent_id: AgentFrameId,
    pub name: ToolName,
    pub parameters: Value
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelToolCall {
    pub id: ToolCallId,
    pub name: ToolName,
    pub parameters: Value
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

impl TryFrom<EventEnvelope> for ToolEvent {
    type Error = ToolError;

    fn try_from(value: EventEnvelope) -> Result<Self, Self::Error> {
        use ToolError::NotToolEvent;

        if !Self::is(&value) { return Err(NotToolEvent) };

        serde_json::from_slice(&value.payload).map_err(|_| NotToolEvent)
    }
}

impl From<ToolEvent> for EventEnvelope {
    fn from(value: ToolEvent) -> Self {
        EventEnvelope { 
            id: String::default(), 
            topic: value.topic(), 
            payload: serde_json::to_vec(&value).unwrap(), 
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

    pub fn is(msg: &EventEnvelope) -> bool {
        msg.topic.starts_with(Self::TOPIC_PREFIX)
    }
}

