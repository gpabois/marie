pub mod catalog;
pub mod client;
pub mod server;
pub mod layers;
pub(crate) mod worker;
pub mod builtin;
pub mod rpc;

use std::borrow::Borrow;
use std::fmt::Display;

use bytemuck::{Pod, Zeroable};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use crate::{agent::AgentId, id::ID, job::JobId, network::worker::JobResult, pubsub::PubSubMessage, session::SessionId, tools::client::ToolError};

pub use rpc::{ExecuteTool, GetTool, InsertTool, ListTool, RemoveTool, UpdateTool};

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
pub struct Tool {
    pub name: ToolId,
    pub description: String,
    pub parameters_schema: Value
}

#[derive(Debug, Hash, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct ToolCallId(SessionId, ID);

impl ToolCallId {
    pub fn session_id(&self) -> SessionId {
        self.0
    }
}

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

/// Sur le même modèle que [`crate::agent::AgentId`]'s `Display` — utilisé
/// pour préfixer les sorties de tool réinjectées dans le contexte de
/// l'agent appelant (voir `session::server::report_tool_execution`).
impl Display for ToolCallId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.0, self.1)
    }
}

impl std::str::FromStr for ToolCallId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (session_part, local_part) = s.split_once('/').ok_or_else(|| anyhow::anyhow!("format de ToolCallId invalide : {s}"))?;
        Ok(Self(session_part.parse()?, local_part.parse()?))
    }
}

/// Sérialisé/désérialisé comme une chaîne plutôt que via le `derive` par
/// défaut — même raison que [`crate::agent::AgentId`] : servir de clé de
/// `HashMap` dans une structure sérialisée en JSON (voir
/// `YieldStatus::WaitingToolReply::tools_outputs`).
impl Serialize for ToolCallId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for ToolCallId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let repr = String::deserialize(deserializer)?;
        repr.parse().map_err(serde::de::Error::custom)
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
    pub agent_id: AgentId,
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

