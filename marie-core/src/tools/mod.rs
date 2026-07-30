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
use crate::{agent::AgentFrameId, catalog::{Catalog, CatalogError, CatalogItemRef, Catalogable}, id::ID, job::JobId, events::EventEnvelope, tools::client::ToolError};

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

impl Catalogable for ToolDefinition {
    const KIND: &str = "/marie/catalog/tools";

    fn id(&self) -> &str {
        self.name.borrow()
    }
}

/// Référence immuable vers une version publiée d'un [`ToolDefinition`] —
/// même rôle que [`crate::model::ModelRef`]/[`crate::graph::GraphRef`].
#[derive(Clone)]
pub struct ToolRef(CatalogItemRef);

/// Catalogue des tools connus du cluster — sur le même principe que
/// [`crate::model::Models`]/[`crate::expert::Experts`]/[`crate::graph::Graphs`] :
/// un [`Catalog`] Postgres versionné, qui remplace l'ancien
/// [`ToolCatalog`](crate::tools::catalog::ToolCatalog) (état CRDT `loro`
/// fusionné entre control planes) maintenant qu'un [`ToolDefinition`] ne
/// porte aucun secret et n'a pas besoin d'être écrit en continu.
#[derive(Clone)]
pub struct Tools {
    catalog: Catalog
}

impl Tools {
    pub fn new(catalog: Catalog) -> Self {
        Self { catalog }
    }

    /// Publie la première version d'un tool.
    pub async fn insert(&self, tool: ToolDefinition) -> Result<ToolRef, CatalogError> {
        Ok(ToolRef(self.catalog.publish(tool).await?))
    }

    /// Publie une nouvelle version d'un tool déjà connu — contrairement à
    /// [`Self::insert`], échoue avec [`CatalogError::NotFound`] si
    /// `tool.name` n'a encore aucune version active.
    pub async fn replace(&self, tool: ToolDefinition) -> Result<ToolRef, CatalogError> {
        self.catalog.latest_ref::<ToolDefinition>(tool.name.borrow()).await?;
        Ok(ToolRef(self.catalog.publish(tool).await?))
    }

    /// Soft-delete (voir [`Catalog::delete`]) la version active de `id`.
    pub async fn delete(&self, id: &ToolId) -> Result<(), CatalogError> {
        let r = self.catalog.latest_ref::<ToolDefinition>(id.borrow()).await?;
        self.catalog.delete(&r).await
    }

    /// Référence de la version active la plus récente de `id`, sans lire son
    /// contenu — voir [`Self::get`] pour la définition désérialisée.
    pub async fn latest(&self, id: &ToolId) -> Result<Option<ToolRef>, CatalogError> {
        match self.catalog.latest_ref::<ToolDefinition>(id.borrow()).await {
            Ok(r) => Ok(Some(ToolRef(r))),
            Err(CatalogError::NotFound { .. }) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Version active la plus récente de `id`, désérialisée.
    pub async fn get(&self, id: &ToolId) -> Result<Option<ToolDefinition>, CatalogError> {
        match self.catalog.latest::<ToolDefinition>(id.borrow()).await {
            Ok(item) => Ok(Some((*item).clone())),
            Err(CatalogError::NotFound { .. }) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub async fn list(&self) -> Result<Vec<ToolDefinition>, CatalogError> {
        let refs = self.catalog.list::<ToolDefinition>().await?;
        let mut tools = Vec::with_capacity(refs.len());
        for r in refs {
            let item = self.catalog.deref::<ToolDefinition>(&r).await?;
            tools.push((*item).clone());
        }
        Ok(tools)
    }
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

pub struct ToolFrame {
    pub id: ToolCallId,
    pub name: ToolName,
    pub parameters: Value
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequestToolCall {
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

