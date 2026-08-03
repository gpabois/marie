pub mod catalog;
#[cfg(feature = "tool-executor")]
pub mod executor;

use std::sync::Arc;
use std::{borrow::Borrow, collections::HashMap};
use std::fmt::Display;

use async_trait::async_trait;
use bytemuck::{Pod, Zeroable};
use parking_lot::Mutex;
use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use crate::di::{Constructible, Resolve};
use crate::events::Event;
#[cfg(feature = "tool-executor")]
use crate::session::SessionId;
use crate::{catalog::{Catalog, CatalogError, CatalogItemRef, Catalogable}, id::ID, job::JobId, events::EventEnvelope};

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
pub enum ToolExecution {
    /// Search for registered tools
    Native,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Tool {
    pub name: ToolId,
    pub description: String,
    pub parameters_schema: Value,
    pub execution: ToolExecution
}

impl Catalogable for Tool {
    const KIND: &str = "/marie/catalog/tools";

    fn id(&self) -> &str {
        self.name.borrow()
    }
}

/// Référence immuable vers un [`Tool`] — même rôle que
/// [`crate::model::ModelRef`]/[`crate::graph::GraphRef`], à ceci près qu'un
/// tool a deux origines possibles : soit une version publiée au catalogue
/// ([`Self::Catalog`], résolue via [`Catalog::deref`]), soit un tool natif
/// enregistré en mémoire par le process via [`Tools::register`]
/// ([`Self::Native`], jamais écrit au catalogue puisqu'il n'a ni contenu
/// versionné ni existence en dehors du binaire qui l'a enregistré).
#[derive(Clone)]
pub enum ToolRef {
    Catalog(CatalogItemRef),
    Native(ToolId),
}

/// Catalogue des tools connus du cluster — sur le même principe que
/// [`crate::model::Models`]/[`crate::expert::Experts`]/[`crate::graph::Graphs`] :
/// un [`Catalog`] Postgres versionné, qui remplace l'ancien
/// [`ToolCatalog`](crate::tools::catalog::ToolCatalog) (état CRDT `loro`
/// fusionné entre control planes) maintenant qu'un [`ToolDefinition`] ne
/// porte aucun secret et n'a pas besoin d'être écrit en continu.
#[derive(Clone)]
pub struct Tools {
    catalog: Catalog,
    #[cfg(feature = "tool-executor")]
    pub(crate) executors: executor::ToolExecutors,
    pub(crate) native_tools: Arc<Mutex<HashMap<ToolId, Tool>>>
}

impl<C> Constructible<C> for Tools where C: Resolve<Catalog> {
    fn construct(container: &C, args: ()) -> Self {
        Self::new(container.resolve(()))
    }
}

impl Tools {
    #[cfg(not(feature = "tool-executor"))]
    pub fn new(catalog: Catalog) -> Self {
        Self { catalog, native_tools: Arc::new(Mutex::new(HashMap::default())) }
    }

    #[cfg(feature = "tool-executor")]
    pub fn new(catalog: Catalog) -> Self {
        Self { 
            catalog, 
            executors: executor::ToolExecutors::default(),
            native_tools: Arc::new(Mutex::new(HashMap::default())) 
        }
    }

    #[cfg(feature = "tool-executor")]
    pub fn register<F, Args, R, Fut>(&self, def: Tool, executor: F) 
        where
            F: Fn(SessionId, Args) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = Result<R, crate::Error>> + Send + 'static,
            R: Serialize,
            Args: DeserializeOwned
    {
        let name = def.name.clone();
        self.native_tools.lock().insert(def.name.clone(), def);
        self.executors.add(name, executor)
    }

    #[cfg(not(feature = "tool-executor"))]
    pub fn register(&self, def: Tool) {
        self.native_tools.lock().insert(def.name.clone(), def);
    }

    /// Exécute le tool natif désigné par `id` — voir [`Self::register`].
    /// Sur le même principe que [`crate::graph::Graphs::execute`].
    #[cfg(feature = "tool-executor")]
    pub async fn execute(&self, session_id: SessionId, id: &ToolId, args: Value) -> crate::Result<Value> {
        let executor = self.executors.get(id)
            .ok_or_else(|| crate::err!("aucun exécuteur enregistré pour le tool {:?}", id))?;

        executor(session_id, args).await
    }


    /// Publie la première version d'un tool.
    pub async fn insert(&self, tool: Tool) -> Result<ToolRef, CatalogError> {
        Ok(ToolRef::Catalog(self.catalog.publish(tool).await?))
    }

    /// Publie une nouvelle version d'un tool déjà connu — contrairement à
    /// [`Self::insert`], échoue avec [`CatalogError::NotFound`] si
    /// `tool.name` n'a encore aucune version active.
    pub async fn replace(&self, tool: Tool) -> Result<ToolRef, CatalogError> {
        self.catalog.latest_ref::<Tool>(tool.name.borrow()).await?;
        Ok(ToolRef::Catalog(self.catalog.publish(tool).await?))
    }

    /// Soft-delete (voir [`Catalog::delete`]) la version active de `id`.
    pub async fn delete(&self, id: &ToolId) -> Result<(), CatalogError> {
        let r = self.catalog.latest_ref::<Tool>(id.borrow()).await?;
        self.catalog.delete(&r).await
    }

    /// Référence de la version active la plus récente de `id`, sans lire son
    /// contenu — voir [`Self::get`] pour la définition désérialisée. Un tool
    /// natif enregistré via [`Self::register`] est prioritaire sur le
    /// catalogue : il n'a pas de version publiée à résoudre, donc rien à
    /// gagner à interroger le catalogue pour un `id` qu'on sait déjà natif.
    pub async fn latest(&self, id: &ToolId) -> Result<Option<ToolRef>, CatalogError> {
        if self.native_tools.lock().contains_key(id) {
            return Ok(Some(ToolRef::Native(id.clone())));
        }

        match self.catalog.latest_ref::<Tool>(id.borrow()).await {
            Ok(r) => Ok(Some(ToolRef::Catalog(r))),
            Err(CatalogError::NotFound { .. }) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Version active la plus récente de `id`, désérialisée — pour un tool
    /// natif (voir [`Self::latest`]), c'est directement la définition fixée
    /// à l'enregistrement ([`Self::register`]), qui n'existe qu'en mémoire.
    pub async fn get(&self, id: &ToolId) -> Result<Option<Tool>, CatalogError> {
        if let Some(tool) = self.native_tools.lock().get(id) {
            return Ok(Some(tool.clone()));
        }

        match self.catalog.latest::<Tool>(id.borrow()).await {
            Ok(item) => Ok(Some((*item).clone())),
            Err(CatalogError::NotFound { .. }) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Résout une référence déjà obtenue (voir [`Self::latest`]) vers sa
    /// définition — [`ToolRef::Native`] n'a rien à relire (voir
    /// [`Self::get`]), seul [`ToolRef::Catalog`] retourne au catalogue.
    pub async fn deref(&self, r: &ToolRef) -> Result<Option<Tool>, CatalogError> {
        match r {
            ToolRef::Native(id) => Ok(self.native_tools.lock().get(id).cloned()),
            ToolRef::Catalog(r) => match self.catalog.deref::<Tool>(r).await {
                Ok(item) => Ok(Some((*item).clone())),
                Err(CatalogError::NotFound { .. }) => Ok(None),
                Err(err) => Err(err),
            },
        }
    }

    /// Tous les tools connus : natifs (enregistrés en mémoire via
    /// [`Self::register`]) puis catalogués.
    pub async fn list(&self) -> Result<Vec<Tool>, CatalogError> {
        let mut tools: Vec<Tool> = self.native_tools.lock().values().cloned().collect();

        let refs = self.catalog.list::<Tool>().await?;
        for r in refs {
            let item = self.catalog.deref::<Tool>(&r).await?;
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

    fn definition() -> Tool {
        Tool {
            name: ToolId::from(Self::NAME),
            description: Self::DESCRIPTION.to_string(),
            parameters_schema: Self::parameters_schema(),
            execution: ToolExecution::Native
        }
    }

    #[cfg(feature = "tool-executor")]
    async fn execute(self, args: Self::Args) -> crate::Result<Self::Return>;

    #[cfg(not(feature = "tool-executor"))]
    fn register(self, tools: &mut Tools) where Self: Clone + Send + Sync + 'static {
        tools.register(Self::definition());
    }

    #[cfg(feature = "tool-executor")]
    fn register(self, tools: &mut Tools) where Self: Clone + Send + Sync + 'static {
        let executor = move |session_id, args| {
            self.clone().execute(args)
        };

        tools.register(Self::definition(), executor);
    }
}

#[derive(Debug, Hash, Clone, Copy, PartialEq, Eq, Pod, Zeroable, Serialize, Deserialize)]
#[repr(C)]
pub struct ToolCallId(ID);

impl From<ID> for ToolCallId {
    fn from(value: ID) -> Self {
        Self(value)
    }
}

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
    pub name: ToolName,
    pub parameters: Value
}

/// Ne porte volontairement aucun identifiant de corrélation : voir la doc de
/// [`crate::expert::AskExpert`] — même raisonnement (déterminisme du
/// rejeu), le [`ToolCallId`] du frame enfant est généré côté
/// `SessionHandler` (voir `SessionHandler::append_tool_call`), jamais fourni
/// par l'appelant.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequestToolCall {
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

impl Event for ToolEvent {
    const TOPIC: &str = "/marie/tools";

    fn id(&self) -> String {
        match self {
            ToolEvent::ToolExecutionDone { id, .. } => id.to_string(),
        }
    }

    fn topics(&self) -> Vec<String> {
        vec![<Self as Event>::TOPIC.to_string()]
    }
}

