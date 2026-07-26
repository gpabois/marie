
use std::sync::Arc;

use async_trait::async_trait;
use futures::future::BoxFuture;
use json_patch::Patch;
use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::{
    bail, di::{Get, Resolve}, expert::{ExpertClient, ExpertId}, graph::{Goto, GraphFrameId, Halt, server::GraphServer}, id::ID, model::ModelClient, state::{State, StateTransaction}, worker::client::WorkerClient
};

pub type NodeName = String;

pub type NodeExecutor<D>  = Arc<dyn Fn(NodeContext<D>, State) -> BoxFuture<'static, crate::Result<NodeOutcome>>>;
pub type NodeFactory<D> = Arc<dyn Fn(Value) -> NodeExecutor<D>>; 

pub struct NodeDefinition {
    name: NodeName,
    schema: serde_json::Value
}

pub type NodeId = ID;

#[derive(Clone)]
pub struct NodeContext<D> {
    pub graph_id: GraphFrameId,
    pub thread_id: ID,
    pub node_id: NodeId,
    pub step: u32,
    pub deps: D
}


/// Ce qu'une node décide après son exécution.
pub enum NodeOutcome {
    /// Continue vers la node suivante déterminée par l'edge enregistrée
    /// pour la node courante (Direct, Conditional ou Fanout).
    Continue(Vec<Patch>),
    Command {
        state: State,
        goto: Goto
    },
    /// Termine l'exécution du graphe immédiatement avec cet état final.
    Halt(Halt),
}

#[async_trait]
pub trait Nodable<D>: Sized + Clone + Send + Sync + 'static where Self: From<Self::Parameters> {
    const NAME: &str;
    type Parameters: DeserializeOwned + JsonSchema;

    fn register(server: &GraphServer<D>) {
        let schema = schema_for!(<Self as Nodable<D>>::Parameters);

        server.register_node_factory(
            Self::NAME, 
            |args| Self::from(args), 
            serde_json::to_value(schema).unwrap()
        );
    }

    async fn execute(
        self, 
        ctx: NodeContext<D>, 
        state: StateTransaction
    ) -> crate::Result<NodeOutcome>;
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ExpertNode(pub ExpertId);

impl From<ExpertId> for ExpertNode {
    fn from(value: ExpertId) -> Self {
        Self(value)
    }
}

#[async_trait]
impl<D> Nodable<D> for ExpertNode where D: Resolve<ExpertClient> + Get<WorkerClient> + Resolve<ModelClient>
{
    type Parameters = ExpertId;
    const NAME: &str = "expert";

    async fn execute(self, ctx: NodeContext<D>, state: StateTransaction) -> crate::Result<NodeOutcome>  {
        let experts: ExpertClient = ctx.deps.resolve();
        let worker: WorkerClient = ctx.deps.get();
        let models: ModelClient = ctx.deps.resolve();

        let expert = experts.get(self.0).await?.ok_or_else(|| bail!("missing expert {}", self.0))?;
        let model = models.get(expert.model_id).await?;
        

        Ok(NodeOutcome::Continue(state.into()))
    }
}