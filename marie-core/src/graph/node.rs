
use std::sync::Arc;

use async_trait::async_trait;
use futures::future::BoxFuture;
use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::{
    di::{Get, Resolve},
     expert::{ExpertId, client::ExpertClient}, 
     graph::{GraphInstanceId, server::GraphServer}, 
     id::ID, 
     network::worker::client::WorkerClient
};

pub type NodeName = String;

pub type NodeExecutor<S, D>  = Arc<dyn Fn(NodeContext<D>, S) -> BoxFuture<'static, anyhow::Result<NodeOutcome<S>>>>;
pub type NodeFactory<S, D> = Arc<dyn Fn(Value) -> NodeExecutor<S, D>>; 

pub struct NodeDefinition {
    name: NodeName,
    schema: serde_json::Value
}

pub type NodeId = ID;

#[derive(Clone)]
pub struct NodeContext<D> {
    pub graph_id: GraphInstanceId,
    pub thread_id: ID,
    pub node_id: NodeId,
    pub step: u32,
    pub deps: D
}

/// Ce qu'une node décide après son exécution.
pub enum NodeOutcome<S> {
    /// Continue vers la node suivante déterminée par l'edge enregistrée
    /// pour la node courante (Direct, Conditional ou Fanout).
    Continue(S),
    /// Court-circuite la table d'edges et saute explicitement vers une node
    /// donnée. Utile pour une node "routeur" qui encode elle-même sa logique
    /// de décision plutôt que de la déléguer à une closure `conditional_edge`.
    Goto(S, NodeId),
    /// Termine l'exécution du graphe immédiatement avec cet état final.
    Halt(S),
}

#[async_trait]
pub trait Nodable<S, D>: Sized + Clone + Send + Sync + 'static where Self: From<Self::Parameters> {
    const NAME: &str;
    type Parameters: DeserializeOwned + JsonSchema;

    fn register(server: &GraphServer<S, D>) {
        let schema = schema_for!(<Self as Nodable<S,D>>::Parameters);

        server.register_node_factory(
            Self::NAME, 
            |args| Self::from(args), 
            serde_json::to_value(schema).unwrap()
        );
    }

    async fn execute(
        self, 
        ctx: NodeContext<D>, 
        state: S
    ) -> anyhow::Result<NodeOutcome<S>>;
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ExpertNode(pub ExpertId);

impl From<ExpertId> for ExpertNode {
    fn from(value: ExpertId) -> Self {
        Self(value)
    }
}

#[async_trait]
impl<S,D> Nodable<S, D> for ExpertNode where D: Resolve<ExpertClient> + Get<WorkerClient>  
{
    type Parameters = ExpertId;
    const NAME: &str = "expert";

    async fn execute(self, ctx: NodeContext<D> , state: S) -> anyhow::Result<NodeOutcome<S> >  {
        let experts: ExpertClient = ctx.deps.resolve();
        let worker: WorkerClient = ctx.deps.get();

        let expert = experts.get(self.0).await?;

        Ok(NodeOutcome::Continue(state))
    }
}