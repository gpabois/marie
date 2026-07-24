use std::{collections::HashMap, sync::Arc};

use parking_lot::Mutex;
use serde::de::DeserializeOwned;

use crate::{di::{Factory, Get, Resolve}, expert::client::ExpertClient, graph::node::{self, Nodable, NodeDefinition, NodeFactory, NodeName}, model::client::ModelClient, network::worker::client::WorkerClient};


#[derive(Clone)]
pub struct GraphServer<D> 
{
    deps: D,
    nodes: Arc<Mutex<HashMap<NodeName, NodeDefinition>>>,
    node_executors: Arc<Mutex<HashMap<NodeName, NodeFactory<D>>>>
}

impl<D> Factory<D> for GraphServer<D> 
    where
        D: Resolve<ExpertClient> + Resolve<ModelClient> + Get<WorkerClient> + Clone + Send + Sync + 'static
{
    fn create(container: &D) -> Self {
        let server = Self {
            deps: container.clone(),
            nodes: Arc::new(Mutex::new(HashMap::default())),
            node_executors: Arc::new(Mutex::new(HashMap::default()))
        };

        node::ExpertNode::register(&server);

        server
    }
}

impl<D> GraphServer<D> {
    pub fn register_node_factory<F, Args, N>(
        &self, 
        name: impl ToString, 
        factory: F, 
        schema: serde_json::Value
    ) where  
        F: Fn(Args) -> N,
        N: Nodable<D>,
        Args: DeserializeOwned {
        let name = name.to_string();
        let factory = Arc::new(|args: serde_json::Value| {
            let args: Args = serde_json::from_value(args).unwrap();
            let node = factory(args);
            let executor = move |ctx, state| {
                let task =  node.clone().execute(ctx, state);
                task
            };

            Arc::new(executor)
        });
        self.node_executors.lock().insert(name, factory);
    }
}