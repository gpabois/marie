use std::{collections::HashMap, sync::Arc};

use futures::StreamExt;
use parking_lot::Mutex;

use crate::{
    layer::Layer,
    worker::{WorkerEvent, WorkerClient},
    rpc::{RemoteProcedureCall, RpcServer},
    sink::SinkBoxExt,
    tools::{
        ToolDefinition, ToolCallId, ToolEvent,
        builtin::register_builtins_tools,
        catalog::{ToolCatalog, ToolId},
        rpc::{ExecuteTool, GetTool, InsertTool, ListTool, RemoveTool, ToolExecutionTracker, UpdateTool},
    },
};

pub struct ToolServerActor;

impl ToolServerActor {
    pub fn new(
        layer: impl Layer<Send = ToolEvent, Received = ToolEvent>,
        worker_layer: impl Layer<Send = WorkerEvent, Received = WorkerEvent>,
        mut rpc: RpcServer,
        worker: WorkerClient
    ) {
        let (tx, rx) = layer.split();
        let tx = tx.boxed_sink();
        let rx = rx.boxed();

        let (_, worker_rx) = worker_layer.split();
        let worker_rx = worker_rx.boxed();

        let ongoings: Arc<Mutex<HashMap<ToolCallId, ToolExecutionTracker>>> = Arc::new(Mutex::new(HashMap::default()));

        ExecuteTool(ongoings, worker).register(&mut rpc);

        let _ = (tx, rx, worker_rx);
    }
}


#[derive(Clone)]
pub struct ToolServer {
    catalog: Arc<Mutex<ToolCatalog>>
}

impl ToolServer {
    pub fn new(mut rpc: RpcServer) -> Self {
        let catalog: Arc<Mutex<ToolCatalog>> = Arc::new(Mutex::new(ToolCatalog::new()));

        GetTool(catalog.clone()).register(&mut rpc);
        ListTool(catalog.clone()).register(&mut rpc);
        InsertTool(catalog.clone()).register(&mut rpc);
        UpdateTool(catalog.clone()).register(&mut rpc);
        RemoveTool(catalog.clone()).register(&mut rpc);

        let server = Self { catalog };
        register_builtins_tools(server.clone());
        server
    }

    /// Insère directement une déclaration dans le catalogue de ce serveur,
    /// sans passer par le réseau (contrairement à [`crate::tools::client::ToolClient::insert`])
    /// — utilisé par [`register_builtins_tools`] pour amorcer les tools
    /// système dès la construction du serveur, avant même qu'un pair distant
    /// ait eu l'occasion de les découvrir via [`crate::tools::rpc::InsertTool`].
    pub fn insert(&self, id: impl Into<ToolId>, tool: ToolDefinition) {
        self.catalog.lock().insert(id.into(), tool);
    }
}
