use std::{borrow::Borrow, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use libp2p::PeerId;
use parking_lot::Mutex;

use crate::{
    job::JobId,
    network::worker::client::WorkerClient,
    rpc::{RemoteProcedureCall, Void},
    tools::{Tool, ToolCall, ToolCallId, ToolId, catalog::ToolCatalog, worker::ToolExecution},
};

/// Suivi d'un appel de tool délégué à un job `JOB_TOOL_EXECUTE` — voir
/// [`ExecuteTool`].
pub struct ToolExecutionTracker {
    pub(crate) job_id: Option<JobId>,
    pub(crate) call: ToolCall,
    pub(crate) expires_at: std::time::Instant,
}

/// Récupère la déclaration d'un tool du catalogue, ou `None` si inconnu de
/// ce nœud — voir [`crate::tools::client::ToolClient::get`].
#[derive(Clone)]
pub struct GetTool(pub(crate) Arc<Mutex<ToolCatalog>>);

#[async_trait]
impl RemoteProcedureCall for GetTool {
    const NAME: &'static str = "marie/tools/get";

    type Args = ToolId;
    type Return = Option<Tool>;

    async fn execute(self, id: ToolId, _: PeerId) -> Option<Tool> {
        self.0.lock().get(id.borrow())
    }
}

/// Liste tout le catalogue de tools connu de ce nœud.
#[derive(Clone)]
pub struct ListTool(pub(crate) Arc<Mutex<ToolCatalog>>);

#[async_trait]
impl RemoteProcedureCall for ListTool {
    const NAME: &'static str = "marie/tools/list";

    type Args = Void;
    type Return = Vec<Tool>;

    async fn execute(self, _: Void, _: PeerId) -> Vec<Tool> {
        self.0.lock().list()
    }
}

/// Crée un tool dans le catalogue.
#[derive(Clone)]
pub struct InsertTool(pub(crate) Arc<Mutex<ToolCatalog>>);

#[async_trait]
impl RemoteProcedureCall for InsertTool {
    const NAME: &'static str = "marie/tools/insert";

    type Args = (ToolId, Tool);
    type Return = Void;

    async fn execute(self, (id, tool): (ToolId, Tool), _: PeerId) -> Void {
        self.0.lock().insert(id, tool);
        Void
    }
}

/// Met à jour la déclaration d'un tool existant.
#[derive(Clone)]
pub struct UpdateTool(pub(crate) Arc<Mutex<ToolCatalog>>);

#[async_trait]
impl RemoteProcedureCall for UpdateTool {
    const NAME: &'static str = "marie/tools/update";

    type Args = (ToolId, Tool);
    type Return = Void;

    async fn execute(self, (id, tool): (ToolId, Tool), _: PeerId) -> Void {
        self.0.lock().insert(id, tool);
        Void
    }
}

/// Retire un tool du catalogue.
#[derive(Clone)]
pub struct RemoveTool(pub(crate) Arc<Mutex<ToolCatalog>>);

#[async_trait]
impl RemoteProcedureCall for RemoveTool {
    const NAME: &'static str = "marie/tools/remove";

    type Args = ToolId;
    type Return = Void;

    async fn execute(self, id: ToolId, _: PeerId) -> Void {
        self.0.lock().remove(id.borrow());
        Void
    }
}

/// Délègue l'exécution d'un tool à un job `JOB_TOOL_EXECUTE`, suivi dans
/// `ongoings` jusqu'à son achèvement (rapporté via
/// [`crate::tools::ToolEvent::ToolExecutionDone`], voir
/// `ToolServerActor`).
#[derive(Clone)]
pub struct ExecuteTool(pub(crate) Arc<Mutex<HashMap<ToolCallId, ToolExecutionTracker>>>, pub(crate) WorkerClient);

#[async_trait]
impl RemoteProcedureCall for ExecuteTool {
    const NAME: &'static str = "marie/tools/execute";

    type Args = ToolCall;
    type Return = Result<(), String>;

    async fn execute(self, call: ToolCall, _: PeerId) -> Result<(), String> {
        let ttl = std::time::Duration::from_mins(5);

        let job_id = self.1.spawn::<ToolExecution>(call.clone(), Some(ttl)).await.unwrap();

        let mut guard = self.0.lock();
        guard.insert(call.id, ToolExecutionTracker { job_id: Some(job_id), call, expires_at: std::time::Instant::now() + ttl });

        Ok(())
    }
}
