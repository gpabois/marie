use async_trait::async_trait;
use futures::channel::mpsc;
use libp2p::PeerId;
use marie_macros::core_rpc;
use serde_json::Value;
use tokio::sync::oneshot;

#[cfg(feature="rpc-executor")]
use crate::workspace::server::WorkspaceServer;
use crate::{
    rpc::{RemoteProcedureCall, Void}, workspace::{
        Workspace, WorkspaceId, WorkspaceSessionRequest, WorkspaceVarsPatchRequest, WorkspaceVarsQueryRequest, WorkspaceVarsRemoveRequest, server::{WorkspaceCommand, query_vars}, store::{WorkspaceStorable, WorkspaceStoreClient},
    },
};


core_rpc! {
    #[rpc(name="/marie/workspaces/get")]
    pub async fn get_workspace(self: Self<WorkspaceServer>, id: WorkspaceId) -> crate::Result<Option<Workspace>> {
        self.0.get(id).await
    }
}

core_rpc! {
    #[rpc(name="/marie/workspaces/list")]
    pub async fn list_workspace(self: Self<WorkspaceServer>, _: Void) -> crate::Result<Vec<Workspace>> {
        self.0.list().await
    }
}

core_rpc! {
    #[rpc(name="/marie/workspaces/create")]
    pub async fn create_workspace(self: Self<WorkspaceServer>, value: Workspace) -> crate::Result<()> {
        self.0.create(value).await
    } 
}

core_rpc! {
    #[rpc(name="/marie/workspaces/delete")]
    pub async fn delete_workspace(self: Self<WorkspaceServer>, id: WorkspaceId) -> crate::Result<()> {
        self.0.delete(id).await
    } 
}


/// Rattache une session à un workspace *existant* — voir
/// [`WorkspaceSessionRequest`] et [`crate::workspace::server::add_session`]
/// (échoue sur un workspace inconnu plutôt que de le créer silencieusement).
#[derive(Clone)]
pub struct AddSession(pub(crate) mpsc::UnboundedSender<WorkspaceCommand>);

#[async_trait]
impl RemoteProcedureCall for AddSession {
    const NAME: &'static str = "/marie/workspaces/sessions/add";

    type Args = WorkspaceSessionRequest;
    type Return = Result<(), String>;

    async fn execute(self, request: WorkspaceSessionRequest, _: PeerId) -> Result<(), String> {
        let (reply, rx) = oneshot::channel();
        let _ = self.0.unbounded_send(WorkspaceCommand::AddSession {
            workspace_id: request.workspace_id,
            session_id: request.session_id,
            reply,
        });
        rx.await.unwrap_or_else(|_| Err("le serveur de workspaces s'est arrêté".to_string()))
    }
}

/// Détache une session d'un workspace existant — voir
/// [`WorkspaceSessionRequest`] et [`crate::workspace::server::remove_session`].
#[derive(Clone)]
pub struct RemoveSession(pub(crate) mpsc::UnboundedSender<WorkspaceCommand>);

#[async_trait]
impl RemoteProcedureCall for RemoveSession {
    const NAME: &'static str = "/marie/workspaces/sessions/remove";

    type Args = WorkspaceSessionRequest;
    type Return = Result<(), String>;

    async fn execute(self, request: WorkspaceSessionRequest, _: PeerId) -> Result<(), String> {
        let (reply, rx) = oneshot::channel();
        let _ = self.0.unbounded_send(WorkspaceCommand::RemoveSession {
            workspace_id: request.workspace_id,
            session_id: request.session_id,
            reply,
        });
        rx.await.unwrap_or_else(|_| Err("le serveur de workspaces s'est arrêté".to_string()))
    }
}

/// Évalue une expression JSONPath contre [`Workspace::vars`] — voir
/// [`WorkspaceVarsQueryRequest`]. Opération de lecture seule : ne passe pas
/// par [`crate::workspace::server::WorkspaceCommand`], contrairement aux RPC
/// mutantes ci-dessus.
#[derive(Clone)]
pub struct QueryVars(pub(crate) WorkspaceStoreClient);

#[async_trait]
impl RemoteProcedureCall for QueryVars {
    const NAME: &'static str = "/marie/workspaces/vars/query";

    type Args = WorkspaceVarsQueryRequest;
    type Return = Result<Vec<Value>, String>;

    async fn execute(self, request: WorkspaceVarsQueryRequest, _: PeerId) -> Result<Vec<Value>, String> {
        query_vars(self.0, request.workspace_id, &request.path).await.map_err(|e| e.to_string())
    }
}

/// Remplace, dans [`Workspace::vars`], chaque nœud trouvé par une expression
/// JSONPath — voir [`WorkspaceVarsPatchRequest`].
#[derive(Clone)]
pub struct PatchVars(pub(crate) mpsc::UnboundedSender<WorkspaceCommand>);

#[async_trait]
impl RemoteProcedureCall for PatchVars {
    const NAME: &'static str = "/marie/workspaces/vars/patch";

    type Args = WorkspaceVarsPatchRequest;
    type Return = Result<(), String>;

    async fn execute(self, request: WorkspaceVarsPatchRequest, _: PeerId) -> Result<(), String> {
        let (reply, rx) = oneshot::channel();
        let _ = self.0.unbounded_send(WorkspaceCommand::PatchVars {
            workspace_id: request.workspace_id,
            path: request.path,
            value: request.value,
            reply,
        });
        rx.await.unwrap_or_else(|_| Err("le serveur de workspaces s'est arrêté".to_string()))
    }
}

pub struct RemoveVars(pub(crate) mpsc::UnboundedSender<WorkspaceCommand>);

#[async_trait]
impl RemoteProcedureCall for RemoveVars {
    const NAME: &'static str = "/marie/workspaces/vars/remove";

    type Args = WorkspaceVarsRemoveRequest;
    type Return = Result<(), String>;

    async fn execute(self, request: WorkspaceVarsRemoveRequest, _: PeerId) -> Result<(), String> {
        let (reply, rx) = oneshot::channel();
        let _ = self.0.unbounded_send(WorkspaceCommand::RemoveVars {
            workspace_id: request.workspace_id,
            path: request.path,
            reply,
        });
        rx.await.unwrap_or_else(|_| Err("le serveur de workspaces s'est arrêté".to_string()))
    }
}