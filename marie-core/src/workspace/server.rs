use crate::{err, layer::BoxLayer, sink::BoxSink};
use futures::{SinkExt as _, StreamExt as _, channel::mpsc::{self, UnboundedSender}};
use libp2p::rendezvous::Namespace;
use serde_json::Value;
use tokio::{select, sync::oneshot};
use typed_builder::TypedBuilder;

use crate::{
    layer::Layer,
    network::bootstrap::BootstrapClient,
    rpc::{RemoteProcedureCall, RpcServer},
    session::SessionId,
    sink::SinkBoxExt as _,
    workspace::{
        NS_WORKSPACE, Workspace, WorkspaceEvent, WorkspaceId,
        rpc::{AddSession, GetWorkspace, InsertWorkspace, ListWorkspace, PatchVars, QueryVars, RemoveSession, RemoveWorkspace},
        store::{WorkspaceStorable, WorkspaceStore},
    },
};

#[derive(TypedBuilder)]
pub struct WorkspaceServerArgs
{
    rpc_server: RpcServer,
    bootstrap: BootstrapClient,
    store: WorkspaceStore,
    #[builder(setter(
        fn transform<L>(layer: L) -> BoxLayer<WorkspaceEvent, WorkspaceEvent> 
            where L: Layer<Send = WorkspaceEvent, Received = WorkspaceEvent>
        {
            let (tx, rx) = layer.split();
            BoxLayer::new(tx, rx)
        }
    ))]
    events_layer:  BoxLayer<WorkspaceEvent, WorkspaceEvent>
}

/// Commandes mutant l'état d'un workspace (persisté via
/// [`WorkspaceStoreClient`]), consommées exclusivement par
/// [`WorkspaceServerActor`] — même indirection RPC -> Command ->
/// mutation + évènement que [`crate::session::server::SessionCommand`] (et
/// pas la mutation directe legacy de `ModelServer`) : c'est elle qui
/// garantit que chaque mutation réussie émet exactement le
/// [`WorkspaceEvent`] correspondant.
pub(crate) enum WorkspaceCommand {
    Insert { workspace: Workspace, reply: oneshot::Sender<crate::Result<()>> },
    Remove { id: WorkspaceId, reply: oneshot::Sender<crate::Result<()>> },
    AddSession { workspace_id: WorkspaceId, session_id: SessionId, reply: oneshot::Sender<Result<(), String>> },
    RemoveSession { workspace_id: WorkspaceId, session_id: SessionId, reply: oneshot::Sender<Result<(), String>> },
    PatchVars { workspace_id: WorkspaceId, path: String, value: Value, reply: oneshot::Sender<Result<(), String>> },
    RemoveVars {workspace_id: WorkspaceId, path: String, reply: oneshot::Sender<Result<(), String>> }
}

type WorkspaceServerEventEmitter = UnboundedSender<WorkspaceEvent>;

#[derive(Clone)]
pub struct WorkspaceServer {
    pub(crate) store: WorkspaceStore,
    pub(crate) event_tx: mspc::UnboundedSender<WorkspaceEvent>
}

impl WorkspaceServer {
    pub fn new<L>(args: WorkspaceServerArgs) -> Self 
        where L: Layer<Send = WorkspaceEvent, Received = WorkspaceEvent> 
    {
        args.bootstrap.register_to_namespaces([Namespace::from_static(NS_WORKSPACE)]);

        let (tx, rx) = args.events_layer.split();
        let (event_tx, mut event_rx) = mpsc::unbounded::<WorkspaceEvent>();

        {
            let store = args.store.clone();
            tokio::spawn(async move {
                loop {
                    select! {
                        Ok(event_to_send) = event_rx.recv() => {
                            let _ = tx.send(event_to_send).await;
                        }
                    }
                }
            });
        }

        let workspaces = Self { store: args.store, event_tx };
        {
            GetWorkspace(store.clone()).register(&mut args.rpc_server);
            ListWorkspace(store.clone()).register(&mut args.rpc_server);
            QueryVars(store.clone()).register(&mut args.rpc_server);

            InsertWorkspace(cmd_tx.clone()).register(&mut args.rpc_server);
            RemoveWorkspace(cmd_tx.clone()).register(&mut args.rpc_server);
            AddSession(cmd_tx.clone()).register(&mut args.rpc_server);
            RemoveSession(cmd_tx.clone()).register(&mut args.rpc_server);
            PatchVars(cmd_tx.clone()).register(&mut args.rpc_server);
        }

        workspaces
    }

    pub fn emit(&self, event: WorkspaceEvent) {
        self.event_tx.send(event);
    }

    pub async fn get(&self, id: WorkspaceId) -> crate::Result<Option<Workspace>> {
        self.store.get(id).await
    }

    pub async fn list(&self) -> crate::Result<Vec<Workspace>> {
        self.store.list().await
    }

    pub async fn create(&self, workspace: Workspace) -> crate::Result<()> {
        let id = workspace.id;
        self.store.insert(workspace).await?;
        self.emit(WorkspaceEvent::Created { id });
        Ok(())
    }

    pub async fn replace(&self, workspace: Workspace) -> crate::Result<()> {
        let id = workspace.id;
        self.store.replace(workspace).await?;
        self.emit(WorkspaceEvent::Replaced { id });
        Ok(())
    }

    pub async fn delete(&self, id: WorkspaceId) -> crate::Result<()> {
        self.store.delete(id).await?;
        self.emit(WorkspaceEvent::Removed { id });
        Ok(())
    }
}


/// Récupère `workspace_id` dans le store, ou une erreur lisible s'il n'est
/// pas (encore) connu de ce nœud — commun aux opérations ci-dessous, qui
/// mutent un workspace existant plutôt que d'en créer un : la création est
/// un acte de cycle de vie explicite (voir
/// [`crate::workspace::rpc::InsertWorkspace`]), une création silencieuse ici
/// ressusciterait un workspace supprimé sur un RPC tardif.
pub(crate) async fn get_workspace(store: WorkspaceStoreClient, workspace_id: WorkspaceId) -> Result<Workspace, crate::Error> {
    store
        .clone()
        .get(workspace_id)
        .await?
        .ok_or_else(|| err!("workspace inconnu : {workspace_id}"))
}

/// Rattache `session_id` au workspace `workspace_id` — voir
/// [`Workspace::add_session`] (idempotent).
pub(crate) async fn add_session(
    store: WorkspaceStoreClient,
    workspace_id: WorkspaceId,
    session_id: SessionId,
) -> Result<(), crate::Error> {
    let mut workspace = get_workspace(store.clone(), workspace_id).await?;
    workspace.add_session(session_id);
    store.replace(workspace).await?;
    Ok(())
}

/// Détache `session_id` du workspace `workspace_id` — voir
/// [`Workspace::remove_session`] (idempotent).
pub(crate) async fn remove_session(
    store: WorkspaceStoreClient,
    workspace_id: WorkspaceId,
    session_id: SessionId,
) -> Result<(), crate::Error> {
    let mut workspace = get_workspace(store.clone(), workspace_id).await?;
    workspace.remove_session(&session_id);
    store.replace(workspace).await?;
    Ok(())
}

/// Évalue `path` (JSONPath) contre [`Workspace::vars`], traité comme un
/// unique document JSON (voir [`crate::workspace::WorkspaceVarsQueryRequest`])
/// — même mécanique que `session::server::query_vars`.
pub(crate) async fn query_vars(
    store: WorkspaceStoreClient,
    workspace_id: WorkspaceId,
    path: &str,
) -> Result<Vec<Value>, crate::Error> {
    let workspace = get_workspace(store, workspace_id).await?;
    let doc = serde_json::to_value(&workspace.vars)?;
    let matches = jsonpath_lib::select(&doc, path)?;
    Ok(matches.into_iter().cloned().collect())
}

/// Remplace, dans [`Workspace::vars`] traité comme un unique document JSON,
/// chaque nœud correspondant à `path` par `value` (voir
/// [`crate::workspace::WorkspaceVarsPatchRequest`]) — même mécanique que
/// `session::server::patch_vars`.
pub(crate) async fn patch_vars(
    store: WorkspaceStoreClient,
    workspace_id: WorkspaceId,
    path: &str,
    value: Value,
) -> Result<(), crate::Error> {
    let mut workspace = get_workspace(store.clone(), workspace_id).await?;
    let doc = serde_json::to_value(&workspace.vars)?;
    let patched = jsonpath_lib::replace_with(doc, path, &mut |_| Some(value.clone()))?;
    workspace.vars = serde_json::from_value(patched)?;

    store.replace(workspace).await?;
    Ok(())
}

pub(crate) async fn remove_vars(
    store: WorkspaceStoreClient,
    workspace_id: WorkspaceId,
    path: &str,
) -> crate::Result<()> {
    let mut workspace = get_workspace(store.clone(), workspace_id).await?;
    let doc = serde_json::to_value(&workspace.vars)?;
    let patched = jsonpath_lib::delete(doc, path)?;
    workspace.vars = serde_json::from_value(patched)?;
    store.replace(workspace).await?;
    Ok(())
}

#[cfg(test)]
mod tests {}
