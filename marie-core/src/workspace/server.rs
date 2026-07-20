use anyhow::anyhow;
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
        store::{WorkspaceStore, WorkspaceStoreClient},
    },
};

#[derive(TypedBuilder)]
pub struct WorkspaceServerArgs {
    rpc_server: RpcServer,
    bootstrap: BootstrapClient,
    store: WorkspaceStoreClient,
}

/// Commandes mutant l'état d'un workspace (persisté via
/// [`WorkspaceStoreClient`]), consommées exclusivement par
/// [`WorkspaceServerActor`] — même indirection RPC -> Command ->
/// mutation + évènement que [`crate::session::server::SessionCommand`] (et
/// pas la mutation directe legacy de `ModelServer`) : c'est elle qui
/// garantit que chaque mutation réussie émet exactement le
/// [`WorkspaceEvent`] correspondant.
pub(crate) enum WorkspaceCommand {
    Insert { workspace: Workspace, reply: oneshot::Sender<anyhow::Result<()>> },
    Remove { id: WorkspaceId, reply: oneshot::Sender<anyhow::Result<()>> },
    AddSession { workspace_id: WorkspaceId, session_id: SessionId, reply: oneshot::Sender<Result<(), String>> },
    RemoveSession { workspace_id: WorkspaceId, session_id: SessionId, reply: oneshot::Sender<Result<(), String>> },
    PatchVars { workspace_id: WorkspaceId, path: String, value: Value, reply: oneshot::Sender<Result<(), String>> },
}

type WorkspaceServerEventEmitter = UnboundedSender<WorkspaceEvent>;

pub struct WorkspaceServerActor;

impl WorkspaceServerActor {
    /// Démarre l'acteur : une tâche unique traite en série les
    /// [`WorkspaceCommand`] reçues (mutation via [`WorkspaceStoreClient`] +
    /// émission de [`WorkspaceEvent`] sur succès, chacune déportée dans son
    /// propre `tokio::spawn` pour ne pas bloquer la réception des commandes
    /// suivantes), pendant que les RPC de lecture
    /// (`GetWorkspace`/`ListWorkspace`/`QueryVars`) accèdent directement au
    /// même [`WorkspaceStoreClient`], partagé (cheap à cloner) — inutile de
    /// les faire transiter par l'acteur puisqu'elles ne mutent rien ni
    /// n'émettent d'évènement.
    ///
    /// Publie ce nœud dans [`NS_WORKSPACE`] — sans quoi
    /// `WorkspaceClient::select_owner` ne pourrait jamais le désigner. Le
    /// hachage cohérent redésignant un autre pair quand le membership du
    /// namespace change, un même workspace peut changer de propriétaire au
    /// fil du temps : le store partagé (Postgres) rend le nouveau
    /// propriétaire immédiatement correct, au prix d'un last-write-wins en
    /// cas de partition — même exposition que le catalogue de sessions, des
    /// leases/epochs restent un travail futur.
    pub fn create(
        layer: impl Layer<Send = WorkspaceEvent, Received = WorkspaceEvent>,
        mut args: WorkspaceServerArgs,
    ) -> WorkspaceServer {
        args.bootstrap.register_to_namespaces([Namespace::from_static(NS_WORKSPACE)]);

        let (tx, rx) = layer.split();
        let mut tx = tx.boxed_sink();
        let _rx = rx.boxed();

        let (event_tx, mut event_rx) = mpsc::unbounded::<WorkspaceEvent>();
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded::<WorkspaceCommand>();

        let store = args.store;

        {
            let store = store.clone();
            tokio::spawn(async move {
                use WorkspaceCommand::*;
                loop {
                    select! {
                        Ok(event_to_send) = event_rx.recv() => {
                            let _ = tx.send(event_to_send).await;
                        }
                        Ok(cmd) = cmd_rx.recv() => {
                            match cmd {
                                Insert { workspace, reply } => {
                                    let store = store.clone();
                                    let event_tx = event_tx.clone();
                                    tokio::spawn(async move {
                                        let result = Self::insert(workspace, store, event_tx).await;
                                        let _ = reply.send(result);
                                    });
                                }
                                Remove { id, reply } => {
                                    let store = store.clone();
                                    let event_tx = event_tx.clone();
                                    tokio::spawn(async move {
                                        let result = Self::remove(id, store, event_tx).await;
                                        let _ = reply.send(result);
                                    });
                                }
                                AddSession { workspace_id, session_id, reply } => {
                                    tokio::spawn(Self::add_session(workspace_id, session_id, store.clone(), event_tx.clone(), reply));
                                }
                                RemoveSession { workspace_id, session_id, reply } => {
                                    tokio::spawn(Self::remove_session(workspace_id, session_id, store.clone(), event_tx.clone(), reply));
                                }
                                PatchVars { workspace_id, path, value, reply } => {
                                    tokio::spawn(Self::patch_vars(workspace_id, path, value, store.clone(), event_tx.clone(), reply));
                                }
                            }
                        }
                    }
                }
            });
        }

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

        WorkspaceServer { store, cmd_tx }
    }

    async fn insert(
        workspace: Workspace,
        store: WorkspaceStoreClient,
        event_tx: WorkspaceServerEventEmitter,
    ) -> Result<(), anyhow::Error> {
        let id = workspace.id;
        store.insert(workspace).await?;
        let _ = event_tx.unbounded_send(WorkspaceEvent::Created { id });
        Ok(())
    }

    async fn remove(
        id: WorkspaceId,
        store: WorkspaceStoreClient,
        event_tx: WorkspaceServerEventEmitter,
    ) -> Result<(), anyhow::Error> {
        store.delete(id).await?;
        let _ = event_tx.unbounded_send(WorkspaceEvent::Removed { id });
        Ok(())
    }

    async fn add_session(
        workspace_id: WorkspaceId,
        session_id: SessionId,
        store: WorkspaceStoreClient,
        event_tx: WorkspaceServerEventEmitter,
        reply: oneshot::Sender<Result<(), String>>,
    ) -> anyhow::Result<()> {
        match add_session(store, workspace_id, session_id).await {
            Ok(()) => {
                let _ = event_tx.unbounded_send(WorkspaceEvent::SessionAdded { workspace_id, session_id });
                let _ = reply.send(Ok(()));
            }
            Err(error) => {
                let _ = reply.send(Err(error.to_string()));
            }
        }

        Ok(())
    }

    async fn remove_session(
        workspace_id: WorkspaceId,
        session_id: SessionId,
        store: WorkspaceStoreClient,
        event_tx: WorkspaceServerEventEmitter,
        reply: oneshot::Sender<Result<(), String>>,
    ) -> anyhow::Result<()> {
        match remove_session(store, workspace_id, session_id).await {
            Ok(()) => {
                let _ = event_tx.unbounded_send(WorkspaceEvent::SessionRemoved { workspace_id, session_id });
                let _ = reply.send(Ok(()));
            }
            Err(error) => {
                let _ = reply.send(Err(error.to_string()));
            }
        }

        Ok(())
    }

    async fn patch_vars(
        workspace_id: WorkspaceId,
        path: String,
        value: Value,
        store: WorkspaceStoreClient,
        event_tx: WorkspaceServerEventEmitter,
        reply: oneshot::Sender<Result<(), String>>,
    ) -> anyhow::Result<()> {
        match patch_vars(store, workspace_id, &path, value).await {
            Ok(()) => {
                let _ = event_tx.unbounded_send(WorkspaceEvent::VarsPatched { workspace_id });
                let _ = reply.send(Ok(()));
            }
            Err(error) => {
                let _ = reply.send(Err(error.to_string()));
            }
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct WorkspaceServer {
    pub(crate) store: WorkspaceStoreClient,
    pub(crate) cmd_tx: mpsc::UnboundedSender<WorkspaceCommand>,
}

/// Récupère `workspace_id` dans le store, ou une erreur lisible s'il n'est
/// pas (encore) connu de ce nœud — commun aux opérations ci-dessous, qui
/// mutent un workspace existant plutôt que d'en créer un : la création est
/// un acte de cycle de vie explicite (voir
/// [`crate::workspace::rpc::InsertWorkspace`]), une création silencieuse ici
/// ressusciterait un workspace supprimé sur un RPC tardif.
pub(crate) async fn get_workspace(store: WorkspaceStoreClient, workspace_id: WorkspaceId) -> Result<Workspace, anyhow::Error> {
    store
        .clone()
        .get(workspace_id)
        .await?
        .ok_or_else(|| anyhow!("workspace inconnu : {workspace_id}"))
}

/// Rattache `session_id` au workspace `workspace_id` — voir
/// [`Workspace::add_session`] (idempotent).
pub(crate) async fn add_session(
    store: WorkspaceStoreClient,
    workspace_id: WorkspaceId,
    session_id: SessionId,
) -> Result<(), anyhow::Error> {
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
) -> Result<(), anyhow::Error> {
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
) -> Result<Vec<Value>, anyhow::Error> {
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
) -> Result<(), anyhow::Error> {
    let mut workspace = get_workspace(store.clone(), workspace_id).await?;
    let doc = serde_json::to_value(&workspace.vars)?;
    let patched = jsonpath_lib::replace_with(doc, path, &mut |_| Some(value.clone()))?;
    workspace.vars = serde_json::from_value(patched)?;

    store.replace(workspace).await?;
    Ok(())
}

#[cfg(test)]
mod tests {}
