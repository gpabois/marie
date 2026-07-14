use std::sync::Arc;

use anyhow::bail;
use futures::StreamExt as _;
use libp2p::PeerId;
use object_store::ObjectStore;
use sqlx::postgres::PgPool;
use tokio::sync::{oneshot, watch};
use tracing::{info, warn};
use yrs::{StateVector, updates::{decoder::Decode, encoder::Encode}};

use crate::{
    network::{
        actor::{NetworkActor, NetworkClient, NetworkEvent},
        cp::rpc::{RpcCall, RpcResult, SessionFetchRequest, WorkspaceFetchRequest},
        peer::NodeKind,
        start_swarm,
    },
    persistency::{SessionStore, WorkspaceStore, vfs::WorkspaceVfs},
    secret::SecretManager,
    session::{SessionId, crdt::YrsSession, sync::{SESSION_SYNC_TOPIC, SessionSyncMessage}},
    workspace::{WorkspaceId, client::WorkspaceClient, crdt::YrsWorkspace, sync::{WORKSPACE_SYNC_TOPIC, WorkspaceSyncMessage}},
};

/// Démarre un nœud `Persistency` : détenteur de secours durable pour les
/// sessions et les workspaces (voir `ControlPlaneState::persistency_nodes`,
/// `network::cp::session_holders_for`/`workspace_holders_for` et
/// `network::cp::reconcile`), qui rejoue les diffs gossipés sur
/// `session::sync::SESSION_SYNC_TOPIC`/`workspace::sync::WORKSPACE_SYNC_TOPIC`
/// dans `store`/`workspace_store` et répond aux demandes
/// [`RpcCall::FETCH_SESSION`]/[`RpcCall::FETCH_WORKSPACE`] à partir de ce qui
/// y est stocké.
///
/// Ne participe ni au cluster Raft du control plane, ni à l'exécution de
/// jobs — un pair de plus dans le mesh, découvert par le control plane comme
/// `WorkerPeerDiscovered`/`ControlPlanePeerDiscovered` le sont (voir
/// `NetworkEvent::PersistencyPeerDiscovered`).
///
/// `ready` : signalé avec le [`NetworkClient`] de ce nœud dès la connexion
/// établie, avant que la boucle ci-dessous ne démarre — voir
/// `node::Marie::start`.
///
/// `shutdown` : demande d'arrêt propre (voir `node::MarieHandle::shutdown`)
/// — chaque événement est traité en séquence, jamais délégué à une tâche de
/// fond (contrairement à `network::worker::start_worker`, qui doit drainer
/// des jobs en vol) : dès que ce nœud sort de la boucle, il n'y a plus rien
/// à terminer, l'actor peut s'arrêter immédiatement à sa suite (voir
/// `NetworkClient::shutdown`).
pub async fn start_persistency(
    secret: Arc<SecretManager>,
    store: Arc<dyn SessionStore>,
    workspace_store: Arc<dyn WorkspaceStore>,
    pool: PgPool,
    object_store: Arc<dyn ObjectStore>,
    mut shutdown: watch::Receiver<bool>,
    ready: oneshot::Sender<NetworkClient>,
) -> Result<(), anyhow::Error> {
    use NodeKind::Persistency;

    let swarm = start_swarm(Persistency, |_| {}).await?;
    let (actor, client) = NetworkActor::new(swarm, secret);
    let _ = ready.send(client.clone());

    // Schéma des tables à schéma fixe du VFS (`fs_alias`, `fs_inode`) — voir
    // `persistency::postgres::run_migrations` ; à appliquer avant toute
    // utilisation de `workspace_vfs` ci-dessous, qui en dépend.
    crate::persistency::run_migrations(&pool).await?;

    // Nécessaire uniquement pour purger `/session/files` d'une session
    // supprimée (voir `RpcCall::DELETE_SESSION` dans [`execute_rpc`]) — ce
    // nœud ne monte jamais de VFS complet, contrairement à un worker.
    let workspace_vfs = WorkspaceVfs::new(WorkspaceClient::new(client.clone()), pool, object_store);

    client.subscribe(SESSION_SYNC_TOPIC);
    client.subscribe(WORKSPACE_SYNC_TOPIC);
    let mut events = client.subscribe_events();

    let actor_task = tokio::spawn(actor.run());

    // `true` une fois `shutdown` fermé sans arrêt explicite demandé (voir
    // `node::MarieHandle`, qui documente qu'abandonner la poignée n'arrête
    // *pas* le nœud) — désactive alors la branche `shutdown.changed()`
    // ci-dessous plutôt que de la laisser se redéclencher en boucle serrée
    // (un canal fermé résout `changed()` immédiatement, à chaque appel).
    let mut shutdown_gone = false;

    loop {
        tokio::select! {
            Some(event) = events.next() => {
                use NetworkEvent::*;
                match event {
                    RequestRemoteProcedureExecution { tx, call, peer: _ } => {
                        let res = execute_rpc(call, &store, &workspace_store, &workspace_vfs, &client).await;
                        let res = match res {
                            Ok(value) => RpcResult::RpcOk(value),
                            Err(error) => RpcResult::RpcErr(error.to_string()),
                        };
                        // `tx` est partagé (voir `RpcReplySlot`) : un seul abonné doit
                        // effectivement répondre, celui qui réussit `.take()` en premier
                        // (ici, toujours nous — ce nœud est seul à vouloir répondre).
                        if let Ok(mut tx) = tx.lock() {
                            if let Some(tx) = tx.take() {
                                let _ = tx.send(res);
                            }
                        }
                    }
                    GossipMessageReceived { topic, data, source } if topic == SESSION_SYNC_TOPIC => {
                        if let Err(error) = ingest_session_diff(&store, &client, source, &data).await {
                            warn!(%error, "traitement du diff de session échoué, ignoré");
                        }
                    }
                    GossipMessageReceived { topic, data, source } if topic == WORKSPACE_SYNC_TOPIC => {
                        if let Err(error) = ingest_workspace_diff(&workspace_store, &client, source, &data).await {
                            warn!(%error, "traitement du diff de workspace échoué, ignoré");
                        }
                    }
                    // Ce nœud ne participe ni au cluster Raft du control plane, ni à
                    // l'exécution de jobs, ni au registre RPC dynamique : seul le gossip
                    // sur `SESSION_SYNC_TOPIC`/`WORKSPACE_SYNC_TOPIC` (traité ci-dessus) le
                    // concerne.
                    ControlPlanePeerDiscovered { .. }
                    | WorkerPeerDiscovered { .. }
                    | PersistencyPeerDiscovered { .. }
                    | PeerDisconnected { .. }
                    | GossipMessageReceived { .. } => {}
                }
            }
            result = shutdown.changed(), if !shutdown_gone => {
                match result {
                    Ok(()) if *shutdown.borrow() => {
                        info!("arrêt propre du nœud persistency demandé");
                        break;
                    }
                    Ok(()) => {}
                    Err(_) => shutdown_gone = true,
                }
            }
        }
    }

    client.shutdown();
    let _ = actor_task.await;
    Ok(())
}

async fn execute_rpc(
    call: RpcCall,
    store: &Arc<dyn SessionStore>,
    workspace_store: &Arc<dyn WorkspaceStore>,
    workspace_vfs: &WorkspaceVfs,
    client: &NetworkClient,
) -> Result<serde_json::Value, anyhow::Error> {
    match call.name.as_str() {
        RpcCall::FETCH_SESSION => {
            let request: SessionFetchRequest = serde_json::from_value(call.args)?;
            let remote_sv = StateVector::decode_v1(&request.state_vector).map_err(|error| anyhow::anyhow!(error))?;

            let Some(diff) = store.diff_since(request.session_id, &remote_sv).await? else {
                bail!("session {} inconnue de ce nœud de persistance", request.session_id);
            };

            Ok(serde_json::to_value(diff)?)
        }
        // Worker/client -> nœud de persistance : même principe que
        // `FETCH_SESSION` ci-dessus, pour le contenu CRDT d'un workspace
        // (voir `persistency::WorkspaceStore`).
        RpcCall::FETCH_WORKSPACE => {
            let request: WorkspaceFetchRequest = serde_json::from_value(call.args)?;
            let remote_sv = StateVector::decode_v1(&request.state_vector).map_err(|error| anyhow::anyhow!(error))?;

            let Some(diff) = workspace_store.diff_since(request.workspace_id, &remote_sv).await? else {
                bail!("workspace {} inconnu de ce nœud de persistance", request.workspace_id);
            };

            Ok(serde_json::to_value(diff)?)
        }
        // Suppression définitive : le contenu CRDT (voir `SessionStore` —
        // qui emporte au passage `/session/var`, porté par le même doc yrs,
        // voir `session::crdt::YrsSession::state`) et `/session/files` (voir
        // `RpcCall::SESSION_WORKSPACE`/`WorkspaceVfs::delete_session_files`) —
        // aucun des deux n'a de sens à conserver seul une fois la session
        // close.
        RpcCall::DELETE_SESSION => {
            let session_id: SessionId = serde_json::from_value(call.args)?;
            store.delete(&session_id).await?;

            let workspace_id: Option<WorkspaceId> =
                client.rpc(RpcCall::new(RpcCall::SESSION_WORKSPACE, session_id)).await.unwrap_or_default();
            if let Some(workspace_id) = workspace_id {
                workspace_vfs.delete_session_files(workspace_id, session_id).await?;
            }

            Ok(serde_json::Value::Null)
        }
        name => bail!("unmanaged remote procedure {name}"),
    }
}

/// Fusionne un diff gossipé sur `SESSION_SYNC_TOPIC` dans le stockage
/// durable. Une session jamais vue localement ne peut pas être reconstruite
/// à partir d'un simple diff incrémental (voir la note sur les racines
/// concurrentes dans `YrsSession::from_diff`) : on récupère alors l'état
/// complet auprès de `source`, le pair qui vient de gossiper ce diff et qui
/// le détient donc forcément.
async fn ingest_session_diff(store: &Arc<dyn SessionStore>, client: &NetworkClient, source: PeerId, data: &[u8]) -> anyhow::Result<()> {
    let message: SessionSyncMessage = serde_json::from_slice(data)?;

    let mut session = match store.get(&message.session_id).await? {
        Some(session) => session,
        None => {
            let request = SessionFetchRequest { session_id: message.session_id, state_vector: StateVector::default().encode_v1() };
            let full_diff: Vec<u8> = client.rpc_to(RpcCall::new(RpcCall::FETCH_SESSION, request), source).await?;
            YrsSession::from_diff(&full_diff)?
        }
    };

    session.apply_diff(&message.diff)?;
    store.put(&message.session_id, &session).await?;
    Ok(())
}

/// Fusionne un diff gossipé sur `WORKSPACE_SYNC_TOPIC` dans le stockage
/// durable — même principe que [`ingest_session_diff`] (voir sa doc pour la
/// justification de la reconstruction complète depuis `source` en cas de
/// premier diff pour ce workspace).
async fn ingest_workspace_diff(store: &Arc<dyn WorkspaceStore>, client: &NetworkClient, source: PeerId, data: &[u8]) -> anyhow::Result<()> {
    let message: WorkspaceSyncMessage = serde_json::from_slice(data)?;

    let mut workspace = match store.get(&message.workspace_id).await? {
        Some(workspace) => workspace,
        None => {
            let request = WorkspaceFetchRequest { workspace_id: message.workspace_id, state_vector: StateVector::default().encode_v1() };
            let full_diff: Vec<u8> = client.rpc_to(RpcCall::new(RpcCall::FETCH_WORKSPACE, request), source).await?;
            YrsWorkspace::from_diff(&full_diff)?
        }
    };

    workspace.apply_diff(&message.diff)?;
    store.put(&message.workspace_id, &workspace).await?;
    Ok(())
}
