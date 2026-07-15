pub mod info;

use std::sync::Arc;
use std::time::Duration;

use anyhow::bail;
use futures::StreamExt as _;
use libp2p::PeerId;
use serde_json::Value;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinSet;
use tracing::{info, warn};

use object_store::ObjectStore;
use sqlx::postgres::PgPool;

use crate::{
    agent::{GlobalAgentId, context::ContextEntry, frame::AgentFrame, role::Role, status::{AgentStatus, YieldStatus}},
    job::{Job, JobId, JobKind, JobState},
    mode::{
        SessionMode,
        executable::{AgentRuntime, NodeOutcome, RustRegistry},
        orchestration::Orchestration,
        state_graph::StateGraph,
    },
    network::{
        actor::{NetworkActor, NetworkService},
        cp::rpc::{JobStateReport, RpcCall, RpcResult, RunJobRequest, SessionFetchRequest, Void, WorkspaceFetchRequest},
        peer::NodeKind,
        start_swarm,
    },
    persistency::vfs::WorkspaceVfs,
    secret::SecretManager,
    session::{SessionId, client::SessionClient},
    workspace::client::WorkspaceClient,
};

/// `secret` : secret partagé par le cluster, utilisé pour vérifier
/// automatiquement qu'un pair prétendant être control plane l'est vraiment
/// (voir `secret::SecretManager::verify_membership` et
/// `network::actor::NetworkActor`) avant de lui faire confiance et de lui
/// envoyer des jobs.
///
/// `pool`/`store` : backends du VFS des sessions (voir
/// `persistency::vfs::WorkspaceVfs`), partagés par tous les workers du
/// cluster — `store` au choix via `persistency::FilesystemConfig`.
///
/// `rust_registry` : fonctions Rust utilisables comme `Executable::Rust` par
/// les nœuds/arêtes d'un `mode::state_graph::StateGraph` exécuté sur ce
/// worker (voir [`RustRegistry`]) — à peupler par l'appelant avant ou après
/// `start`, l'instance passée ici reste modifiable ensuite (`RustRegistry`
/// est bon marché à cloner, mutation intérieure). Un
/// [`AgentRuntime`](crate::mode::executable::AgentRuntime) est construit ici
/// même, à partir du [`NetworkClient`] de ce worker, pour les nœuds
/// `Executable::Agent` d'un tel graphe — pas besoin de le recevoir en
/// paramètre, contrairement à `rust_registry` : il n'y a rien à peupler par
/// avance, juste des clients vers le control plane.
///
/// `ready` : signalé avec le [`NetworkClient`] de ce nœud dès la connexion
/// établie, avant que la boucle ci-dessous ne démarre — voir
/// `node::Marie::start`.
///
/// `shutdown` : demande d'arrêt propre (voir `node::MarieHandle::shutdown`)
/// — la boucle cesse d'accepter de nouveaux événements dès qu'elle se
/// déclenche, puis les jobs déjà en vol (voir [`execute_rpc`]) ont jusqu'à
/// [`SHUTDOWN_GRACE_PERIOD`] pour rapporter leur issue (voir
/// [`drain_job_tasks`]) avant que la connexion réseau ne soit coupée.
pub async fn start_worker(
    secret: Arc<SecretManager>,
    pool: PgPool,
    store: Arc<dyn ObjectStore>,
    rust_registry: RustRegistry,
    mut shutdown: watch::Receiver<bool>,
    ready: oneshot::Sender<NetworkService>,
) -> Result<(), anyhow::Error> {
    use NodeKind::Worker;

    let swarm = start_swarm(Worker, |_| {}).await?;
    let local_peer_id = *swarm.local_peer_id();
    let (actor, client) = NetworkActor::new(swarm, secret);
    let _ = ready.send(client.clone());

    // `WorkspaceClient` sur le même principe que `SessionClient` ci-dessous,
    // pour servir les demandes `RpcCall::FETCH_WORKSPACE` d'un pair (voir
    // [`execute_rpc`]) — construit avant `sessions` car `WorkspaceVfs` (donc
    // `SessionClient::vfs`) en a besoin.
    let workspaces = WorkspaceClient::new(client.clone());
    // Schéma des tables à schéma fixe du VFS (`fs_alias`, `fs_inode`) — voir
    // `persistency::postgres::run_migrations` ; à appliquer avant toute
    // utilisation de `workspace_vfs` ci-dessous, qui en dépend.
    crate::persistency::run_migrations(&pool).await?;
    // `SessionClient` s'abonne lui-même au flux d'événements de `client` (voir
    // `NetworkClient::subscribe_events`) pour le gossip qui l'intéresse — un flux
    // indépendant de celui que cette boucle consomme ci-dessous pour répondre aux
    // `RequestRemoteProcedureExecution`.
    let workspace_vfs = WorkspaceVfs::new(workspaces.clone(), pool, store);
    let sessions = SessionClient::new(client.clone(), workspace_vfs);
    // Clients du control plane nécessaires à un nœud `Executable::Agent`
    // (voir `mode::state_graph::run_agent_task`) — construits une fois ici,
    // comme `sessions`, plutôt qu'à chaque job (bon marché à cloner).
    let agents = AgentRuntime::new(client.clone());
    let mut events = client.subscribe_events();

    let actor_task = tokio::spawn(actor.run());

    // Jobs actuellement en vol (voir `RpcCall::RUN_JOB` dans [`execute_rpc`])
    // — suivis pour pouvoir les laisser se terminer avant de couper le
    // réseau à l'arrêt (voir [`drain_job_tasks`]), plutôt que de les
    // abandonner en plein milieu d'un pas.
    let mut job_tasks: JoinSet<()> = JoinSet::new();

    // `true` une fois `shutdown` fermé sans arrêt explicite demandé (voir
    // `node::MarieHandle`, qui documente qu'abandonner la poignée n'arrête
    // *pas* le nœud) — désactive alors la branche `shutdown.changed()`
    // ci-dessous plutôt que de la laisser se redéclencher en boucle serrée.
    let mut shutdown_gone = false;

    loop {
        tokio::select! {
            Some(event) = events.next() => {
                use crate::network::actor::NetworkEvent::*;
                match event {
                    RequestRemoteProcedureExecution { tx, call, peer: _ } => {
                        let res = execute_rpc(call, &client, &sessions, &workspaces, &rust_registry, &agents, &mut job_tasks, local_peer_id).await;
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
                    // Ce nœud ne participe pas au cluster Raft du control plane, ni au
                    // registre RPC dynamique inter-control-planes : ces événements ne
                    // concernent que les control planes entre eux. `GossipMessageReceived`
                    // est traité indépendamment par `SessionClient` (voir plus haut).
                    ControlPlanePeerDiscovered { .. }
                    | WorkerPeerDiscovered { .. }
                    | PersistencyPeerDiscovered { .. }
                    | PeerDisconnected { .. }
                    | PubSubReceived { .. } => {}
                }
            }
            result = shutdown.changed(), if !shutdown_gone => {
                match result {
                    Ok(()) if *shutdown.borrow() => {
                        info!("arrêt propre du worker demandé");
                        break;
                    }
                    Ok(()) => {}
                    Err(_) => shutdown_gone = true,
                }
            }
        }
    }

    drain_job_tasks(&mut job_tasks).await;

    client.shutdown();
    let _ = actor_task.await;
    Ok(())
}

/// Délai maximal laissé aux jobs en vol pour se terminer d'eux-mêmes à
/// l'arrêt du worker (voir [`drain_job_tasks`]) — au-delà, on cesse
/// d'attendre plutôt que de bloquer l'arrêt indéfiniment : un job dont
/// l'issue n'a pas pu être rapportée avant expiration reste `Running` selon
/// `ControlPlaneState`, et sera détecté et réassigné par le control plane
/// comme n'importe quel worker mort dès que ce nœud aura effectivement
/// disparu (voir `network::cp::reconcile`).
const SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(30);

/// Attend que `job_tasks` se vide (chaque job en vol a rapporté son issue,
/// voir [`execute_and_report`]), borné par [`SHUTDOWN_GRACE_PERIOD`].
async fn drain_job_tasks(job_tasks: &mut JoinSet<()>) {
    if job_tasks.is_empty() {
        return;
    }

    info!(remaining = job_tasks.len(), "attente de la fin des jobs en vol avant arrêt");

    let drain = async {
        while job_tasks.join_next().await.is_some() {}
    };

    if tokio::time::timeout(SHUTDOWN_GRACE_PERIOD, drain).await.is_err() {
        warn!(remaining = job_tasks.len(), "délai de grâce d'arrêt épuisé, jobs en vol abandonnés (seront réassignés)");
    }
}

async fn execute_rpc(
    call: RpcCall,
    client: &NetworkService,
    sessions: &SessionClient,
    workspaces: &WorkspaceClient,
    rust_registry: &RustRegistry,
    agents: &AgentRuntime,
    job_tasks: &mut JoinSet<()>,
    local_peer_id: PeerId,
) -> Result<serde_json::Value, anyhow::Error> {
    match call.name.as_str() {
        RpcCall::RUN_JOB => {
            let request: RunJobRequest = serde_json::from_value(call.args)?;
            let client = client.clone();
            let sessions = sessions.clone();
            let workspaces = workspaces.clone();
            let rust_registry = rust_registry.clone();
            let agents = agents.clone();

            // Suivi via `job_tasks` (voir `start_worker`) plutôt qu'un
            // `tokio::spawn` détaché : permet de laisser ce job se terminer
            // avant de couper le réseau à l'arrêt (voir `drain_job_tasks`) —
            // le control plane, lui, n'attend toujours qu'un accusé de
            // réception, pas l'issue du job.
            job_tasks.spawn(execute_and_report(client, sessions, workspaces, rust_registry, agents, request, local_peer_id));

            Ok(serde_json::Value::Null)
        }
        // Worker -> worker : un pair qui reprend une session dont nous avons la
        // dernière version demande le diff qui lui manque (voir
        // `SessionClient::acquire`). Refusé silencieusement (erreur) si nous ne
        // la détenons pas (plus localement, ou jamais détenue).
        RpcCall::FETCH_SESSION => {
            let request: SessionFetchRequest = serde_json::from_value(call.args)?;
            Ok(serde_json::to_value(sessions.serve_fetch(request).await?)?)
        }
        // Worker -> worker : même principe que `FETCH_SESSION` ci-dessus,
        // pour un workspace (voir `WorkspaceClient::acquire`).
        RpcCall::FETCH_WORKSPACE => {
            let request: WorkspaceFetchRequest = serde_json::from_value(call.args)?;
            Ok(serde_json::to_value(workspaces.serve_fetch(request).await?)?)
        }
        name => bail!("unmanaged remote procedure {name}"),
    }
}

/// Exécute le job puis rapporte son résultat au control plane.
///
/// Les erreurs de rapport (control plane injoignable, pas leader, etc.) sont
/// loggées mais ne remontent nulle part : c'est un fire-and-forget, cohérent
/// avec la réassignation côté control plane — un job sans nouvelle finira par
/// être détecté et réassigné au prochain healthcheck manqué.
async fn execute_and_report(
    client: NetworkService,
    sessions: SessionClient,
    workspaces: WorkspaceClient,
    rust_registry: RustRegistry,
    agents: AgentRuntime,
    request: RunJobRequest,
    local_peer_id: PeerId,
) {
    let job_id = request.job.id;

    if let Err(error) = report_job_state(&client, job_id, JobState::Running { worker: local_peer_id }).await {
        warn!(%error, %job_id, "impossible de rapporter le démarrage du job");
    }

    let new_state = match run_job(request, &client, &sessions, &workspaces, &rust_registry, &agents).await {
        Ok(RunOutcome::Completed { result }) => JobState::Completed { result },
        Ok(RunOutcome::Yielded { reason }) => JobState::Yielded { reason },
        Err(error) => JobState::Failed { error },
    };

    if let Err(error) = report_job_state(&client, job_id, new_state).await {
        warn!(%error, %job_id, "impossible de rapporter l'issue du job");
    }
}

async fn report_job_state(client: &NetworkService, job_id: JobId, state: JobState) -> Result<(), anyhow::Error> {
    client.rpc::<Void>(RpcCall::new(RpcCall::REPORT_JOB_STATE, JobStateReport { job_id, state })).await?;
    Ok(())
}

/// Issue d'un run borné d'agent (voir [`run_job`]) — jamais `Pending`/
/// `Scheduled`/`Running`, qui décrivent un job avant/pendant son exécution :
/// seule l'exécution elle-même sait laquelle de ces deux issues terminales
/// (voir `job::JobState`, dont chaque variante ici a un pendant direct) elle
/// a atteinte.
#[derive(Debug)]
enum RunOutcome {
    Completed { result: String },
    /// Le run s'est arrêté sans conclure (voir `agent::status::YieldStatus`) —
    /// le job se termine ici ; reprendre l'agent une fois la condition
    /// résolue est la responsabilité du control plane (voir
    /// `network::cp::mod::on_job_terminated`/`resume_after_hitl_answer`),
    /// pas de ce worker.
    Yielded { reason: crate::agent::status::YieldStatus },
}

/// Exécute effectivement le job : synchronise la session (voir
/// [`SessionClient::acquire`]) puis pilote le mode actuellement au sommet de
/// sa pile (voir `mode::SessionMode`) — chaque mode a sa propre logique de
/// run borné, voir [`drive_state_graph`] pour `StateGraph`, [`run_simple`]
/// pour `Simple` et [`run_orchestration`] pour `Orchestration`.
async fn run_job(
    request: RunJobRequest,
    client: &NetworkService,
    sessions: &SessionClient,
    workspaces: &WorkspaceClient,
    rust_registry: &RustRegistry,
    agents: &AgentRuntime,
) -> Result<RunOutcome, String> {
    use crate::job::JobKind::RunAgent;

    match request.job.kind {
        RunAgent(global_agent_id) => {
            let session_id = global_agent_id.session_id();

            sessions.acquire(session_id).await.map_err(|error| error.to_string())?;

            match sessions.current_mode(session_id).await {
                SessionMode::StateGraph(graph) => drive_state_graph(sessions, rust_registry, agents, session_id, graph).await,
                SessionMode::Simple => run_simple(sessions, agents, global_agent_id).await,
                SessionMode::Orchestration(orchestration) => {
                    run_orchestration(sessions, workspaces, client, global_agent_id, orchestration).await
                }
            }
        }
    }
}

/// Pilote un agent en mode [`SessionMode::Simple`] : charge son [`AgentFrame`](crate::agent::frame::AgentFrame)
/// depuis la session déjà synchronisée (voir [`SessionClient::frame`]) et lui
/// délègue un run borné (voir [`crate::agent::run`], qui persiste lui-même sa
/// progression au fil de l'eau — rien à persister ici après coup,
/// contrairement à [`drive_state_graph`]). Échoue si ce frame n'existe pas
/// encore dans la session : contrairement à un `StateGraph` (créé à la volée
/// par [`StateGraph::new`](crate::mode::state_graph::StateGraph::new) lors du
/// push du mode), un frame `Simple` doit avoir été écrit au moins une fois
/// (voir `session::crdt::YrsSession::put_frame`) avant qu'un job ne référence
/// cet agent — pas encore câblé (voir `agent::AgentSpawnRequest`).
async fn run_simple(sessions: &SessionClient, agents: &AgentRuntime, global_agent_id: crate::agent::GlobalAgentId) -> Result<RunOutcome, String> {
    let session_id = global_agent_id.session_id();
    let local_id = global_agent_id.local_id();

    let mut frame = sessions
        .frame(session_id, local_id)
        .await
        .ok_or_else(|| format!("frame {local_id} inconnu de la session {session_id}"))?;

    match crate::agent::run(&mut frame, &agents.model, &agents.tools, &agents.hitl, sessions).await {
        Ok(crate::agent::RunOutcome::Completed { text }) => Ok(RunOutcome::Completed { result: text.unwrap_or_default() }),
        Ok(crate::agent::RunOutcome::Yielded { reason }) => Ok(RunOutcome::Yielded { reason }),
        Err(error) => Err(error.to_string()),
    }
}

/// Pilote un agent en mode [`SessionMode::Orchestration`] : contrairement à
/// [`run_simple`]/[`drive_state_graph`], deux passages bien distincts
/// partagent cette fonction, discriminés par [`Orchestration::children`] —
/// aucun autre état ne permet de les distinguer, `global_agent_id` restant
/// le même frame (l'orchestrateur) tout au long du cycle :
///
/// - Premier passage (`children` vide) : le frame `global_agent_id` porte la
///   tâche à déléguer, sous la forme de son dernier message [`Role::User`].
///   Un unique enfant est créé dans une session neuve du même workspace
///   (voir [`WorkspaceClient::create_session`]) — une session séparée plutôt
///   qu'un second frame de la même session : la pile de modes est une
///   propriété de la session entière (voir `mode::SessionMode`), pas du
///   frame, un second frame de cette session hériterait donc aussitôt du
///   mode `Orchestration` de son parent au lieu de s'exécuter en `Simple`.
///   L'enfant reçoit cette tâche comme unique message initial et est
///   aussitôt soumis comme job (voir [`NetworkClient::spawn_job`]). Le run
///   se termine sur [`YieldStatus::WaitingChildren`] : la reprise, une fois
///   l'enfant `Completed`, est déclenchée par le control plane (voir
///   `network::cp::mod::resume_orchestration_parents`), pas par ce worker.
/// - Reprise (`children` non vide) : le control plane ne resoumet un job
///   pour ce même `global_agent_id` qu'une fois *tous* les enfants
///   `Completed` (voir `resume_orchestration_parents`) — leur dernier
///   message [`Role::Assistant`] est agrégé dans le contexte du frame
///   parent (comme un message [`Role::Tool`]), le mode est dépilé (retour à
///   [`SessionMode::Simple`], ou au mode englobant précédent) et le run
///   conclut sur `Completed`.
///
/// Un seul enfant par cycle : rien dans [`Orchestration`] ne porte
/// aujourd'hui plusieurs tâches à répartir à la fois — `strategy` est
/// conservé tel quel pour un futur pilotage à plusieurs enfants créés d'un
/// coup, sans effet observable tant qu'un seul est créé par cycle. Si un
/// second job est soumis pour ce `global_agent_id` alors qu'un cycle est
/// déjà en cours (`children` non vide mais pas tous encore `Finished`), le
/// run échoue explicitement plutôt que d'agréger un résultat partiel : un
/// seul cycle d'orchestration à la fois par session.
async fn run_orchestration(
    sessions: &SessionClient,
    workspaces: &WorkspaceClient,
    client: &NetworkService,
    global_agent_id: GlobalAgentId,
    mut orchestration: Orchestration,
) -> Result<RunOutcome, String> {
    let session_id = global_agent_id.session_id();
    let local_id = global_agent_id.local_id();

    let frame = sessions
        .frame(session_id, local_id)
        .await
        .ok_or_else(|| format!("frame {local_id} inconnu de la session {session_id}"))?;

    if orchestration.children.is_empty() {
        let task = frame
            .context
            .iter()
            .rev()
            .find(|entry| entry.role == Role::User)
            .map(|entry| entry.content.clone())
            .ok_or_else(|| "aucun message utilisateur à déléguer dans le contexte de l'orchestrateur".to_string())?;

        let workspace_id = sessions.workspace_of(session_id).await.map_err(|error| error.to_string())?;
        workspaces.acquire(workspace_id).await.map_err(|error| error.to_string())?;
        let child_session_id = workspaces.create_session(workspace_id).await.map_err(|error| error.to_string())?;
        sessions.acquire(child_session_id).await.map_err(|error| error.to_string())?;

        let child_local_id = crate::id::generate_id();
        let child_frame = AgentFrame {
            session_id: child_session_id,
            id: child_local_id,
            model_id: frame.model_id.clone(),
            status: AgentStatus::Initial,
            allowed_tools: frame.allowed_tools.clone(),
            context: vec![ContextEntry { role: Role::User, content: task }].into(),
            stdio: String::new(),
            stderr: String::new(),
        };
        sessions.put_frame(child_session_id, child_local_id, &child_frame).await.map_err(|error| error.to_string())?;

        let child_agent_id = GlobalAgentId::new(child_session_id, child_local_id);
        let child_job = Job { id: crate::id::generate_id(), kind: JobKind::RunAgent(child_agent_id) };
        client.spawn_job(child_job).await.map_err(|error| error.to_string())?;

        orchestration.add_child(child_agent_id);
        sessions.update_current_mode(session_id, SessionMode::Orchestration(orchestration.clone())).await.map_err(|error| error.to_string())?;

        let reason = YieldStatus::WaitingChildren { children: orchestration.children.clone() };
        sessions.set_frame_status(session_id, local_id, AgentStatus::Yielding(reason.clone())).await.map_err(|error| error.to_string())?;

        return Ok(RunOutcome::Yielded { reason });
    }

    let mut aggregated = String::new();
    for child_id in &orchestration.children {
        sessions.acquire(child_id.session_id()).await.map_err(|error| error.to_string())?;
        let child_frame = sessions
            .frame(child_id.session_id(), child_id.local_id())
            .await
            .ok_or_else(|| format!("frame enfant {child_id:?} introuvable"))?;

        if child_frame.status != AgentStatus::Finished {
            return Err(format!("cycle d'orchestration déjà en cours pour {global_agent_id:?} : enfant {child_id:?} pas encore terminé"));
        }

        let result = child_frame
            .context
            .iter()
            .rev()
            .find(|entry| entry.role == Role::Assistant)
            .map(|entry| entry.content.clone())
            .unwrap_or_default();
        aggregated.push_str(&result);
        aggregated.push('\n');
    }

    sessions
        .push_context_entry(session_id, local_id, ContextEntry { role: Role::Tool, content: aggregated.clone() })
        .await
        .map_err(|error| error.to_string())?;
    sessions.pop_mode(session_id).await.map_err(|error| error.to_string())?;
    sessions.set_frame_status(session_id, local_id, AgentStatus::Finished).await.map_err(|error| error.to_string())?;

    Ok(RunOutcome::Completed { result: aggregated })
}

/// Nombre maximal de pas (nœud exécuté + tentative de transition) qu'un
/// seul job peut effectuer avant de yielder — garde-fou pour qu'un graphe
/// qui boucle sans jamais atteindre un nœud terminal ne monopolise pas
/// indéfiniment ce worker (voir `job::JobState`, où un job reste un run
/// *borné*, voir aussi `network::cp::mod::submit_resume_job`). Le control
/// plane reprend automatiquement un job yieldé pour cette raison au
/// prochain rapport (voir `network::cp::mod::on_job_terminated`).
const MAX_STATE_GRAPH_STEPS_PER_RUN: u32 = 64;

/// Pilote un `mode::state_graph::StateGraph` : exécute le nœud courant,
/// avance, persiste la progression (voir
/// [`SessionClient::update_current_mode`]) après chaque pas — pour qu'un
/// crash en plein milieu ne perde pas ce qui a déjà été accompli, un
/// nouveau job reprendrait exactement où celui-ci s'est arrêté — jusqu'à :
///
/// - un nœud sans arête sortante qui matche (fin naturelle du graphe, voir
///   `mode::state_graph::StateGraph::advance`) : le run est `Completed`,
///   avec la dernière valeur produite par un nœud comme résultat ;
/// - une action de nœud qui retourne [`NodeOutcome::Yield`] : le run est
///   `Yielded` sur cette raison, sans avancer (voir la note sur
///   [`NodeFn`](crate::mode::executable::NodeFn)) ;
/// - l'épuisement de [`MAX_STATE_GRAPH_STEPS_PER_RUN`] : `Yielded { RunExhausted }`.
async fn  drive_state_graph(
    sessions: &SessionClient,
    rust_registry: &RustRegistry,
    agents: &AgentRuntime,
    session_id: SessionId,
    mut graph: StateGraph,
) -> Result<RunOutcome, String> {
    let mut input = Value::Null;

    for _ in 0..MAX_STATE_GRAPH_STEPS_PER_RUN {
        match graph.execute_current(rust_registry, Some(agents), input.clone()).await.map_err(|error| error.to_string())? {
            None => {}
            Some(NodeOutcome::Value(value)) => input = value,
            Some(NodeOutcome::Yield(reason)) => return Ok(RunOutcome::Yielded { reason }),
        }

        let advanced = graph.advance(rust_registry, input.clone()).await.map_err(|error| error.to_string())?;

        sessions
            .update_current_mode(session_id, SessionMode::StateGraph(graph.clone()))
            .await
            .map_err(|error| error.to_string())?;

        if !advanced {
            return Ok(RunOutcome::Completed { result: input.to_string() });
        }
    }

    Ok(RunOutcome::Yielded { reason: crate::agent::status::YieldStatus::RunExhausted })
}
