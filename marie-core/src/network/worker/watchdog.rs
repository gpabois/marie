use std::{collections::HashMap, sync::Arc};

use futures::{SinkExt as _, StreamExt};
use libp2p::rendezvous::Namespace;
use parking_lot::Mutex;
use tokio::{select, sync::mpsc};
use typed_builder::TypedBuilder;

use crate::{
    job::{Job, JobId, JobState}, 
    layer::Layer, 
    network::{bootstrap::{BootstrapClient, client::PeerSelection}, 
    worker::{JobResult, NS_WORKER, NS_WORKER_WATCHDOG, RPC_GET_STATE_JOB, RPC_SCHEDULE_JOB, RPC_WATCH_JOB, WorkerError, WorkerEvent}}, rpc::{RpcClient, RpcError, RpcServer, Void, client::RpcCallArgs}, sink::{BoxSink, SinkBoxExt}
};

type WorkEventEmitter = BoxSink<'static, WorkerEvent, anyhow::Error>;
type JobTrackers = Arc<Mutex<HashMap<JobId, JobTrackerInfo>>>;
type CommandEmitter = mpsc::UnboundedSender<Command>;

struct JobTrackerInfo {
    id: JobId,
    job: Job,
    state: JobState,
    retry: u8,
    expires_at: Option<std::time::Duration>
}

enum Command {
    ManageJob(JobId),
    ScheduleJob(JobId),
    SendEvent(WorkerEvent)
}

#[derive(TypedBuilder)]
pub struct WorkerWatchdogArgs {
    bootstrap: BootstrapClient,
    rpc_client: RpcClient,
    rpc_server: RpcServer
}

pub struct WorkerWatchdogActor;

impl WorkerWatchdogActor {
    pub fn new(
        layer: impl Layer<Send = WorkerEvent, Received = WorkerEvent>,
        mut args: WorkerWatchdogArgs
    ) -> WorkerWatchdog {
        use Command::*;

        args.bootstrap.register_to_namespaces([Namespace::from_static(NS_WORKER_WATCHDOG)]);

        let (tx, rx) = layer.split();

        let mut tx = tx.boxed_sink();
        let mut rx = rx.boxed();

        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<Command>();
        let tracked: JobTrackers = Arc::new(Mutex::new(HashMap::default()));
        
        let cmd_tx_1 = cmd_tx.clone();
        let tracked_1 = tracked.clone();
        let bootstrap = args.bootstrap.clone();
        let rpc_client = args.rpc_client.clone();
        tokio::spawn(async move {
            let cmd_tx = cmd_tx_1;
            let tracked = tracked_1;
            loop {
                select! {
                    Some(event) = rx.next() => {
                        match event {
                            WorkerEvent::JobDone { id, result } => {
                                let task = update_job_state_upon_result(
                                    id, 
                                    result, 
                                    cmd_tx.clone(), 
                                    tracked.clone(),
                                    bootstrap.clone()
                                );
                                tokio::spawn(task);
                            },
                            _ => {}
                        }
                    },
                    Some(cmd) = cmd_rx.recv() => {
                        match cmd {
                            ScheduleJob(id) => {
                                let task = schedule_job(
                                    id,
                                    cmd_tx.clone(),
                                    tracked.clone(),
                                    bootstrap.clone(),
                                    rpc_client.clone()
                                );

                                tokio::spawn(task);
                            },
                            ManageJob(id) => {
                                let task = schedule_job(
                                    id,
                                    cmd_tx.clone(),
                                    tracked.clone(),
                                    bootstrap.clone(),
                                    rpc_client.clone()
                                );

                                tokio::spawn(task);
                            },
                            SendEvent(event) => {
                                let _ = tx.send(event);
                            },
                        }
                    }
                }
            }
        });
        
        let trac = tracked.clone();
        args.rpc_server.register(RPC_WATCH_JOB, move |job: Job, _| {
            trac.lock().insert(job.id, JobTrackerInfo {
                id: job.id,
                state: JobState::Pending,
                job,
                retry: 3,
                expires_at: None
            });
            std::future::ready(())
        });

        let trac = tracked.clone();
        args.rpc_server.register(RPC_GET_STATE_JOB, move |id: JobId, _| {
            std::future::ready(trac.lock().get(&id).map(|infos| infos.state.clone()))
        });

        WorkerWatchdog
    }
}

pub async fn watch_job(job: Job, bootstrap: BootstrapClient, rpc: RpcClient) -> Result<(), WorkerError> {
    let watchdog = bootstrap.select_peer(NS_WORKER_WATCHDOG, &job.id).ok_or(WorkerError::NoWatchdogFound)?;
    RpcCallArgs::builder()
        .name(RPC_WATCH_JOB)
        .args(&job)
        .destination(watchdog)
        .build()
        .call::<Void>(&rpc)
        .await?;

    Ok(())
}

pub async fn get_job_state(id: JobId, bootstrap: BootstrapClient, rpc: RpcClient) -> Result<Option<JobState>, WorkerError> {
    let watchdog = bootstrap.select_peer(NS_WORKER_WATCHDOG, &id).ok_or(WorkerError::NoWatchdogFound)?;

    let state = RpcCallArgs::builder()
        .name(RPC_GET_STATE_JOB)
        .args(&id)
        .destination(watchdog)
        .build()
        .call::<Option<JobState>>(&rpc)
        .await?;

    Ok(state)
}

fn has_competence(id: JobId, bootstrap: &BootstrapClient) -> bool {
    matches!(bootstrap.select_peer_with_local(NS_WORKER_WATCHDOG, id), PeerSelection::Local)
}

async fn schedule_job(
    id: JobId, 
    cmd_tx: mpsc::UnboundedSender<Command>, 
    tracked: JobTrackers, 
    bootstrap: BootstrapClient, 
    rpc: RpcClient
) {
    let Some(job) = tracked.lock().get(&id).map(|info| info.job.clone()) else { return };

    let Some(worker_id) = bootstrap.select_peer(NS_WORKER, id) else { return }; 

    let Ok(_) = RpcCallArgs::builder()
        .name(RPC_SCHEDULE_JOB)
        .args(job)
        .destination(worker_id)
        .build()
        .call::<Void>(&rpc)
        .await else { return };

    let mut guard = tracked.lock();
    let infos = guard.get_mut(&id).unwrap();

    infos.state = JobState::Scheduled { worker: worker_id };
    let _ = cmd_tx.send(Command::SendEvent(WorkerEvent::JobStateUpdate { id, state: infos.state.clone() }));
}

async fn update_job_state_upon_result(
    id: JobId, 
    result: JobResult, 
    cmd_tx: mpsc::UnboundedSender<Command>, 
    tracked: JobTrackers,
    bootstrap: BootstrapClient
) {
    // le watchdog n'a pas compétence pour gérer ce job.
    if !has_competence(id, &bootstrap) { return };

    let mut guard = tracked.lock();
    let Some(infos) = guard.get_mut(&id) else { return };
    
    let state = match result {
        JobResult::Success(value) => {
            JobState::Completed(value)
        },
        JobResult::Failed(error) => {
            JobState::Failed { error }
        },
    };

    infos.state = state.clone();
    drop(guard);

    let _ = cmd_tx.send(Command::SendEvent(WorkerEvent::JobStateUpdate { id, state }));
}

#[must_use]
pub struct WorkerWatchdog;