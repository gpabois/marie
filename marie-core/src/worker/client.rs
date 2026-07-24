use std::{collections::HashMap, sync::{Arc, Weak}};

use futures::StreamExt;
use parking_lot::Mutex;
use tokio::{select, sync::{self, mpsc, watch}};
use typed_builder::TypedBuilder;

use crate::{
    annuary::Annuary, di::{Factory, Get}, id, job::{Job, JobId, JobInstance, JobState}, layer::{Layer, LayerExt as _}, network::{Network, bootstrap::{self, BootstrapClient}}, pubsub::layers::PubSubLayer, rpc::{RpcClient, Void, client::RpcCallArgs}, worker::{NS_WORKER_WATCHDOG, RPC_WATCH_JOB, WorkerError, WorkerEvent, layers::WorkerEventLayer}
};

type JobTrackers = Arc<Mutex<HashMap<JobId, TrackedJobInfo>>>;

#[derive(TypedBuilder)]
pub struct WorkerClientArgs {
    rpc: RpcClient,
    annuary: Annuary
}


#[derive(Clone)]
pub struct WorkerClient {
    rpc: RpcClient,
    annuary: Annuary,
    trackers: Arc<Mutex<HashMap<JobId, TrackedJobInfo>>>,
    cmd_tx: mpsc::UnboundedSender<Command>
}

impl<C> Factory<C> for WorkerClient where C: Get<Network> + Get<Annuary> + Get<RpcClient> {
    fn create(container: &C) -> Self {
        let network: Network = container.get();
        
        let args = WorkerClientArgs::builder()
            .rpc(container.get())
            .annuary(container.get())
            .build();

        Actor::create(
            network
                .layer()
                .chain::<PubSubLayer, _>(())
                .chain::<WorkerEventLayer, _>(()),
            args
        )
    }
}

impl WorkerClient {
    /// Track a job in the cluster
    pub async fn track(&mut self, job_id: JobId) -> Result<JobTracker, WorkerError> {
        use Command::Track;

        let guard = self.trackers.lock();
        if let Some(infos) = guard.get(&job_id) 
            && let Some(keeper) = infos.keeper.upgrade() {
            
            return Ok(JobTracker {
                job_id,
                listener: infos.listeners.clone(),
                keeper
            });
        }
        drop(guard);

        let (tx, rx) = watch::channel(JobState::Unknown);
        let keeper = Arc::new(TrackerKeeper(job_id, self.trackers.clone()));

        let tracker = JobTracker {
            job_id,
            listener: rx.clone(),
            keeper: keeper.clone()
        };
        
        let info = TrackedJobInfo {
            job_id,
            state: JobState::Unknown,
            listeners: rx.clone(),
            subscribers: tx,
            keeper: Arc::downgrade(&keeper),
        };

        let _ = self.cmd_tx.send(Track(info));
        
        Ok(tracker)
    }

    /// Spawn a new job in the cluster. Générique sur [`Job`] — sur le même
    /// modèle que [`crate::rpc::RpcClient::invoke`] — pour que `J::NAME` soit
    /// la seule source de vérité du nom envoyé au worker, sans risque de
    /// diverger d'une constante dupliquée côté appelant.
    pub async fn spawn<J: Job>(&self, args: impl Into<J::Args>, ttl: Option<std::time::Duration>) -> Result<JobId, WorkerError> {
        let id = id::generate_id();

        let job = JobInstance {
            id,
            name: J::NAME.to_string(),
            args: serde_json::to_value(args.into()).unwrap(),
        };

        super::watchdog::watch_job(job, self.bootstrap.clone(), self.rpc.clone()).await?;

        Ok(id)
    }
}

pub struct Actor;

struct TrackedJobInfo {
    job_id: JobId,
    state: JobState,
    listeners: watch::Receiver<JobState>,
    subscribers: watch::Sender<JobState>,
    keeper: Weak<TrackerKeeper>
}

impl Actor {
    pub fn create(
        layer: impl Layer<Send = WorkerEvent, Received = WorkerEvent>,
        args: WorkerClientArgs
    ) -> WorkerClient {
        let (_, mut rx) = layer.boxed_split();

        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<Command>();
        let tracks: Arc<Mutex<HashMap<JobId, TrackedJobInfo>>> = Arc::new(Mutex::new(HashMap::default()));

        let trackers = tracks.clone();
        let rpc = args.rpc.clone();
        let annuary = args.annuary.clone();
        tokio::spawn(async move {
            loop {
                select! { 
                    Some(event) = rx.next() => {
                        match event {
                            WorkerEvent::JobStateUpdate { id: job_id, state } => {
                                update_job_state(trackers.clone(), job_id, state);
                            },
                            _ => {}
                        }
                    },
                    Some(cmd) = cmd_rx.recv() => {
                        match cmd {
                            Command::Track(tracked_job_info) => {
                                let job_id = tracked_job_info.job_id;
                                let task = super::watchdog::get_job_state(tracked_job_info.job_id, annuary.clone(), rpc.clone());
                                trackers.lock().insert(tracked_job_info.job_id, tracked_job_info);

                                let trackers = trackers.clone();
                                tokio::spawn(async move {
                                    if let Ok(Some(state)) = task.await {
                                        update_job_state(trackers.clone(), job_id, state)
                                    }
                                });
                            },
                        }
                    }
                }
            }

        });

        WorkerClient {
            rpc: args.rpc.clone(),
            annuary: args.annuary.clone(),
            trackers: tracks.clone(),
            cmd_tx
        }
    }
}

fn update_job_state(trackers: JobTrackers, job_id: JobId, state: JobState) {
    if let Some(infos) = trackers.lock().get_mut(&job_id) {
        infos.state = state.clone();
        let _ = infos.subscribers.send(state);
    }
}

/// Supprime le tracker en cas de drop.
struct TrackerKeeper(JobId, Arc<Mutex<HashMap<JobId, TrackedJobInfo>>>);

impl Drop for TrackerKeeper {
    fn drop(&mut self) {
        let mut guard = self.1.lock();
        guard.remove(&self.0);
    }
}

enum Command {
    Track(TrackedJobInfo)
}

#[derive(Clone)]
pub struct JobTracker {
    job_id: JobId,
    listener: sync::watch::Receiver<JobState>,
    keeper: Arc<TrackerKeeper>
}
