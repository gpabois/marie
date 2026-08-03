use std::{collections::HashMap, marker::PhantomData, sync::Arc};

use async_stream::stream;
use chrono::{Duration, Utc};
use futures::{Stream, StreamExt};
use parking_lot::Mutex;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::{select, sync};
use tokio_stream::wrappers::WatchStream;
use typed_builder::TypedBuilder;

use crate::{
    annuary::{Annuary, capabilities::Capability}, di::{Constructible, Resolve}, events::EventBus, id::{self, IdGenerator}, job::{Job, JobId, JobInstance, JobState}, node::NodeId, rpc::RpcClient, stream::{DynamicStreamPool, StreamHandle}, worker::{ WorkerError, WorkerEvent, rpc::{GetJobState, ScheduleJob}}
};


#[derive(TypedBuilder)]
pub struct WorkerClientArgs {
    rpc: RpcClient,
    id: IdGenerator,
    events: EventBus,
    annuary: Annuary
}

#[derive(Clone)]
enum TimerEvent {
    ProbingTimeOut(JobId),
    AckTimeOut(JobId),
    Probe(JobId)
}

#[derive(Clone)]
pub struct WorkerClient {
    rpc: RpcClient,
    id: IdGenerator,
    events: EventBus,
    annuary: Annuary,
    trackers: Arc<Mutex<HashMap<JobId, JobInfo>>>,
    jobs_timers: StreamHandle<TimerEvent>,
}

impl<C> Constructible<C> for WorkerClient 
    where C: Resolve<RpcClient> 
        + Resolve<EventBus> 
        + Resolve<Annuary>
        + Resolve<IdGenerator>
{
    fn construct(container: &C, args: ()) -> Self {
        let args = WorkerClientArgs::builder()
            .rpc(container.resolve(()))
            .annuary(container.resolve(()))
            .events(container.resolve(()))
            .id(container.resolve(()))
            .build();

        Self::new(args)
    }
}

impl WorkerClient {
    pub fn new(args: WorkerClientArgs) -> Self {
        let pool = DynamicStreamPool::<TimerEvent>::new(300);
        let jobs_timers = pool.handle();
        let client = WorkerClient {
            rpc: args.rpc,
            annuary: args.annuary,
            events: args.events,
            id: args.id,
            trackers: Arc::new(Mutex::new(HashMap::default())),
            jobs_timers
        };

        tokio::spawn(client.clone().run(pool));

        client
    }

    /// Spawn a new job in the cluster. Générique sur [`Job`] — sur le même
    /// modèle que [`crate::rpc::RpcClient::invoke`] — pour que `J::NAME` soit
    /// la seule source de vérité du nom envoyé au worker, sans risque de
    /// diverger d'une constante dupliquée côté appelant.
    pub async fn spawn<J: Job>(&self, args: impl Into<J::Args>, ttl: Option<std::time::Duration>) -> Result<JobHandle<J::Return>, WorkerError> {
        let id = self.id.next();

        let instance = JobInstance {
            id,
            name: J::NAME.to_string(),
            args: serde_json::to_value(args.into()).unwrap(),
        };

        let worker = self.annuary.pick_top_n(id, Capability::Worker, 3).pop_front().ok_or_else(|| WorkerError::NoWorkerAvailable)?;
        
        let handle = self.watch::<J::Return>(id, JobState::PendingScheduling { instance, worker });

        Ok(handle)
    }


    async fn run(self, mut timers: DynamicStreamPool<TimerEvent>) {
        let mut rx = self.events.stream_events::<WorkerEvent>("/marie/workers/events");

        loop {
            select! {
                Some(event) = rx.next() => {
                    match event.payload {
                        WorkerEvent::JobUpdate {id, state} => {
                            self.handle_job_state(id, state);
                        }
                    }
                },
                Some(timer_event) = timers.next() => self.handle_timer_event(timer_event)
            }
        }
    }

    fn handle_timer_event(&self, event: TimerEvent) {
        use JobState::{Failed, Probing, Running, Scheduled};

        match event {
            TimerEvent::ProbingTimeOut(id) => {
                let Some(state) = self.trackers.lock().get(&id).map(|tracker| tracker.state.clone()) else { return };

                if state.is_probing() {
                    self.handle_job_state(id, Failed { error: WorkerError::ProbingTimeOut });
                }
            },
            TimerEvent::Probe(id) => {
                let Some(state) = self.trackers.lock().get(&id).map(|tracker| tracker.state.clone()) else { return };

                match state {
                    Running { worker } | Scheduled { worker } => {
                        self.handle_job_state(id, Probing { worker });
                    },
                    _ => {}
                }
            },
            TimerEvent::AckTimeOut(id) => {
                let Some(state) = self.trackers.lock().get(&id).map(|tracker| tracker.state.clone()) else { return };
                if state.is_pending_ack() {
                    self.handle_job_state(id, Failed { error: WorkerError::AckTimeOut });
                }
            }
        }
    }

    fn handle_job_state(&self, id: JobId, state: JobState) {
        use JobState::{PendingAck, Probing, Scheduled, Running, PendingScheduling};

        {
            let mut guard = self.trackers.lock();
            let Some(tracker) = guard.get_mut(&id) else { return };
            tracker.state = state.clone();
            tracker.listeners
                .iter()
                .for_each(|listener| {
                    listener.send(state.clone());
                });
        }

        match state {
            PendingScheduling { instance, worker } => {
                tokio::spawn(self.clone().schedule_job(instance, worker));
            },
            PendingAck { .. } => {
                self.jobs_timers.after(Duration::seconds(30), TimerEvent::AckTimeOut(id));
            },
            Probing { worker } => {
                tokio::spawn(self.clone().probe_state(id, worker));
                self.jobs_timers.after(Duration::seconds(30), TimerEvent::ProbingTimeOut(id));
            },
            Scheduled { worker } | Running { worker } => {
                tokio::spawn(self.clone().probe_state(id, worker.clone()));
                self.jobs_timers.after(Duration::seconds(30), TimerEvent::Probe(id));
            },
            _ => {}
        }
    }

    async fn schedule_job(self, instance: JobInstance, worker: NodeId) -> Result<(), WorkerError> {
        use JobState::{PendingAck, Failed, Scheduled};
        let id = instance.id;

        self.handle_job_state(id, PendingAck { worker: worker.clone() });

        match self.rpc.invoke::<ScheduleJob>(instance, [worker.clone()]).await {
            Err(error) => self.handle_job_state(id, Failed { error: WorkerError::ScheduledError(error.to_string()) }),
            Ok(Err(error)) => self.handle_job_state(id, Failed { error: error.into() }),
            Ok(Ok(_)) => self.handle_job_state(id, Scheduled {worker})
        };

        Ok(())
    }

    /// Probe the state of the job in the worker.
    async fn probe_state(self, id: JobId, worker: NodeId) -> Result<(), WorkerError> {

        let Some(state) = self.trackers.lock().get(&id).map(|tracker| tracker.state.clone()) else { return Ok(()) };
        
        // pas besoin de probe si le travail est terminé.
        if state.has_terminated() {
            return Ok(());
        }

        self.handle_job_state(id, JobState::Probing { worker: worker.clone() });
        let state = self.rpc.invoke::<GetJobState>(id, [worker]).await??;
        self.handle_job_state(id, state);
        Ok(())
    }
    
}

impl WorkerClient {
    /// Track a job s
    fn watch<R>(&self, job_id: JobId, initial: JobState) -> JobHandle<R> {        
        let (tx, rx) = sync::watch::channel(JobState::Unknown);

        let info = JobInfo {
            id: job_id,
            state: JobState::Unknown,
            listeners: vec![tx],
            check_at: Utc::now() + chrono::Duration::seconds(30),
        };

        self.trackers.lock().insert(job_id, info);
        self.handle_job_state(job_id, initial);
        
        JobHandle {
            job_id,
            _phantom: Default::default(),
            listener: rx
        }
    }
}

struct JobInfo {
    id: JobId,
    state: JobState,
    listeners: Vec<sync::watch::Sender<JobState>>,
    check_at: chrono::DateTime<Utc>
}

#[derive(Clone)]
pub struct JobHandle<R=Value> {
    job_id: JobId,
    _phantom: PhantomData<R>,
    listener: sync::watch::Receiver<JobState>,
}

impl<R> JobHandle<R> where R: DeserializeOwned + Send + 'static {
    pub fn stream(self) -> impl Stream<Item = Result<JobState<R>, WorkerError>> + Send + 'static {
        let mut stream = WatchStream::new(self.listener);

        stream! {
            while let Some(state) = stream.next().await {
                let terminated = state.has_terminated();
                yield state.deserialize();
                if terminated {
                    break;
                }
            }
        }
    }
}