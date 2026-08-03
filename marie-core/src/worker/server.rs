use std::{any::Any, collections::HashMap, panic::AssertUnwindSafe, sync::Arc};

use crate::{
    di::{Constructible, Get, Resolve}, events::EventBus, job::{JobId, JobInstance, JobState}, node::OwnNodeId, rpc::{RemoteProcedureCall as _, RpcServer}, worker::{JobAck, WorkerError, WorkerEvent, rpc::{GetJobState, ScheduleJob}}
};
use futures::{FutureExt, future::BoxFuture};
use parking_lot::Mutex;
use serde::{Serialize, de::DeserializeOwned};
use typed_builder::TypedBuilder;

#[derive(TypedBuilder)]
pub struct WorkerArgs {
    local_node_id: OwnNodeId,
    rpc: RpcServer,
    events: EventBus
}

type JobExecutor = Arc<dyn (Fn(serde_json::Value) -> BoxFuture<'static, Result<serde_json::Value, crate::Error>>) + Send + Sync + 'static>;

#[derive(Clone)]
pub struct Worker {
    local_node_id: OwnNodeId,
    events: EventBus,
    executors: Arc<Mutex<HashMap<String, JobExecutor>>>,
    jobs: Arc<Mutex<HashMap<JobId, JobState>>>
}

impl<C> Constructible<C> for Worker 
    where C: Get<OwnNodeId> + Resolve<EventBus> + Resolve<RpcServer>
{
    fn construct(container: &C, args: ()) -> Self {
        let args = WorkerArgs::builder()
            .local_node_id(container.get())
            .events(container.resolve(()))
            .rpc(container.resolve(()))
            .build();

        Self::new(args)
    }
}

impl Worker {
    pub fn new(mut args: WorkerArgs) -> Self {
        let worker = Self {
            local_node_id: args.local_node_id,
            events: args.events,
            executors: Arc::new(Mutex::new(HashMap::default())),
            jobs: Arc::new(Mutex::new(HashMap::default()))
        };

        ScheduleJob::new(worker.clone()).register(&mut args.rpc);
        GetJobState::new(worker.clone()).register(&mut args.rpc);

        worker
    }

    pub fn register_job_executor<F, Args, R, Fut>(&self, name: impl ToString, executor: F)
        where F: (Fn(Args) -> Fut) + Send + Sync + 'static,
                Fut: Future<Output=Result<R, crate::Error>> + Send + 'static,
                Args: DeserializeOwned,
                R: Serialize
    {

        let wrapped = move |args: serde_json::Value| {
            let args = serde_json::from_value(args).unwrap();
            let task = executor(args);

            async move {
                 task
                 .await
                 .map(|value| serde_json::to_value(value).unwrap())
            }.boxed()
        };

        let name = name.to_string();

        self.executors.lock().insert(name.clone(), Arc::new(wrapped));
    }

    fn update_state(&self, id: JobId, state: JobState) {
        *self.jobs.lock().entry(id).or_default() = state.clone();
        self.events.emit(WorkerEvent::JobUpdate { id, state });
    }

    pub(super) fn get_job_state(self, id: &JobId) -> Result<JobState, WorkerError> {
        self.jobs.lock().get(id).cloned().ok_or_else(|| WorkerError::NoJobFound(*id))
    }

    async fn execute_job(self, job: JobInstance) {
        use JobState::{Failed, Running, Completed};
        
        let Some(executor) = self.executors.lock()
            .get(&job.name)
            .cloned() else {

            self.update_state(job.id, Failed { error: WorkerError::NoJobExecutorFound(job.name.clone()) });
            return;
        };
   
        self.update_state(job.id, Running { worker: self.local_node_id.clone().into() });

        let result = match AssertUnwindSafe(executor(job.args)).catch_unwind().await {
            Err(err) => {
                let error = WorkerError::Panicked(extract_panic_message(err));
                self.update_state(job.id, Failed {error});
                return;
            }

            Ok(result) => result
        };

        match result.map_err(WorkerError::ExecutionError) {
            Ok(value) => self.update_state(job.id, Completed(value)),
            Err(error) => self.update_state(job.id, Failed {error}),
        }
    }
    

    pub(super) async fn schedule_job(self, job: JobInstance) -> Result<JobAck, WorkerError> {
        use JobState::{Failed, Scheduled};

        if self.executors.lock().get(&job.name).is_none() {
            self.update_state(job.id, Failed { error: WorkerError::NoJobExecutorFound(job.name.clone()) });
            return Err(WorkerError::NoJobExecutorFound(job.name.clone()));
        }
        
        let job_id = job.id.clone();
        let scheduled = Scheduled { worker: self.local_node_id.clone().into() };
        self.update_state(job_id, scheduled);

        tokio::spawn(self.clone().execute_job(job));

        Ok(JobAck {
            worker_id: self.local_node_id.clone().into(),
            job_id
        })
    }
}

fn extract_panic_message(err: Box<dyn Any + Send>) -> String {
    if let Some(s) = err.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = err.downcast_ref::<String>() {
        s.clone()
    } else {
        "panique inconnue".to_string()
    }
}