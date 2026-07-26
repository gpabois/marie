use std::{collections::HashMap, panic::AssertUnwindSafe, sync::Arc};

use crate::{
    job::JobInstance,
    layer::Layer,
    network::bootstrap::BootstrapClient,
    worker::{NS_WORKER, RPC_SCHEDULE_JOB, WorkerEvent},
    rpc::RpcServer,
    sink::SinkBoxExt
};
use futures::{FutureExt, SinkExt, StreamExt, channel::mpsc, future::BoxFuture};
use libp2p::rendezvous::Namespace;
use parking_lot::Mutex;
use serde::{Serialize, de::DeserializeOwned};
use tokio::select;
use typed_builder::TypedBuilder;

#[derive(TypedBuilder)]
pub struct WorkerServerArgs<'di, Di> {
    container: &'di Di,
}

type JobExecutor = Arc<dyn (Fn(serde_json::Value) -> BoxFuture<'static, Result<serde_json::Value, crate::Error>>) + Send + Sync + 'static>;

enum Command {
    Register(String, JobExecutor)
}

pub struct WorkerServerActor;

impl WorkerServerActor {
    pub fn new(
        layer: impl Layer<Send=WorkerEvent, Received = WorkerEvent>,
        mut args: WorkerServerArgs,
    ) -> WorkerServer {
        args.bootstrap.register_to_namespaces([Namespace::from_static(NS_WORKER)]);

        let (tx, rx) = layer.split();

        let mut tx = tx.boxed_sink();
        let _rx = rx.boxed();

        let (event_tx, mut event_rx) = mpsc::unbounded::<WorkerEvent>();
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded::<Command>();

        let executors: Arc<Mutex<HashMap<String, JobExecutor>>> = Default::default();
        let execs = executors.clone();

        tokio::spawn(async move {
            use Command::Register;
            loop {
                select! {
                    Ok(event_to_send) = event_rx.recv() => {
                        let _ = tx.send(event_to_send);
                    }
                    Ok(cmd) = cmd_rx.recv() => {
                        match cmd {
                            Register(name, executor) => {
                                let _ = executors.lock().insert(name, executor);
                            }
                        }
                    }
                }
            }
        });
        
        // on enregistre ce qu'il faut
        let evtx = event_tx.clone();

        // enregistre la fonction execute
        args.rpc_server.register(RPC_SCHEDULE_JOB, move |job: JobInstance, _| {
            let Some(executor) = execs.lock().get(&job.name).cloned() else {
                return std::future::ready(Err("aucun exécuteur pour le travail n'a été trouvé")).boxed();
            };

            let Ok(args) = serde_json::from_value(job.args) else {
                return std::future::ready(Err("erreur lors de la desérialization des arguments du job")).boxed();
            };

            let mut evtx = evtx.clone();

            let _ = tokio::spawn(async move {
                let task = AssertUnwindSafe(executor(args));
                let result = task.catch_unwind().await;

                match result {
                    Ok(Ok(result)) => {
                        let _ = evtx.send(WorkerEvent::JobDone { 
                            id: job.id, 
                            result: super::JobResult::Success(result)
                        }).await;
                    },
                    Ok(Err(error)) => {
                        let _ = evtx.send(WorkerEvent::JobDone { 
                            id: job.id, 
                            result: super::JobResult::Failed(format!("le travail {}#{} a échoué: {error}", job.name, job.id)) 
                        }).await;
                    }
                    Err(_) => {
                        let _ = evtx.send(WorkerEvent::JobDone { 
                            id: job.id, 
                            result: super::JobResult::Failed(format!("le travail {}#{} a paniqué", job.name, job.id)) 
                        }).await;
                    }
                }
                
            });

            std::future::ready(Ok(())).boxed()
        });


        WorkerServer { event_tx, cmd_tx }
    }
}

#[derive(Clone)]
pub struct WorkerServer {
    event_tx: mpsc::UnboundedSender<WorkerEvent>,
    cmd_tx: mpsc::UnboundedSender<Command>
}

impl WorkerServer {
    pub fn register_job_executor<F, Args, R, Fut>(&mut self, name: impl ToString, executor: F)
        where F: (Fn(Args) -> Fut) + Send + Sync + 'static,
                Fut: Future<Output=Result<R, crate::Error>> + Send + 'static,
                Args: DeserializeOwned,
                R: Serialize
    {
        use Command::Register;

        let wrapped = move |args: serde_json::Value| {
            let args = serde_json::from_value(args).unwrap();
            let task = executor(args);

            async move {
                 task
                 .await
                 .map(|value| serde_json::to_value(value).unwrap())
            }.boxed()
        };

        let _ = self.cmd_tx.send(Register(name.to_string(), Arc::new(wrapped)));
    }
}

