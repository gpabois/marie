use std::{collections::HashMap, panic::AssertUnwindSafe, sync::Arc};

use crate::{
    job::JobInstance,
    layer::Layer,
    network::{bootstrap::BootstrapClient, worker::{NS_WORKER, RPC_SCHEDULE_JOB, WorkerEvent}},
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
pub struct WorkerServerArgs<Cx, B> where B: Fn(&JobInstance) -> Cx + Send + Sync + 'static {
    bootstrap: BootstrapClient,
    rpc_server: RpcServer,
    job_context_builder: B
}

type JobExecutor<Cx> =  Arc<dyn (Fn(Cx, serde_json::Value) -> BoxFuture<'static, Result<serde_json::Value, anyhow::Error>>) + Send + Sync + 'static>;

enum Command<Cx> {
    Register(String, JobExecutor<Cx>)
}

pub struct WorkerServerActor;

impl WorkerServerActor {
    pub fn new<B, Cx>(
        layer: impl Layer<Send=WorkerEvent, Received = WorkerEvent>,
        mut args: WorkerServerArgs<Cx, B>
    ) -> WorkerServer<Cx>
        where
            B: Fn(&JobInstance) -> Cx + Send + Sync + 'static,
            Cx: Send + 'static
    {
        args.bootstrap.register_to_namespaces([Namespace::from_static(NS_WORKER)]);

        let (tx, rx) = layer.split();

        let mut tx = tx.boxed_sink();
        let _rx = rx.boxed();

        let (event_tx, mut event_rx) = mpsc::unbounded::<WorkerEvent>();
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded::<Command<Cx>>();

        let executors: Arc<Mutex<HashMap<String, JobExecutor<Cx>>>> = Default::default();
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
        let job_context_builder = args.job_context_builder;

        // enregistre la fonction execute
        args.rpc_server.register(RPC_SCHEDULE_JOB, move |job: JobInstance, _| {
            let Some(executor) = execs.lock().get(&job.name).cloned() else {
                return std::future::ready(Err("aucun exécuteur pour le travail n'a été trouvé")).boxed();
            };

            let cx = job_context_builder(&job);

            let Ok(args) = serde_json::from_value(job.args) else {
                return std::future::ready(Err("erreur lors de la desérialization des arguments du job")).boxed();
            };

            let mut evtx = evtx.clone();
            
            let _ = tokio::spawn(async move {
                let task = AssertUnwindSafe(executor(cx, args));
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
pub struct WorkerServer<Cx> {
    event_tx: mpsc::UnboundedSender<WorkerEvent>,
    cmd_tx: mpsc::UnboundedSender<Command<Cx>>
}

impl<Cx: Send> WorkerServer<Cx> {
    pub fn register_job_executor<F, Args, R, Fut>(&mut self, name: impl ToString, executor: F)
        where F: (Fn(Cx, Args) -> Fut) + Send + Sync + 'static, 
                Fut: Future<Output=Result<R, anyhow::Error>> + Send + 'static,
                Args: DeserializeOwned,
                R: Serialize
    {
        use Command::Register;

        let wrapped = move |cx: Cx, args: serde_json::Value| {
            let args = serde_json::from_value(args).unwrap();
            let task = executor(cx, args);

            async move {
                 task
                 .await
                 .map(|value| serde_json::to_value(value).unwrap())
            }.boxed()
        };

        let _ = self.cmd_tx.send(Register(name.to_string(), Arc::new(wrapped)));
    }
}

