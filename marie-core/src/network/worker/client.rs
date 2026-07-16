use serde::Serialize;

use crate::{id, job::Job, network::{bootstrap::BootstrapClient, worker::{NS_WORKER, NS_WORKER_WATCHDOG, RPC_EXECUTE_JOB, RPC_WATCH_JOB, WorkerError}}, rpc::{RpcClient, client::RpcCallArgs}};


pub struct WorkerClient {
    rpc: RpcClient,
    bootstrap: BootstrapClient
}

impl WorkerClient {
    pub async fn spawn(&self, name: String, args: impl Serialize, ttl: Option<std::time::Duration>) -> Result<(), WorkerError> {
        let id = id::generate_id();

        let job = Job {
            id,
            name,
            args: serde_json::to_value(args).unwrap(),
        };
        
        let worker_id = self.bootstrap
            .select_peer(NS_WORKER, id)
            .ok_or_else(|| WorkerError::NoWorkerFound)?;

        let watchdog_id = self.bootstrap
            .select_peer(NS_WORKER_WATCHDOG, id)
            .ok_or_else(|| WorkerError::NoWatchdogFound)?;

        // On va demander à un worker d'exécuter le job
        RpcCallArgs::builder()
            .name(RPC_EXECUTE_JOB)
            .args(&job)
            .destination(worker_id)
            .build()
            .call::<Result<(), String>>(&self.rpc)
            .await?;

        // On va demander explicitement à un watchdog de surveiller le job
        // pour des raisons de fiabilité, on ne peut pas considérer que 
        // le spawn a fonctionné car on a aucun moyen de surveiller si le job
        // a bien été exécuté et le cas échéant retry
        RpcCallArgs::builder()
            .name(RPC_WATCH_JOB)
            .args((&job, worker_id))
            .build()
            .call::<Result<(), String>>(&self.rpc)
            .await?;

        Ok(())
    }
}