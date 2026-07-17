use std::{collections::HashMap, sync::Arc};

use futures::StreamExt;
use parking_lot::Mutex;
use tokio::select;

use crate::{job::JobId, layer::Layer, network::worker::{JobResult, WorkerEvent, client::WorkerClient}, rpc::RpcServer, sink::{BoxSink, SinkBoxExt}, tools::{JOB_TOOL_EXECUTE, RPC_TOOL_EXECUTE, ToolCall, ToolCallId, ToolEvent}};

pub struct ToolServerActor;

pub struct ToolExecutionTracker {
    job_id: Option<JobId>,
    call: ToolCall,
    expires_at: std::time::Instant
}

impl ToolServerActor {
    pub fn new(
        layer: impl Layer<Send = ToolEvent, Received = ToolEvent>,
        worker_layer: impl Layer<Send = WorkerEvent, Received = WorkerEvent>,
        mut rpc: RpcServer,
        worker: WorkerClient
    ) {
        let (tx, rx) = layer.split();
        let tx = tx.boxed_sink();
        let mut rx = rx.boxed();

        let (_, worker_rx) = worker_layer.split();
        let mut worker_rx = worker_rx.boxed();
        
        let ongoings_: Arc<Mutex<HashMap<ToolCallId, ToolExecutionTracker>>> = Arc::new(Mutex::new(HashMap::default()));
        
        let ongoings = ongoings_.clone();
        let workr = worker.clone();
        
        rpc.register(RPC_TOOL_EXECUTE,  move|call: ToolCall, _| {
            let workr = workr.clone();
            let ongoings: Arc<parking_lot::lock_api::Mutex<parking_lot::RawMutex, HashMap<ToolCallId, ToolExecutionTracker>>> = ongoings.clone();
            async move {
                let ttl = std::time::Duration::from_mins(5);

                let job_id = workr
                    .spawn(JOB_TOOL_EXECUTE, &call, Some(ttl))
                    .await
                    .unwrap();

                let mut guard = ongoings.lock();
                guard.insert(call.id, ToolExecutionTracker { 
                    job_id: Some(job_id), 
                    call, 
                    expires_at: std::time::Instant::now() + ttl
                });
            }
        });

        let ongoings = ongoings_.clone();
        tokio::spawn(async move {
            use WorkerEvent::JobDone;

            loop {
                select! {
                    Some(event) = worker_rx.next() => {
                        match event {
                            WorkerEvent::JobDone { id, result } => {

                                
                            },
                        }
                    },
                    Some(event) = rx.next() => {}
                }
            }
        });


    }
}

fn handle_job_result(
    ongoings: &mut Arc<Mutex<HashMap<ToolCallId, ToolExecutionTracker>>>,
    job_id: JobId,
    job_result: JobResult,
    tx: &mut BoxSink<'_, ToolEvent, anyhow::Error>
) {
    let mut guard = ongoings.lock();
    let Some(tool_call_id) = guard
        .iter()
        .find(|(tool_call_id, infos)| infos.job_id == Some(tool_call_id))
        .map(|(tool_call_id, _)| *tool_call_id) 
        else { return };

    let Some(infos) = guard.remove(&tool_call_id) else { return };
    
    match job_result {
        JobResult::Success(value) => todo!(),
        JobResult::Failed(err) => todo!(),
    }
}

pub struct ToolServer;


