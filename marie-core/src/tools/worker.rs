use crate::{network::worker::{JobContext, server::WorkerServer}, tools::{JOB_TOOL_EXECUTE, ToolCall}};

pub struct ToolWorker {

}

impl ToolWorker {
    pub fn new(worker: &WorkerServer<JobContext>) {
        worker.register_job_executor(JOB_TOOL_EXECUTE, |_, call: ToolCall| async move {
            todo!("impl. exécution des tools")
        });
    }
}