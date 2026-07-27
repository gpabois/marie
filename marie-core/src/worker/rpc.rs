use marie_macros::core_rpc;

use crate::{job::{JobId, JobInstance, JobState}, worker::{JobAck, WorkerError}};
#[cfg(feature="rpc-executor")]
use crate::worker::server::Worker;

core_rpc! {
    #[rpc(name="/marie/workers/schedule-job")]
    pub async fn schedule_job(self: Self<Worker>, job: JobInstance) -> Result<JobAck, WorkerError> {
        self.0.schedule_job(job).await
    }
}

core_rpc! {
    #[rpc(name="/marie/workers/get-job-state")]
    pub async fn get_job_state(self: Self<Worker>, id: JobId) -> Result<JobState, WorkerError> {
        self.0.get_job_state(&id)
    }
}