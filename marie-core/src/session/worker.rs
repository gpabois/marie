use marie_macros::core_job;
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::{
    graph::Graphs, 
    model::Models, 
    tools::Tools,
    session::{SessionId, frames::FrameId, protocol::FrameResponse}
};

#[derive(Debug, Clone, Serialize, Deserialize, TypedBuilder)]
pub struct RunFrameArgs {
    session_id: SessionId,
    frame_id: FrameId,
}

core_job! {
    #[job(name="/marie/sessions/jobs/run-frame")]
    pub async fn run_frame(self: Self<{models: Models, tools: Tools, graphs: Graphs}>, args: RunFrameArgs) -> FrameResponse {
        todo!("...")
    }
}