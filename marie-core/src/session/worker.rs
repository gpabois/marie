use marie_macros::core_job;

use crate::{graph::Graphs, model::Models, session::{frames::{NewFrame, FrameId}, protocol::FrameResponse}, tools::Tools};

core_job! {
    #[job(name="/marie/sessions/jobs/run-frame")]
    pub async fn run_frame(self: Self<{models: Models, tools: Tools, graphs: Graphs}>, _: (FrameId, Frame)) -> FrameResponse {
        todo!("...")
    }
}