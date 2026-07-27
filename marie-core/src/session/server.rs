use crate::{agent::frame::AgentFrame, node::LocalNodeId, session::worker::RunAgent, worker::{Worker, WorkerClient}};

pub struct SessionManager {
    workers: WorkerClient,
    local_node_id: LocalNodeId
}

impl SessionManager {
    
    fn run_agent_frame(&self, frame: AgentFrame) {
        self.workers.spawn::<RunAgent>(args, ttl)
    }
}